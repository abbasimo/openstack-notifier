package store

import (
	"context"
	"hash/fnv"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"
)

const (
	// shardCount is a power of two so the FNV hash can be masked instead of divided. 32 keeps
	// lock contention negligible at the worker counts we run (default 16).
	shardCount = 32
	// retention is how long an expired entry stays poppable before the sweeper drops it. Without
	// it a D6 timeout that nobody popped (replica restarted) would leak forever.
	retention = time.Hour
	// sweepInterval is how often expired-and-forgotten entries are collected.
	sweepInterval = time.Minute
	// evictFraction: when a shard is full, this fraction of it is evicted at once (earliest
	// deadline first), so the O(n) scan is amortised over many inserts instead of running on
	// every one.
	evictFraction = 64
)

// Memory is the default Store: sharded maps with lazy expiry, a background sweeper and a hard
// entry cap. It never returns an error — every method's error result is nil, which is what lets
// the correlator treat store failure as "only duration accuracy is lost".
//
// Memory is per-replica. With several replicas, dedup and correlation are best-effort; see
// docs/ARCHITECTURE.md §7 and ADR-0001/ADR-0004.
type Memory struct {
	shards  [shardCount]shard
	perCap  int // entry cap per shard; 0 = unlimited
	onEvict func(n int)
}

var _ Store = (*Memory)(nil)

type shard struct {
	mu      sync.Mutex
	entries map[string]Entry
}

// NewMemory returns an in-memory Store holding at most maxEntries entries (0 = unlimited).
// onEvict, if set, is called with the number of entries dropped when the cap is reached; the
// caller wires it to store_evictions_total. It must not call back into the store.
func NewMemory(maxEntries int, onEvict func(n int)) *Memory {
	m := &Memory{onEvict: onEvict}
	if maxEntries > 0 {
		// Keys are hashed, so an even split across shards is the right approximation of a global
		// cap and needs no cross-shard coordination on the hot path.
		m.perCap = max(1, (maxEntries+shardCount-1)/shardCount)
	}
	for i := range m.shards {
		m.shards[i].entries = make(map[string]Entry)
	}
	return m
}

func (m *Memory) shard(key string) *shard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key)) // hash.Hash never fails
	return &m.shards[h.Sum32()&(shardCount-1)]
}

// SetNX sets key only if it is absent or expired.
func (m *Memory) SetNX(_ context.Context, key string, val []byte, ttl time.Duration) (bool, error) {
	now := time.Now()
	s := m.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[key]; ok && now.Before(e.Deadline) {
		return false, nil
	}
	m.insert(s, key, val, now.Add(ttl))
	return true, nil
}

// Set stores key unconditionally.
func (m *Memory) Set(_ context.Context, key string, val []byte, ttl time.Duration) error {
	s := m.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	m.insert(s, key, val, time.Now().Add(ttl))
	return nil
}

// Get returns the value unless the key is absent or past its deadline.
func (m *Memory) Get(_ context.Context, key string) ([]byte, bool, error) {
	s := m.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	e, ok := s.entries[key]
	if !ok || !time.Now().Before(e.Deadline) {
		return nil, false, nil
	}
	return slices.Clone(e.Value), true, nil
}

// Delete removes the key, expired or not.
func (m *Memory) Delete(_ context.Context, key string) error {
	s := m.shard(key)
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, key)
	return nil
}

// PopExpired removes and returns up to max entries with the given prefix whose deadline has
// passed. Each entry is taken and removed under its shard's lock, so an entry is handed to exactly
// one caller even with several tickers running.
//
// The returned batch is sorted earliest-deadline first. When more than max entries are expired,
// *which* max is unspecified; the rest stay poppable and come back on the next call.
func (m *Memory) PopExpired(_ context.Context, prefix string, now time.Time, maxEntries int) ([]Entry, error) {
	if maxEntries <= 0 {
		return nil, nil
	}
	found := make([]Entry, 0, min(maxEntries, 64))
	for i := range m.shards {
		if len(found) == maxEntries {
			break
		}
		s := &m.shards[i]
		s.mu.Lock()
		for key, e := range s.entries {
			if len(found) == maxEntries {
				break
			}
			if strings.HasPrefix(key, prefix) && !now.Before(e.Deadline) {
				found = append(found, e)
				delete(s.entries, key)
			}
		}
		s.mu.Unlock()
	}
	slices.SortFunc(found, func(a, b Entry) int { return a.Deadline.Compare(b.Deadline) })
	return found, nil
}

// Run sweeps entries that expired more than an hour ago until ctx is done. Without it, entries
// nobody pops (a `pending:` whose replica no longer runs the D6 ticker) would live forever.
func (m *Memory) Run(ctx context.Context) {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sweep(time.Now())
		}
	}
}

func (m *Memory) sweep(now time.Time) int {
	removed := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		for key, e := range s.entries {
			if now.After(e.Deadline.Add(retention)) {
				delete(s.entries, key)
				removed++
			}
		}
		s.mu.Unlock()
	}
	return removed
}

// Len reports the number of entries currently held, expired ones included. Used by tests and by
// the pending_builds gauge.
func (m *Memory) Len() int {
	n := 0
	for i := range m.shards {
		s := &m.shards[i]
		s.mu.Lock()
		n += len(s.entries)
		s.mu.Unlock()
	}
	return n
}

// insert writes one entry, evicting first if the shard is at its cap. Callers hold s.mu.
func (m *Memory) insert(s *shard, key string, val []byte, deadline time.Time) {
	if m.perCap > 0 {
		if _, replacing := s.entries[key]; !replacing && len(s.entries) >= m.perCap {
			m.evict(s)
		}
	}
	s.entries[key] = Entry{Key: key, Value: slices.Clone(val), Deadline: deadline}
}

// evict drops the earliest-deadline entries from a full shard — the ones closest to being
// useless anyway. Callers hold s.mu.
func (m *Memory) evict(s *shard) {
	keys := slices.SortedFunc(maps.Keys(s.entries), func(a, b string) int {
		return s.entries[a].Deadline.Compare(s.entries[b].Deadline)
	})
	n := max(1, len(keys)/evictFraction)
	for _, key := range keys[:min(n, len(keys))] {
		delete(s.entries, key)
	}
	if m.onEvict != nil {
		m.onEvict(n)
	}
}
