// Package store is the notifier's small TTL key/value abstraction: everything the correlator and
// the dedup stage need to remember, and nothing more.
//
// FROZEN since the end of Phase 5.1: change the Store interface only by ADR (AGENTS.md → Binding
// rules). It is deliberately tiny so a Redis implementation (Plan.md 10.1) is `SET NX PX` plus a
// sorted set for PopExpired. Contract: docs/ARCHITECTURE.md §4.4. Imports stdlib only.
package store

import (
	"context"
	"time"
)

// Entry is one stored key with its value and expiry deadline.
type Entry struct {
	Key      string
	Value    []byte
	Deadline time.Time
}

// Store is a TTL key/value store with one extra operation — PopExpired — which is what makes
// "a build that never finished" detectable without scanning anything.
//
// Expiry contract (ARCHITECTURE §4.4):
//   - An entry past its deadline is invisible to Get and does not block SetNX, but it is still
//     returned by PopExpired until it is popped, or until deadline+1h when the sweeper drops it.
//   - Delete removes the entry whether or not it has expired.
//
// Implementations must be safe for concurrent use.
type Store interface {
	// SetNX sets key only if it is absent or expired. It reports whether it was set.
	SetNX(ctx context.Context, key string, val []byte, ttl time.Duration) (bool, error)
	// Set sets key unconditionally.
	Set(ctx context.Context, key string, val []byte, ttl time.Duration) error
	// Get returns the value, or ok=false when the key is absent or expired.
	Get(ctx context.Context, key string) ([]byte, bool, error)
	// Delete removes the key. Deleting an absent key is not an error.
	Delete(ctx context.Context, key string) error
	// PopExpired atomically removes and returns up to max entries with the given key prefix
	// whose deadline is at or before now.
	PopExpired(ctx context.Context, prefix string, now time.Time, max int) ([]Entry, error)
}
