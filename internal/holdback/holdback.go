// Package holdback delays fallback failure outcomes so a better one can overtake them.
//
// `instance.update building→error` is the only failure signal Nova emits on every failure path,
// but it carries no fault and arrives *before* the detailed event (docs/DOMAIN.md, scenario C).
// Sending it immediately would mean an email saying only "the build failed" for failures we can
// describe precisely. So it is held for FALLBACK_GRACE; by the time it is released, the detailed
// event has usually been handled and dedup drops the fallback (ADR-0009).
//
// Contract: docs/ARCHITECTURE.md §4.6.
package holdback

import (
	"context"
	"sync"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/obs"
)

// checkInterval is how often Run looks for due items. The grace period is a minute by default, so
// a second of granularity is plenty.
const checkInterval = time.Second

type held struct {
	outcome model.Outcome
	ack     model.Acker
	due     time.Time
}

// Hold is a bounded set of outcomes waiting out their grace period. Each one owns an unsettled
// AMQP delivery, which is why max exists and why it counts against AMQP_PREFETCH
// (docs/ARCHITECTURE.md §8).
type Hold struct {
	max     int
	grace   time.Duration
	metrics *obs.Metrics

	mu     sync.Mutex
	items  []held
	closed bool
}

// New returns a Hold keeping at most max outcomes for grace each. A grace of 0 disables holding:
// Offer always returns false and fallback outcomes are sent immediately.
func New(max int, grace time.Duration, m *obs.Metrics) *Hold {
	return &Hold{max: max, grace: grace, metrics: m}
}

// Offer hands an outcome and its delivery to the hold. false means the caller keeps it and
// proceeds immediately: holding is disabled, the hold is full, or we are shutting down.
func (h *Hold) Offer(o model.Outcome, ack model.Acker, now time.Time) bool {
	if h.grace <= 0 || h.max <= 0 {
		return false
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.closed || len(h.items) >= h.max {
		return false
	}
	h.items = append(h.items, held{outcome: o, ack: ack, due: now.Add(h.grace)})
	h.metrics.FallbackHeld.Set(int64(len(h.items)))
	return true
}

// Run releases outcomes as their grace expires, until ctx is done. Each release runs in its own
// goroutine — a slow SMTP send must not delay the others — and Run waits for all of them before
// returning, so shutdown stays ordered.
func (h *Hold) Run(ctx context.Context, release func(context.Context, model.Outcome, model.Acker)) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			for _, item := range h.due(now) {
				wg.Add(1)
				go func() {
					defer wg.Done()
					release(ctx, item.outcome, item.ack)
				}()
			}
		}
	}
}

// due removes and returns the items whose grace has expired.
func (h *Hold) due(now time.Time) []held {
	h.mu.Lock()
	defer h.mu.Unlock()

	var ready []held
	kept := h.items[:0]
	for _, item := range h.items {
		if now.Before(item.due) {
			kept = append(kept, item)
			continue
		}
		ready = append(ready, item)
	}
	h.items = kept
	if len(ready) > 0 {
		h.metrics.FallbackHeld.Set(int64(len(h.items)))
	}
	return ready
}

// Drain returns every held delivery to the broker (ARCHITECTURE §5 row 14). Held outcomes are not
// sent on shutdown: requeuing them means another replica — or this one after a restart — decides
// again with full information, which is better than a rushed email during shutdown.
func (h *Hold) Drain() {
	h.mu.Lock()
	items := h.items
	h.items = nil
	h.closed = true
	h.mu.Unlock()

	for _, item := range items {
		if item.ack != nil {
			_ = item.ack.Nack(true) // the broker requeues; a failure here means it already did
		}
	}
	h.metrics.FallbackHeld.Set(0)
}

// Len reports how many outcomes are currently held.
func (h *Hold) Len() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.items)
}
