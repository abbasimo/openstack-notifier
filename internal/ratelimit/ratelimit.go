// Package ratelimit keeps a build storm from turning into a mail storm, without losing anything.
//
// Two pieces: a token bucket over outbound mail, and a digest buffer that collects the outcomes
// the bucket turned down and sends them as one summary email. A buffered outcome still owns its
// unsettled AMQP delivery, so nothing is acknowledged before its email is sent.
//
// Contract: docs/ARCHITECTURE.md §4.7.
package ratelimit

import (
	"context"
	"sync"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/dedup"
	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/obs"
)

// Bucket is a token bucket: burst tokens available at once, refilled at perMinute per minute.
// Time is a parameter rather than read from the clock, so the pipeline's single clock governs it.
type Bucket struct {
	mu       sync.Mutex
	tokens   float64
	burst    float64
	perSec   float64
	lastFill time.Time
}

// NewBucket returns a full bucket. perMinute ≤ 0 or burst ≤ 0 means "no limit".
func NewBucket(perMinute, burst int) *Bucket {
	return &Bucket{
		tokens: float64(burst),
		burst:  float64(burst),
		perSec: float64(perMinute) / 60,
	}
}

// Allow consumes one token and reports whether there was one.
func (b *Bucket) Allow(now time.Time) bool {
	if b.perSec <= 0 || b.burst <= 0 {
		return true // unlimited
	}
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lastFill.IsZero() {
		b.lastFill = now
	}
	if elapsed := now.Sub(b.lastFill); elapsed > 0 {
		b.tokens = min(b.burst, b.tokens+elapsed.Seconds()*b.perSec)
		b.lastFill = now
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// Tokens reports the tokens available at now (tests and diagnostics).
func (b *Bucket) Tokens(now time.Time) float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.lastFill.IsZero() {
		return b.tokens
	}
	return min(b.burst, b.tokens+max(0, now.Sub(b.lastFill).Seconds())*b.perSec)
}

// Entry is one outcome waiting in the digest, with everything needed to finish it: the dedup token
// to commit or release, and the delivery to settle. Ack is nil for D6 timeout outcomes, which have
// no AMQP delivery behind them.
type Entry struct {
	Outcome model.Outcome
	Token   dedup.Token
	Ack     model.Acker
}

// Digest collects rate-limited outcomes and hands them over as one batch, on an interval or as
// soon as it is full.
type Digest struct {
	max      int
	interval time.Duration
	metrics  *obs.Metrics

	mu      sync.Mutex
	entries []Entry
	closed  bool

	full chan struct{} // capacity 1: a nudge, never a queue
}

// NewDigest returns a digest holding at most max entries and flushing every interval.
func NewDigest(max int, interval time.Duration, m *obs.Metrics) *Digest {
	return &Digest{max: max, interval: interval, metrics: m, full: make(chan struct{}, 1)}
}

// Add buffers an outcome. false means the digest is full (or closed) and the caller must send this
// one directly — the alternative, dropping it, is the one thing we never do.
func (d *Digest) Add(e Entry) bool {
	if d.max <= 0 {
		return false
	}
	d.mu.Lock()
	if d.closed || len(d.entries) >= d.max {
		d.mu.Unlock()
		return false
	}
	d.entries = append(d.entries, e)
	full := len(d.entries) >= d.max
	d.mu.Unlock()

	d.metrics.RatelimitDeferred.Inc()
	if full {
		select {
		case d.full <- struct{}{}:
		default: // a flush is already pending
		}
	}
	return true
}

// Run flushes the buffer every interval, and immediately whenever it fills up. It returns when ctx
// is done **without** flushing: the final flush belongs to the shutdown path, which has its own
// deadline (ARCHITECTURE §9, row 15) — see Take.
func (d *Digest) Run(ctx context.Context, flush func(context.Context, []Entry)) {
	ticker := time.NewTicker(d.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-d.full:
		}
		if entries := d.Take(); len(entries) > 0 {
			flush(ctx, entries)
		}
	}
}

// Take removes and returns everything buffered. The shutdown path calls it for the final flush.
func (d *Digest) Take() []Entry {
	d.mu.Lock()
	defer d.mu.Unlock()
	entries := d.entries
	d.entries = nil
	return entries
}

// Close stops Add from accepting new entries; buffered ones are still returned by Take.
func (d *Digest) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
}

// Len reports how many entries are buffered.
func (d *Digest) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.entries)
}
