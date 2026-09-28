// Package backoff retries a function with exponential backoff and full jitter.
// It is used by the listener's reconnect loop (unlimited) and by the notifier's send retries
// (bounded by NOTIFY_RETRY_MAX_ELAPSED). Imports stdlib only.
package backoff

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Policy describes how to space retries. The zero value is unusable; set at least Initial and Max.
type Policy struct {
	Initial    time.Duration // upper bound of the first delay
	Max        time.Duration // upper bound of any delay
	Multiplier float64       // growth per attempt; ≤ 1 means 2
	MaxElapsed time.Duration // total time budget including the attempts; 0 = unlimited

	// Rand returns a value in [0,1) for the jitter. nil means math/rand/v2.Float64.
	Rand func() float64
	// OnRetry, when set, is called before sleeping. attempt counts from 1.
	OnRetry func(attempt int, err error, delay time.Duration)
	// Abort, when set, reports whether an error is worth no further attempts (an SMTP 5xx, say).
	// Retry then returns it immediately. nil means every error is retried.
	Abort func(error) bool
}

// Delay returns the wait before the retry following attempt (attempt counts from 1): a random
// point in (0, min(Max, Initial×Multiplier^(attempt-1))] — "full jitter", which spreads reconnects
// of many replicas instead of synchronising them.
func (p Policy) Delay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	mult := p.Multiplier
	if mult <= 1 {
		mult = 2
	}
	d := float64(p.Initial) * math.Pow(mult, float64(attempt-1))
	if max := float64(p.Max); max > 0 && (d > max || math.IsInf(d, 1)) {
		d = max
	}
	r := p.Rand
	if r == nil {
		r = rand.Float64
	}
	// (0,1] instead of [0,1) so a delay is never zero.
	return time.Duration(d * (1 - r()))
}

// Retry calls fn until it returns nil, ctx is done, Abort rejects the error, or MaxElapsed is
// exhausted; it then returns fn's last error. Callers that treat cancellation as success (the listener's shutdown) check
// ctx.Err() themselves.
func Retry(ctx context.Context, p Policy, fn func(context.Context) error) error {
	start := time.Now()
	for attempt := 1; ; attempt++ {
		err := fn(ctx)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
		if p.Abort != nil && p.Abort(err) {
			return err
		}
		delay := p.Delay(attempt)
		if p.MaxElapsed > 0 && time.Since(start)+delay > p.MaxElapsed {
			return err
		}
		if p.OnRetry != nil {
			p.OnRetry(attempt, err, delay)
		}
		if !Sleep(ctx, delay) {
			return err
		}
	}
}

// Sleep waits for d or until ctx is done; it reports whether the full delay elapsed.
func Sleep(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
