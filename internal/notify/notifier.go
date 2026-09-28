// Package notify delivers a rendered message — over SMTP, or into a mock for demos and tests.
//
// Contract: docs/ARCHITECTURE.md §4.9. Operator documentation: docs/NOTIFICATIONS.md.
// Imports stdlib + internal/util/backoff only, so the transport can be reasoned about on its own.
package notify

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/util/backoff"
)

// Message is one rendered notification: both MIME parts plus the extra headers the pipeline wants
// on it (X-OpenStack-Instance-UUID). Addresses, Date and Message-ID belong to the transport.
type Message struct {
	Subject  string
	TextBody string
	HTMLBody string
	Headers  map[string]string
}

// Notifier delivers a Message. Implementations must respect ctx and must be safe for concurrent
// use: every worker sends through the same Notifier.
type Notifier interface {
	Notify(ctx context.Context, msg Message) error
	Name() string
}

// ErrPermanent marks a failure that retrying cannot fix — a rejected recipient, a refused
// authentication, a relay that will not accept the message. Wrap it with %w. Anything not wrapping
// it is treated as transient, which is the safe default: we retry rather than drop.
var ErrPermanent = errors.New("permanent notification failure")

// Permanent reports whether err is a failure not worth retrying.
func Permanent(err error) bool { return errors.Is(err, ErrPermanent) }

// Retry wraps a Notifier with exponential backoff and full jitter. The retries happen *inside* the
// worker, which is deliberate: a busy worker stops taking deliveries, prefetch fills up, and the
// broker holds the traffic back for us (DD5). Requeuing instead would hot-loop.
//
// A permanent failure returns at once. The policy's MaxElapsed (NOTIFY_RETRY_MAX_ELAPSED) bounds
// the whole thing, and must stay below MESSAGE_DEADLINE and the broker's consumer_timeout.
func Retry(n Notifier, p backoff.Policy) Notifier {
	p.Abort = Permanent
	return &retrier{next: n, policy: p}
}

type retrier struct {
	next   Notifier
	policy backoff.Policy
}

func (r *retrier) Name() string { return r.next.Name() }

func (r *retrier) Notify(ctx context.Context, msg Message) error {
	return backoff.Retry(ctx, r.policy, func(ctx context.Context) error {
		return r.next.Notify(ctx, msg)
	})
}

// Multi sends to every notifier and reports every failure. It succeeds only if all of them do, so
// one broken destination cannot silently swallow a failure notification. A permanent failure
// anywhere makes the whole send permanent — retrying would re-deliver to the ones that worked.
func Multi(ns ...Notifier) Notifier { return multi(ns) }

type multi []Notifier

func (m multi) Name() string {
	names := make([]string, len(m))
	for i, n := range m {
		names[i] = n.Name()
	}
	return strings.Join(names, "+")
}

func (m multi) Notify(ctx context.Context, msg Message) error {
	var errs []error
	permanent := false
	for _, n := range m {
		if err := n.Notify(ctx, msg); err != nil {
			permanent = permanent || Permanent(err)
			errs = append(errs, fmt.Errorf("%s: %w", n.Name(), err))
		}
	}
	err := errors.Join(errs...)
	if err != nil && permanent && !Permanent(err) {
		return fmt.Errorf("%w: %w", ErrPermanent, err)
	}
	return err
}

// Result classifies one send attempt for notifications_sent_total{result}.
const (
	ResultOK        = "ok"
	ResultTransient = "transient_failure"
	ResultPermanent = "permanent_failure"
)

// Observer is told the outcome of every individual attempt. It exists so `notify` can stay out of
// `obs` (import rules, ARCHITECTURE §3) while still reporting per-attempt metrics.
type Observer func(notifier, result string, latency time.Duration)

// Observe wraps a Notifier so each attempt is reported. Put it *inside* Retry —
// Retry(Observe(smtp, f), policy) — so every attempt is counted, not just the last one.
func Observe(n Notifier, f Observer) Notifier {
	if f == nil {
		return n
	}
	return &observed{next: n, observer: f}
}

type observed struct {
	next     Notifier
	observer Observer
}

func (o *observed) Name() string { return o.next.Name() }

func (o *observed) Notify(ctx context.Context, msg Message) error {
	start := time.Now()
	err := o.next.Notify(ctx, msg)
	o.observer(o.next.Name(), classify(err), time.Since(start))
	return err
}

func classify(err error) string {
	switch {
	case err == nil:
		return ResultOK
	case Permanent(err):
		return ResultPermanent
	default:
		return ResultTransient
	}
}
