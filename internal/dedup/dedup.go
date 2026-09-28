// Package dedup suppresses duplicate emails without ever suppressing a real one.
//
// Contract: docs/ARCHITECTURE.md §4.5. The sequence is check → dispatch → commit (or release):
// nothing is marked as sent before the email is actually accepted, so a transient SMTP failure
// followed by an AMQP redelivery still produces exactly one email — not zero.
package dedup

import (
	"context"
	"log/slog"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/obs"
	"github.com/abbasimo/openstack-notifier/internal/store"
)

// Key prefixes and their lifetimes (docs/ARCHITECTURE.md §4.4).
const (
	msgPrefix      = "msg:"      // one AMQP message, redelivered
	outcomePrefix  = "outcome:"  // one build outcome reported by two different event types
	inflightPrefix = "inflight:" // another worker is handling this outcome right now
	msgTTL         = time.Hour
	outcomeTTL     = 24 * time.Hour
)

// Token identifies the work a Check granted. Pass it back to Commit or Release.
type Token struct {
	MessageID string
	UUID      string
	Kind      model.OutcomeKind
}

// Dedup is stateless: all state is in the Store.
type Dedup struct {
	store    store.Store
	metrics  *obs.Metrics
	log      *slog.Logger
	deadline time.Duration // MESSAGE_DEADLINE; inflight entries live 2× that
}

func New(s store.Store, m *obs.Metrics, log *slog.Logger, messageDeadline time.Duration) *Dedup {
	return &Dedup{store: s, metrics: m, log: log, deadline: messageDeadline}
}

func (d *Dedup) outcomeKey(t Token) string {
	return outcomePrefix + t.UUID + ":" + t.Kind.String()
}

func (d *Dedup) inflightKey(t Token) string {
	return inflightPrefix + t.UUID + ":" + t.Kind.String()
}

// Check reports whether this outcome should be notified. false means "already handled" — the
// caller acks the delivery and sends nothing.
//
// A store error makes Check proceed: a duplicate email is a nuisance, a missed failure email is
// the bug this service exists to prevent (ARCHITECTURE §4.5, fail open).
func (d *Dedup) Check(ctx context.Context, o model.Outcome) (Token, bool) {
	t := Token{MessageID: o.Event.MessageID, UUID: o.Event.Instance.UUID, Kind: o.Kind}

	if t.MessageID != "" {
		if _, seen, err := d.store.Get(ctx, msgPrefix+t.MessageID); err != nil {
			d.logErr("reading message marker", t, err)
		} else if seen {
			return d.duplicate(t, "message already handled")
		}
	}
	if _, seen, err := d.store.Get(ctx, d.outcomeKey(t)); err != nil {
		d.logErr("reading outcome marker", t, err)
	} else if seen {
		return d.duplicate(t, "outcome already reported")
	}

	// Claim the outcome for this worker. Two events for the same build can arrive on two workers
	// at once (create.error and the building→error update); only one gets through.
	claimed, err := d.store.SetNX(ctx, d.inflightKey(t), []byte(t.MessageID), 2*d.deadline)
	if err != nil {
		d.logErr("claiming outcome", t, err)
		return t, true // fail open
	}
	if !claimed {
		return d.duplicate(t, "another worker is handling this outcome")
	}
	return t, true
}

func (d *Dedup) duplicate(t Token, reason string) (Token, bool) {
	d.metrics.DedupDropped.Inc()
	d.log.Info("duplicate outcome suppressed",
		slog.String(obs.KeyInstanceUUID, t.UUID),
		slog.String(obs.KeyMessageID, t.MessageID),
		slog.String(obs.KeyOutcome, t.Kind.String()),
		slog.String("reason", reason))
	return t, false
}

// Commit records that the email was sent. Only after this does a redelivery become a no-op.
func (d *Dedup) Commit(ctx context.Context, t Token) {
	if t.MessageID != "" {
		if err := d.store.Set(ctx, msgPrefix+t.MessageID, []byte("1"), msgTTL); err != nil {
			d.logErr("recording message marker", t, err)
		}
	}
	if err := d.store.Set(ctx, d.outcomeKey(t), []byte(t.MessageID), outcomeTTL); err != nil {
		d.logErr("recording outcome marker", t, err)
	}
	if err := d.store.Delete(ctx, d.inflightKey(t)); err != nil {
		d.logErr("clearing inflight marker", t, err)
	}
}

// Release gives the claim back after a failed send, so the redelivery can try again. Forgetting
// this would turn one transient SMTP failure into a permanently missing email.
func (d *Dedup) Release(ctx context.Context, t Token) {
	if err := d.store.Delete(ctx, d.inflightKey(t)); err != nil {
		d.logErr("releasing inflight marker", t, err)
	}
}

func (d *Dedup) logErr(what string, t Token, err error) {
	d.log.Warn("store error; dedup degraded", obs.Err(err),
		slog.String("operation", what),
		slog.String(obs.KeyInstanceUUID, t.UUID))
}
