package pipeline

import (
	"context"
	"log/slog"
	"runtime/debug"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/correlator"
	"github.com/abbasimo/openstack-notifier/internal/dedup"
	"github.com/abbasimo/openstack-notifier/internal/holdback"
	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/notify"
	"github.com/abbasimo/openstack-notifier/internal/obs"
	"github.com/abbasimo/openstack-notifier/internal/parser"
	"github.com/abbasimo/openstack-notifier/internal/ratelimit"
	"github.com/abbasimo/openstack-notifier/internal/render"
)

// timeoutTick is how often the D6 ticker looks for stuck builds. It also reclaims expired pending
// entries, so it runs even with BUILD_TIMEOUT_ENABLED=false.
const timeoutTick = 10 * time.Second

// timeoutBatch bounds one tick's work, so a backlog is drained over several ticks instead of
// blocking one for minutes.
const timeoutBatch = 100

// Deps are the constructed stages. cmd/notifier builds them all; nothing here is package state.
type Deps struct {
	Parser     *parser.Parser
	Correlator *correlator.Correlator
	Dedup      *dedup.Dedup
	Hold       *holdback.Hold
	Bucket     *ratelimit.Bucket
	Digest     *ratelimit.Digest
	Renderer   *render.Renderer
	Notifier   notify.Notifier
	Log        *slog.Logger
	Metrics    *obs.Metrics
	Now        func() time.Time

	// EventTypes is the allow-list, used to keep the events_filtered_total label set bounded.
	EventTypes []string
}

type Pipeline struct {
	d       Deps
	allowed map[string]bool
}

func New(d Deps) *Pipeline {
	if d.Now == nil {
		d.Now = time.Now
	}
	allowed := make(map[string]bool, len(d.EventTypes))
	for _, t := range d.EventTypes {
		allowed[t] = true
	}
	return &Pipeline{d: d, allowed: allowed}
}

// Handle is listener.Handler: one delivery in, one disposition out. It never returns an error and
// never panics — a single bad message must not take down a replica (ack-table row 13).
func (p *Pipeline) Handle(ctx context.Context, body []byte, ack model.Acker) (disposition model.Disposition) {
	defer func() {
		if r := recover(); r != nil {
			p.d.Metrics.PanicsRecovered.Inc()
			p.d.Log.Error("panic in the pipeline; dead-lettering the message", "panic", r, "stack", string(debug.Stack()))
			disposition = model.DeadLetter
		}
	}()

	res, err := p.d.Parser.Parse(body, p.d.Now())
	if err != nil {
		p.d.Metrics.EventsParseFailed.Inc()
		p.d.Log.Error("cannot parse notification", obs.Err(err), "bytes", len(body),
			slog.String(obs.KeyMessageID, res.Event.MessageID))
		return model.DeadLetter // row 1
	}
	if !res.Keep {
		p.d.Metrics.EventsFiltered.With(p.filterLabel(res.Event.EventType)).Inc()
		return model.Ack // row 2
	}
	if len(res.Missing) > 0 {
		p.d.Log.Warn("parse_partial",
			slog.String(obs.KeyEventType, res.Event.EventType),
			slog.String(obs.KeyMessageID, res.Event.MessageID),
			slog.Any("missing", res.Missing))
	}

	outcome, ok := p.d.Correlator.Observe(ctx, res.Event)
	if !ok {
		return model.Ack // rows 3 and 4: a recorded start, or a build deleted while building
	}

	// A fallback failure carries no fault. Hold it: if the detailed event arrives during the grace
	// period it is handled first, and dedup drops this one (ADR-0009, row 7).
	if outcome.Fallback && p.d.Hold.Offer(outcome, ack, p.d.Now()) {
		p.d.Log.Debug("holding a fallback failure",
			slog.String(obs.KeyInstanceUUID, outcome.Event.Instance.UUID))
		return model.Deferred
	}
	disposition, _ = p.tail(ctx, outcome, ack) // disposition is Handle's named return
	return disposition
}

// tail is everything after correlation: dedup → rate limit → render → send. It is shared by
// Handle, the holdback release and the D6 ticker, so all three obey the same ack table.
//
// It also reports whether the outcome was a duplicate, because `Ack` alone cannot say: rows 5 and 6
// both ack, and the difference is exactly what fallback_released_total measures.
func (p *Pipeline) tail(ctx context.Context, o model.Outcome, ack model.Acker) (model.Disposition, bool) {
	token, proceed := p.d.Dedup.Check(ctx, o)
	if !proceed {
		return model.Ack, true // row 5
	}

	// Out of tokens: collect the outcome into the digest rather than mailing it now. The entry
	// keeps this delivery unsettled, so nothing is acknowledged before its email is sent (row 8).
	if !p.d.Bucket.Allow(p.d.Now()) {
		if p.d.Digest.Add(ratelimit.Entry{Outcome: o, Token: token, Ack: ack}) {
			return model.Deferred, false
		}
		// The digest is full; sending directly beats dropping or blocking.
		p.d.Log.Debug("digest full; sending directly",
			slog.String(obs.KeyInstanceUUID, o.Event.Instance.UUID))
	}

	msg, err := p.d.Renderer.Outcome(o)
	if err != nil {
		// A template bug reproduces on every redelivery, so requeuing would loop (row 9).
		p.d.Dedup.Release(ctx, token)
		p.d.Log.Error("cannot render the notification", obs.Err(err),
			slog.String(obs.KeyInstanceUUID, o.Event.Instance.UUID),
			slog.String(obs.KeyOutcome, o.Kind.String()))
		return model.DeadLetter, false
	}
	return p.send(ctx, msg, []dedup.Token{token}, o.Kind.String(), o.Event.Instance.UUID), false
}

// send delivers one message and maps the result onto the ack table. tokens are committed together
// on success and released together on failure — the digest path passes several.
func (p *Pipeline) send(ctx context.Context, msg notify.Message, tokens []dedup.Token, outcome, uuid string) model.Disposition {
	err := p.d.Notifier.Notify(ctx, msg)
	switch {
	case err == nil:
		for _, t := range tokens {
			p.d.Dedup.Commit(ctx, t)
		}
		p.d.Log.Info("notification sent",
			slog.String(obs.KeyOutcome, outcome),
			slog.String(obs.KeyInstanceUUID, uuid),
			slog.String("subject", msg.Subject))
		return model.Ack // row 6

	case notify.Permanent(err):
		p.release(ctx, tokens)
		p.d.Log.Error("notification permanently rejected; dead-lettering", obs.Err(err),
			slog.String(obs.KeyOutcome, outcome), slog.String(obs.KeyInstanceUUID, uuid))
		return model.DeadLetter // row 10

	default:
		// Transient, or the context ended mid-send during shutdown: give the message back to the
		// broker so another attempt — here or on another replica — still delivers it (rows 11, 12).
		p.release(ctx, tokens)
		p.d.Log.Warn("notification not sent; requeuing", obs.Err(err),
			slog.String(obs.KeyOutcome, outcome), slog.String(obs.KeyInstanceUUID, uuid))
		return model.Requeue
	}
}

func (p *Pipeline) release(ctx context.Context, tokens []dedup.Token) {
	for _, t := range tokens {
		p.d.Dedup.Release(ctx, t)
	}
}

// Release is the holdback callback: the grace period expired, so run the tail and settle the
// delivery ourselves — the listener already handed it over (row 7 completing as rows 5–12).
func (p *Pipeline) Release(ctx context.Context, o model.Outcome, ack model.Acker) {
	disposition, duplicate := p.tail(ctx, o, ack)
	// "duplicate" is the outcome ADR-0009 hopes for: a detailed failure event overtook the
	// fallback during the grace period, so the admin got the better email instead of this one.
	result := "sent"
	if duplicate {
		result = "duplicate"
	}
	p.d.Metrics.FallbackReleased.With(result).Inc()
	p.settle(disposition, ack, o.Event.Instance.UUID)
}

// FlushDigest sends one digest email for a batch of deferred outcomes and settles every delivery
// in it (row 8, and row 15 on shutdown). Either all of them are acked, or all are requeued.
func (p *Pipeline) FlushDigest(ctx context.Context, entries []ratelimit.Entry) {
	if len(entries) == 0 {
		return
	}
	outcomes := make([]model.Outcome, 0, len(entries))
	tokens := make([]dedup.Token, 0, len(entries))
	for _, e := range entries {
		outcomes = append(outcomes, e.Outcome)
		tokens = append(tokens, e.Token)
	}

	msg, err := p.d.Renderer.Digest(outcomes)
	if err != nil {
		p.release(ctx, tokens)
		p.d.Log.Error("cannot render the digest; requeuing its messages", obs.Err(err),
			"entries", len(entries))
		p.settleAll(model.Requeue, entries)
		return
	}

	// A digest does not consume a token: it is the answer to running out of them.
	disposition := p.send(ctx, msg, tokens, "digest", "")
	if disposition == model.Ack {
		p.d.Metrics.DigestSent.Inc()
	}
	p.settleAll(disposition, entries)
}

// Timeouts runs the D6 ticker until ctx is done. It also reclaims expired pending builds when the
// timeout feature is off, which is why it always runs.
func (p *Pipeline) Timeouts(ctx context.Context) {
	ticker := time.NewTicker(timeoutTick)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		for _, o := range p.d.Correlator.Expired(ctx, p.d.Now(), timeoutBatch) {
			// A timeout outcome has no AMQP delivery behind it: nothing to settle, and a failed
			// send is simply lost (documented in ARCHITECTURE §4.3 / Plan.md 5.3).
			p.tail(ctx, o, nil) //nolint:errcheck // no delivery to settle
		}
	}
}

// Drain is the shutdown callback the listener calls before closing the AMQP channel: hand back
// everything the holdback still owns (row 14), then send one last digest (row 15).
func (p *Pipeline) Drain(ctx context.Context) {
	p.d.Hold.Drain()
	p.d.Digest.Close()
	p.FlushDigest(ctx, p.d.Digest.Take())
}

// settle applies a disposition to a delivery the pipeline owns (holdback and digest paths). The
// listener does this for Handle's return value; here there is nobody else to do it.
func (p *Pipeline) settle(d model.Disposition, ack model.Acker, uuid string) {
	if ack == nil {
		return // a D6 timeout outcome: no delivery
	}
	var err error
	switch d {
	case model.Ack:
		err = ack.Ack()
	case model.Requeue:
		err = ack.Nack(true)
	case model.DeadLetter:
		err = ack.Nack(false)
	case model.Deferred:
		return // somebody else owns it now
	}
	p.d.Metrics.Dispositions.With(d.String()).Inc()
	if err != nil {
		// The channel closed under us; the broker has already requeued the delivery.
		p.d.Log.Debug("cannot settle a delivery", obs.Err(err),
			slog.String(obs.KeyDisposition, d.String()),
			slog.String(obs.KeyInstanceUUID, uuid))
	}
}

func (p *Pipeline) settleAll(d model.Disposition, entries []ratelimit.Entry) {
	for _, e := range entries {
		p.settle(d, e.Ack, e.Outcome.Event.Instance.UUID)
	}
}

// filterLabel keeps events_filtered_total bounded: tenant traffic cannot invent label values.
func (p *Pipeline) filterLabel(eventType string) string {
	if p.allowed[eventType] {
		return eventType
	}
	return "other"
}

// Digest and Hold expose the two background components main must run and, on shutdown, drain.
// They are accessors rather than fields on Deps' owner so there is exactly one place — New — that
// decides what the pipeline is made of.
func (p *Pipeline) Digest() *ratelimit.Digest { return p.d.Digest }

func (p *Pipeline) Hold() *holdback.Hold { return p.d.Hold }
