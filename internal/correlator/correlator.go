// Package correlator turns a stream of events into a stream of decisions: it decides whether a
// notification means a build succeeded, failed, or nothing at all.
//
// Contract: docs/ARCHITECTURE.md §4.3. Event roles: ADR-0009 and docs/DOMAIN.md §2.
// Observe never fails — a store error costs duration or timeout accuracy, never an email.
package correlator

import (
	"context"
	"log/slog"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/obs"
	"github.com/abbasimo/openstack-notifier/internal/store"
)

// Key prefixes (docs/ARCHITECTURE.md §4.4).
const (
	pendingPrefix = "pending:"
	donePrefix    = "done:"
	// defaultTTL is how long pending/done entries live when D6 is off: long enough for a build
	// and any reordering, short enough that a stopped notifier forgets quickly.
	defaultTTL = time.Hour
)

// Exceptions that mean the instance was deleted while it was still building. Nova reports these
// as create.error, but nothing failed — the user asked for the delete (docs/DOMAIN.md §5).
var deletedDuringBuild = map[string]bool{
	"InstanceNotFound":                 true,
	"UnexpectedDeletingTaskStateError": true,
}

// Config is the correlator's slice of the configuration.
type Config struct {
	TimeoutEnabled bool          // BUILD_TIMEOUT_ENABLED (D6); unsafe with several replicas
	Timeout        time.Duration // BUILD_TIMEOUT
}

// Correlator holds no state of its own beyond a gauge: everything lives in the Store, so replicas
// can share one (Redis, Plan.md 10.1) without changing this code.
type Correlator struct {
	store   store.Store
	cfg     Config
	log     *slog.Logger
	metrics *obs.Metrics
}

func New(s store.Store, cfg Config, log *slog.Logger, m *obs.Metrics) *Correlator {
	return &Correlator{store: s, cfg: cfg, log: log, metrics: m}
}

// ttl is how long a pending build is remembered. With D6 on it must equal BUILD_TIMEOUT, because
// expiry *is* the timeout signal.
func (c *Correlator) ttl() time.Duration {
	if c.cfg.TimeoutEnabled {
		return c.cfg.Timeout
	}
	return defaultTTL
}

// Observe classifies one event. ok is false when the event carries no decision — a recorded
// `.start`, or a failure that is not one (deleted during build).
func (c *Correlator) Observe(ctx context.Context, ev model.Event) (model.Outcome, bool) {
	uuid := ev.Instance.UUID
	switch ev.EventType {
	case model.EventCreateStart:
		c.recordStart(ctx, ev)
		return model.Outcome{}, false

	case model.EventCreateEnd:
		return c.outcome(ctx, ev, model.OutcomeSuccess, model.StageNone, false), true

	case model.EventCreateError:
		if ev.Fault != nil && deletedDuringBuild[ev.Fault.Exception] {
			// Not a failure: the user deleted the instance mid-build.
			c.finish(ctx, uuid)
			c.metrics.EventsFiltered.With(model.EventCreateError).Inc()
			c.log.Info("instance deleted during build; no email",
				slog.String(obs.KeyInstanceUUID, uuid),
				slog.String("exception", ev.Fault.Exception))
			return model.Outcome{}, false
		}
		return c.outcome(ctx, ev, model.OutcomeFailure, model.StageCompute, false), true

	case model.EventBuildInstancesError:
		// The conductor reports scheduling failures here — except MaxRetriesExceeded, which means
		// a compute host did try and failed (docs/DOMAIN.md §5).
		stage := model.StageScheduling
		if ev.Fault != nil && ev.Fault.Exception == "MaxRetriesExceeded" {
			stage = model.StageCompute
		}
		return c.outcome(ctx, ev, model.OutcomeFailure, stage, false), true

	case model.EventInstanceUpdate:
		// The parser only keeps building→error updates. It is the one failure signal present on
		// every failure path, but it carries no fault — so it is a fallback, held for
		// FALLBACK_GRACE in case a detailed event follows (ADR-0009).
		return c.outcome(ctx, ev, model.OutcomeFailure, model.StageUnknown, true), true
	}
	return model.Outcome{}, false
}

// recordStart remembers when a build started, for the duration in the email and (with D6) for the
// timeout. A start that arrives after its own terminal event is ignored, so out-of-order delivery
// cannot resurrect a finished build as a stuck one.
func (c *Correlator) recordStart(ctx context.Context, ev model.Event) {
	uuid := ev.Instance.UUID
	if _, done, err := c.store.Get(ctx, donePrefix+uuid); err != nil {
		c.logStoreErr("reading done marker", uuid, err)
	} else if done {
		c.log.Debug("late create.start for a finished build; ignored",
			slog.String(obs.KeyInstanceUUID, uuid))
		return
	}
	start, err := ev.Timestamp.MarshalText() // RFC 3339 nano
	if err != nil {
		c.logStoreErr("encoding start time", uuid, err)
		return
	}
	set, err := c.store.SetNX(ctx, pendingPrefix+uuid, start, c.ttl())
	if err != nil {
		c.logStoreErr("recording pending build", uuid, err)
		return
	}
	if set {
		c.metrics.PendingBuilds.Inc()
	}
}

// outcome builds the Outcome for a terminal event and closes out the pending build.
func (c *Correlator) outcome(ctx context.Context, ev model.Event, kind model.OutcomeKind, stage model.FailureStage, fallback bool) model.Outcome {
	o := model.Outcome{Kind: kind, Stage: stage, Event: ev, Fallback: fallback}
	if start, ok := c.finish(ctx, ev.Instance.UUID); ok {
		if d := ev.Timestamp.Sub(start); d > 0 {
			o.Duration = d
		}
	}
	c.metrics.Outcomes.With(kind.String()).Inc()
	return o
}

// finish removes the pending entry and leaves a marker so a late `.start` is ignored. It reports
// the recorded start time when there was one.
func (c *Correlator) finish(ctx context.Context, uuid string) (time.Time, bool) {
	raw, found, err := c.store.Get(ctx, pendingPrefix+uuid)
	if err != nil {
		c.logStoreErr("reading pending build", uuid, err)
	}
	if err := c.store.Delete(ctx, pendingPrefix+uuid); err != nil {
		c.logStoreErr("clearing pending build", uuid, err)
	}
	if found {
		c.metrics.PendingBuilds.Dec()
	}
	if err := c.store.Set(ctx, donePrefix+uuid, []byte("1"), c.ttl()); err != nil {
		c.logStoreErr("recording done marker", uuid, err)
	}
	if !found {
		return time.Time{}, false
	}
	var start time.Time
	if err := start.UnmarshalText(raw); err != nil {
		c.logStoreErr("decoding start time", uuid, err)
		return time.Time{}, false
	}
	return start, true
}

// Expired pops pending builds whose deadline has passed. It runs on a ticker regardless of D6, so
// the pending_builds gauge and the store both stay honest; only when D6 is enabled does it turn
// those builds into Timeout outcomes (ADR-0004: with the in-memory store that needs one replica).
func (c *Correlator) Expired(ctx context.Context, now time.Time, maxOutcomes int) []model.Outcome {
	entries, err := c.store.PopExpired(ctx, pendingPrefix, now, maxOutcomes)
	if err != nil {
		c.logStoreErr("popping expired builds", "", err)
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	c.metrics.PendingBuilds.Add(int64(-len(entries)))
	if !c.cfg.TimeoutEnabled {
		return nil
	}

	outcomes := make([]model.Outcome, 0, len(entries))
	for _, e := range entries {
		uuid := e.Key[len(pendingPrefix):]
		var start time.Time
		if err := start.UnmarshalText(e.Value); err != nil {
			c.logStoreErr("decoding start time", uuid, err)
		}
		o := model.Outcome{
			Kind:  model.OutcomeTimeout,
			Stage: model.StageNone,
			Event: model.Event{
				EventType: model.EventCreateStart, // the last thing we heard about this build
				Timestamp: start,
				Instance:  model.Instance{UUID: uuid},
			},
		}
		if !start.IsZero() {
			o.Duration = now.Sub(start)
		}
		// A build that timed out is finished as far as we are concerned.
		if err := c.store.Set(ctx, donePrefix+uuid, []byte("1"), c.ttl()); err != nil {
			c.logStoreErr("recording done marker", uuid, err)
		}
		c.metrics.Outcomes.With(model.OutcomeTimeout.String()).Inc()
		c.log.Warn("build did not complete in time",
			slog.String(obs.KeyInstanceUUID, uuid),
			slog.String("timeout", c.cfg.Timeout.String()))
		outcomes = append(outcomes, o)
	}
	return outcomes
}

// Pending reports how many builds are awaiting a terminal event — the pending_builds gauge, which
// is the counter itself (obs.Gauge is atomic).
func (c *Correlator) Pending() int64 { return c.metrics.PendingBuilds.Value() }

func (c *Correlator) logStoreErr(what, uuid string, err error) {
	c.log.Warn("store error; correlation degraded", obs.Err(err),
		slog.String("operation", what), slog.String(obs.KeyInstanceUUID, uuid))
}
