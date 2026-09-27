package obs

import (
	"bytes"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)


// Prefix of every metric this service defines. Go runtime metrics keep their conventional names.
const Prefix = "vmnotifier_"

// otherLabel replaces label values outside the declared set, so cardinality stays bounded.
const otherLabel = "other"

// Counter is a monotonically increasing value.
type Counter struct{ c prometheus.Counter }

func (c *Counter) Inc()         { c.c.Inc() }
func (c *Counter) Add(n uint64) { c.c.Add(float64(n)) }

// Value reads the counter back. Used by tests; it is not on any hot path.
func (c *Counter) Value() uint64 { return uint64(readValue(c.c)) }

// Gauge is a value that goes up and down.
type Gauge struct{ g prometheus.Gauge }

func (g *Gauge) Set(n int64) { g.g.Set(float64(n)) }
func (g *Gauge) Add(n int64) { g.g.Add(float64(n)) }
func (g *Gauge) Inc()        { g.g.Inc() }
func (g *Gauge) Dec()        { g.g.Dec() }

// Value reads the gauge back. Used by tests; it is not on any hot path.
func (g *Gauge) Value() int64 { return int64(readValue(g.g)) }

// readValue extracts the current value of a counter or gauge. Any error means the metric could not
// be collected, which for our metric types cannot happen; 0 keeps the accessors total.
func readValue(m prometheus.Metric) float64 {
	var pb dto.Metric
	if err := m.Write(&pb); err != nil {
		return 0
	}
	switch {
	case pb.Counter != nil:
		return pb.Counter.GetValue()
	case pb.Gauge != nil:
		return pb.Gauge.GetValue()
	}
	return 0
}

// CounterVec is a counter with a bounded set of label combinations. Values outside the declared
// sets collapse into "other".
type CounterVec struct {
	labels  []string
	allowed []map[string]bool
	vec     *prometheus.CounterVec

	mu     sync.RWMutex
	series map[string]*Counter
}

// newCounterVec declares one label name with its allowed values per variadic argument and
// pre-creates every combination, so scrapes show 0 instead of a missing series.
func newCounterVec(name, help string, labels []string, allowed ...[]string) *CounterVec {
	v := &CounterVec{
		labels: labels,
		vec: prometheus.NewCounterVec(
			prometheus.CounterOpts{Name: Prefix + name, Help: help}, labels),
		series: map[string]*Counter{},
	}
	for _, values := range allowed {
		set := make(map[string]bool, len(values))
		for _, s := range values {
			set[s] = true
		}
		v.allowed = append(v.allowed, set)
	}
	for _, combo := range combinations(allowed) {
		v.get(combo)
	}
	return v
}

func combinations(sets [][]string) [][]string {
	out := [][]string{nil}
	for _, set := range sets {
		next := make([][]string, 0, len(out)*len(set))
		for _, prefix := range out {
			for _, v := range set {
				next = append(next, append(append([]string{}, prefix...), v))
			}
		}
		out = next
	}
	return out
}

// With returns the counter for one label combination. Unknown values become "other".
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic("obs: wrong number of label values for " + strings.Join(v.labels, ","))
	}
	normalised := make([]string, len(values))
	for i, s := range values {
		if v.allowed[i][s] {
			normalised[i] = s
		} else {
			normalised[i] = otherLabel
		}
	}
	return v.get(normalised)
}

func (v *CounterVec) get(values []string) *Counter {
	key := strings.Join(values, "\x00")
	v.mu.RLock()
	c, ok := v.series[key]
	v.mu.RUnlock()
	if ok {
		return c
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok := v.series[key]; ok {
		return c
	}
	c = &Counter{c: v.vec.WithLabelValues(values...)}
	v.series[key] = c
	return c
}

// Histogram has fixed buckets; bounds are upper-inclusive (Prometheus "le").
type Histogram struct{ h prometheus.Histogram }

// Observe records one value (seconds).
func (h *Histogram) Observe(v float64) { h.h.Observe(v) }

// ObserveSince records the time elapsed since start.
func (h *Histogram) ObserveSince(start time.Time, now time.Time) { h.Observe(now.Sub(start).Seconds()) }

// Metrics holds every metric of the service. Construct with NewMetrics and pass it around; there
// is no global registry.
type Metrics struct {
	EventsReceived      *Counter
	EventsParseFailed   *Counter
	EventsFiltered      *CounterVec // event_type
	Outcomes            *CounterVec // outcome
	DedupDropped        *Counter
	RatelimitDeferred   *Counter
	DigestSent          *Counter
	NotificationsSent   *CounterVec // notifier, result
	NotificationLatency *Histogram
	Dispositions        *CounterVec // disposition
	PanicsRecovered     *Counter
	AMQPReconnects      *Counter
	AMQPConnected       *Gauge
	InflightMessages    *Gauge
	PendingBuilds       *Gauge
	StoreEvictions      *Counter
	FallbackHeld        *Gauge
	FallbackReleased    *CounterVec // result

	now         func() time.Time
	lastMessage atomic.Int64 // unix nanoseconds
	reg         *prometheus.Registry
}

// Label values (metric labels are part of the operator contract — docs/OBSERVABILITY.md).
var (
	outcomeLabels     = []string{"success", "failure", "timeout"}
	notifierLabels    = []string{"smtp", "mock"}
	sendResultLabels  = []string{"ok", "transient_failure", "permanent_failure"}
	dispositionLabels = []string{"ack", "requeue", "dead_letter", "deferred"}
	fallbackResults   = []string{"sent", "duplicate"}
	latencyBuckets    = []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
)

// NewMetrics creates the metric set. eventTypes bounds the event_type label (values outside it and
// outside the allow-list collapse to "other"); now defaults to time.Now.
func NewMetrics(eventTypes []string, now func() time.Time) *Metrics {
	if now == nil {
		now = time.Now
	}
	m := &Metrics{now: now, reg: prometheus.NewRegistry()}
	m.lastMessage.Store(now().UnixNano()) // age counts from startup until the first message

	counter := func(name, help string) *Counter {
		return &Counter{c: prometheus.NewCounter(prometheus.CounterOpts{Name: Prefix + name, Help: help})}
	}
	gauge := func(name, help string) *Gauge {
		return &Gauge{g: prometheus.NewGauge(prometheus.GaugeOpts{Name: Prefix + name, Help: help})}
	}

	m.EventsReceived = counter("events_received_total", "Notifications received from the broker.")
	m.EventsParseFailed = counter("events_parse_failed_total", "Notifications that could not be parsed (dead-lettered).")
	m.EventsFiltered = newCounterVec("events_filtered_total", "Notifications dropped by the filter, by event type.",
		[]string{"event_type"}, append(append([]string{}, eventTypes...), otherLabel))
	m.Outcomes = newCounterVec("outcomes_total", "Build outcomes derived from notifications.",
		[]string{"outcome"}, outcomeLabels)
	m.DedupDropped = counter("dedup_dropped_total", "Outcomes suppressed as duplicates.")
	m.RatelimitDeferred = counter("ratelimit_deferred_total", "Outcomes deferred into a digest by the rate limiter.")
	m.DigestSent = counter("digest_sent_total", "Digest emails sent.")
	m.NotificationsSent = newCounterVec("notifications_sent_total", "Notification send attempts by notifier and result.",
		[]string{"notifier", "result"}, notifierLabels, sendResultLabels)
	m.NotificationLatency = &Histogram{h: prometheus.NewHistogram(prometheus.HistogramOpts{
		Name: Prefix + "notification_latency_seconds", Help: "Time to send one notification.", Buckets: latencyBuckets})}
	m.Dispositions = newCounterVec("dispositions_total", "Delivery dispositions decided by the pipeline.",
		[]string{"disposition"}, dispositionLabels)
	m.PanicsRecovered = counter("panics_recovered_total", "Panics recovered in the pipeline.")
	m.AMQPReconnects = counter("amqp_reconnects_total", "Broker connection attempts after the first success.")
	m.AMQPConnected = gauge("amqp_connected", "1 when the consumer is connected and registered, else 0.")
	m.InflightMessages = gauge("inflight_messages", "Deliveries currently being handled.")
	m.PendingBuilds = gauge("pending_builds", "Builds awaiting a terminal event (correlator state).")
	m.StoreEvictions = counter("store_evictions_total", "Store entries evicted because STORE_MAX_ENTRIES was reached.")
	m.FallbackHeld = gauge("fallback_held", "Fallback outcomes held for FALLBACK_GRACE.")
	m.FallbackReleased = newCounterVec("fallback_released_total", "Fallback outcomes released from the hold, by result.",
		[]string{"result"}, fallbackResults)

	m.reg.MustRegister(
		m.EventsReceived.c, m.EventsParseFailed.c, m.EventsFiltered.vec, m.Outcomes.vec,
		m.DedupDropped.c, m.RatelimitDeferred.c, m.DigestSent.c, m.NotificationsSent.vec,
		m.NotificationLatency.h, m.Dispositions.vec, m.PanicsRecovered.c, m.AMQPReconnects.c,
		m.AMQPConnected.g, m.InflightMessages.g, m.PendingBuilds.g, m.StoreEvictions.c,
		m.FallbackHeld.g, m.FallbackReleased.vec,
		prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name: Prefix + "last_message_age_seconds",
				Help: "Seconds since the last delivery (since startup if none).",
			},
			func() float64 { return m.now().Sub(time.Unix(0, m.lastMessage.Load())).Seconds() }),
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}

// MarkMessage records that a delivery just arrived (drives last_message_age_seconds).
func (m *Metrics) MarkMessage() { m.lastMessage.Store(m.now().UnixNano()) }

// Gatherer exposes the registry so cmd/notifier can serve it over HTTP.
func (m *Metrics) Gatherer() prometheus.Gatherer { return m.reg }

// WriteTo writes the Prometheus text exposition of every metric. Families come out sorted by name.
func (m *Metrics) WriteTo(w io.Writer) (int64, error) {
	families, err := m.reg.Gather()
	if err != nil {
		return 0, err
	}
	var b bytes.Buffer
	for _, f := range families {
		if _, err := expfmt.MetricFamilyToText(&b, f); err != nil {
			return 0, err
		}
	}
	n, err := w.Write(b.Bytes())
	return int64(n), err
}
