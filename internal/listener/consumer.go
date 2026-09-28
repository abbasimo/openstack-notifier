// Package listener consumes deliveries from our RabbitMQ queue and settles them according to the
// Disposition its Handler returns. It treats bodies as opaque bytes.
//
// Contract: docs/ARCHITECTURE.md §4.1; topology and reconnect rationale: ADR-0006, ADR-0008;
// dispositions: ADR-0007. This is the only package besides tools/* that imports amqp091-go.
package listener

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/model"
	"github.com/abbasimo/openstack-notifier/internal/obs"
	"github.com/abbasimo/openstack-notifier/internal/util/backoff"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Handler processes one delivery body. It must not block longer than the context allows and must
// return the disposition the listener applies (or Deferred after taking ownership of ack).
type Handler func(ctx context.Context, body []byte, ack model.Acker) model.Disposition

// Config is the listener's slice of the service configuration (docs/CONFIG.md → AMQP).
type Config struct {
	URL, CAFile, Exchange, Queue, QueueType                   string
	BindingKeys                                               []string
	QueueTTL, DLQTTL, Heartbeat, DialTimeout, MessageDeadline time.Duration
	QueueMaxLength, DLQMaxLength, DeliveryLimit               int
	Prefetch, Workers                                         int
	ConsumerTag                                               string // defaults to vm-notifier@<hostname>
}

// Reconnect pacing: unlimited attempts, capped at 30s, full jitter (ADR-0008).
var reconnectPolicy = backoff.Policy{Initial: 250 * time.Millisecond, Max: 30 * time.Second}

// Consumer owns one connection at a time and rebuilds it whenever the broker goes away.
type Consumer struct {
	cfg     Config
	handle  Handler
	log     *slog.Logger
	metrics *obs.Metrics

	// dial is swapped in tests for a fake broker.
	dial   func(ctx context.Context, cfg Config) (connection, error)
	policy backoff.Policy

	ready atomic.Bool
}

// New builds a Consumer; nothing happens until Run is called.
func New(cfg Config, h Handler, log *slog.Logger, m *obs.Metrics) *Consumer {
	if cfg.ConsumerTag == "" {
		cfg.ConsumerTag = clientName()
	}
	return &Consumer{cfg: cfg, handle: h, log: log, metrics: m, dial: dial, policy: reconnectPolicy}
}

// Ready reports whether the connection and channel are open and the consumer is registered.
// It is what /readyz answers; an idle but connected consumer is ready.
func (c *Consumer) Ready() bool { return c.ready.Load() }

// Run consumes until ctx is cancelled, reconnecting forever in between. Before closing the
// channel of the final session it calls drain, so components holding a Deferred delivery can still
// settle it (ARCHITECTURE §9). drain receives a context detached from ctx; a caller that wants a
// deadline (SHUTDOWN_TIMEOUT) applies its own. Run returns nil on a clean stop.
func (c *Consumer) Run(ctx context.Context, drain func(context.Context)) error {
	defer func() {
		c.ready.Store(false)
		c.metrics.AMQPConnected.Set(0)
	}()

	policy := c.policy
	policy.OnRetry = func(attempt int, err error, delay time.Duration) {
		c.metrics.AMQPReconnects.Inc()
		c.log.Warn("broker session lost; reconnecting", obs.Err(err), "attempt", attempt, "retry_in", delay.String())
	}
	err := backoff.Retry(ctx, policy, func(ctx context.Context) error { return c.session(ctx, drain) })
	if ctx.Err() != nil {
		c.log.Info("consumer stopped")
		return nil
	}
	return err
}

// errDeliveriesClosed means the delivery channel was closed without a NotifyClose/NotifyCancel
// reason reaching us first — treated like any other session loss.
var errDeliveriesClosed = errors.New("delivery channel closed by the broker")

// session runs one connection: dial → topology → consume → dispatch until something breaks.
// It returns nil only when ctx was cancelled (clean shutdown).
func (c *Consumer) session(ctx context.Context, drain func(context.Context)) error {
	conn, err := c.dial(ctx, c.cfg)
	if err != nil {
		return fmt.Errorf("connecting to the broker: %w", err)
	}
	defer conn.Close()
	connLost := conn.NotifyClose(make(chan *amqp.Error, 1))

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("opening channel: %w", err)
	}
	// These channels must stay drained or amqp091-go blocks; they are buffered and read in the
	// dispatch loop below, and the session ends as soon as one of them fires.
	chLost := ch.NotifyClose(make(chan *amqp.Error, 1))
	cancelled := ch.NotifyCancel(make(chan string, 1))

	if err := declareTopology(ctx, ch, c.cfg); err != nil {
		if advice, ok := topologyAdvice(c.cfg, err); ok {
			c.log.Error("broker topology not usable", obs.Err(err), "advice", advice)
		}
		_ = ch.Close()
		return fmt.Errorf("declaring topology: %w", err)
	}

	// The consume context must outlive ctx: when it is cancelled, amqp091-go issues a synchronous
	// basic.cancel, which deadlocks against our own ch.Close() during shutdown. We close the
	// channel first (which ends the consumer) and release this context afterwards.
	consumeCtx, endConsume := context.WithCancel(context.WithoutCancel(ctx))
	defer endConsume()
	deliveries, err := ch.ConsumeWithContext(consumeCtx, c.cfg.Queue, c.cfg.ConsumerTag, false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return fmt.Errorf("consuming from %q: %w", c.cfg.Queue, err)
	}

	// Workers take deliveries from an unbuffered channel: prefetch is the only buffer (DD5).
	work := make(chan amqp.Delivery)
	workCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()
	var workers sync.WaitGroup
	for range max(c.cfg.Workers, 1) {
		workers.Go(func() {
			for d := range work {
				c.handleDelivery(workCtx, ch, d)
			}
		})
	}

	c.ready.Store(true)
	c.metrics.AMQPConnected.Set(1)
	c.log.Info("consuming", "queue", c.cfg.Queue, "exchange", c.cfg.Exchange,
		"binding_keys", strings.Join(c.cfg.BindingKeys, ","), "prefetch", c.cfg.Prefetch,
		"workers", c.cfg.Workers, "consumer_tag", c.cfg.ConsumerTag, "queue_type", c.cfg.QueueType)

	cause := c.dispatch(ctx, deliveries, work, connLost, chLost, cancelled)

	c.ready.Store(false)
	c.metrics.AMQPConnected.Set(0)

	// Stop consuming, let in-flight handlers observe the cancellation (their sends abort and
	// requeue), then drain the Deferred holders while the channel is still usable.
	stopWorkers()
	close(work)
	workers.Wait()
	if ctx.Err() != nil && drain != nil {
		drain(context.WithoutCancel(ctx))
	}
	_ = ch.Close()
	return cause
}

// dispatch feeds deliveries to the workers until the session ends, and returns why it ended
// (nil = ctx cancelled).
func (c *Consumer) dispatch(ctx context.Context, deliveries <-chan amqp.Delivery, work chan<- amqp.Delivery,
	connLost, chLost chan *amqp.Error, cancelled chan string) error {
	for {
		select {
		case d, ok := <-deliveries:
			if !ok {
				return errDeliveriesClosed
			}
			c.metrics.EventsReceived.Inc()
			c.metrics.MarkMessage()
			select {
			case work <- d:
			case <-ctx.Done():
				// Unacked: the broker redelivers it to us or another replica.
				return nil
			case err := <-connLost:
				return connError("connection", err)
			case err := <-chLost:
				return connError("channel", err)
			}
		case err := <-connLost:
			return connError("connection", err)
		case err := <-chLost:
			return connError("channel", err)
		case tag := <-cancelled:
			// Queue deleted, or a quorum-queue leader moved: reconnect, do not exit (Plan 3.6).
			return fmt.Errorf("consumer %q cancelled by the broker", tag)
		case <-ctx.Done():
			return nil
		}
	}
}

func connError(what string, err *amqp.Error) error {
	if err == nil {
		return fmt.Errorf("%s closed by the broker", what)
	}
	return fmt.Errorf("%s closed: %w", what, err)
}

// handleDelivery runs the handler for one delivery and applies its disposition.
func (c *Consumer) handleDelivery(ctx context.Context, ch channel, d amqp.Delivery) {
	c.metrics.InflightMessages.Inc()
	defer c.metrics.InflightMessages.Dec()

	ack := &acker{ch: ch, tag: d.DeliveryTag, log: c.log}
	hctx, cancel := context.WithTimeout(ctx, c.cfg.MessageDeadline)
	defer cancel()

	disposition := c.callHandler(hctx, d, ack)
	c.metrics.Dispositions.With(disposition.String()).Inc()

	var err error
	switch disposition {
	case model.Ack:
		err = ack.Ack()
	case model.Requeue:
		err = ack.Nack(true)
	case model.DeadLetter:
		err = ack.Nack(false)
	case model.Deferred:
		// Whoever took the acker settles it later (digest, fallback hold — ADR-0007, ADR-0009).
	}
	if err != nil {
		c.log.Error("settling delivery failed", obs.Err(err), obs.KeyDisposition, disposition.String(),
			obs.KeyMessageID, d.MessageId)
	}
}

// callHandler isolates a panicking handler from the worker pool. The pipeline recovers panics
// itself (ARCHITECTURE §5 row 13); this is the second line of defence, so one bad message can
// never take down a replica.
func (c *Consumer) callHandler(ctx context.Context, d amqp.Delivery, ack model.Acker) (disposition model.Disposition) {
	defer func() {
		if r := recover(); r != nil {
			c.metrics.PanicsRecovered.Inc()
			c.log.Error("panic in handler", "panic", fmt.Sprint(r), obs.KeyMessageID, d.MessageId,
				"stack", string(debug.Stack()))
			disposition = model.DeadLetter
		}
	}()
	return c.handle(ctx, d.Body, ack)
}

// acker settles exactly one delivery, tolerating a channel that has since closed.
type acker struct {
	ch     channel
	tag    uint64
	log    *slog.Logger
	closed atomic.Bool
}

func (a *acker) Ack() error {
	return a.settle("ack", func() error { return a.ch.Ack(a.tag, false) })
}

func (a *acker) Nack(requeue bool) error {
	name := "nack"
	if requeue {
		name = "nack-requeue"
	}
	return a.settle(name, func() error { return a.ch.Nack(a.tag, false, requeue) })
}

func (a *acker) settle(action string, fn func() error) error {
	if !a.closed.CompareAndSwap(false, true) {
		a.log.Warn("delivery already settled", "delivery_tag", a.tag, "action", action)
		return nil
	}
	switch err := fn(); {
	case err == nil:
		return nil
	case errors.Is(err, amqp.ErrClosed):
		// The channel died first; the broker has already requeued this delivery.
		a.log.Debug("settling on a closed channel, ignored", "delivery_tag", a.tag, "action", action)
		return nil
	default:
		return fmt.Errorf("%s delivery %d: %w", action, a.tag, err)
	}
}

// dial opens one connection. TLS is used for amqps:// URLs, with real verification.
func dial(_ context.Context, cfg Config) (connection, error) {
	amqpCfg := amqp.Config{
		Heartbeat:  cfg.Heartbeat,
		Locale:     "en_US",
		Dial:       amqp.DefaultDial(cfg.DialTimeout),
		Properties: amqp.Table{"connection_name": cfg.ConsumerTag, "product": "vm-notifier"},
	}
	u, err := url.Parse(cfg.URL)
	if err != nil {
		return nil, errors.New("AMQP_URL is not a valid URL") // never echo the URL: it holds the password
	}
	if u.Scheme == "amqps" {
		tlsCfg, err := tlsConfig(u.Hostname(), cfg.CAFile)
		if err != nil {
			return nil, err
		}
		amqpCfg.TLSClientConfig = tlsCfg
	}
	conn, err := amqp.DialConfig(cfg.URL, amqpCfg)
	if err != nil {
		return nil, redactURL(err, cfg.URL)
	}
	return connAdapter{conn}, nil
}

func tlsConfig(serverName, caFile string) (*tls.Config, error) {
	cfg := &tls.Config{ServerName: serverName, MinVersion: tls.VersionTLS12}
	if caFile == "" {
		return cfg, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("reading AMQP_CA_FILE: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("AMQP_CA_FILE %q contains no certificate", caFile)
	}
	cfg.RootCAs = pool
	return cfg, nil
}

// redactURL keeps a broker error from carrying the AMQP password into the logs.
func redactURL(err error, raw string) error {
	u, perr := url.Parse(raw)
	if perr != nil {
		return err
	}
	if pw, ok := u.User.Password(); ok && pw != "" && strings.Contains(err.Error(), pw) {
		return errors.New(strings.ReplaceAll(err.Error(), pw, "xxxxx"))
	}
	return err
}

// clientName identifies this process in the broker's connection list.
func clientName() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "unknown"
	}
	return "vm-notifier@" + host
}

// connAdapter lets *amqp.Connection satisfy connection (its Channel returns a concrete type).
type connAdapter struct{ *amqp.Connection }

func (c connAdapter) Channel() (channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}
	return ch, nil
}
