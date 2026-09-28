package listener

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// channel is the subset of *amqp.Channel this package uses, so tests can supply a fake broker.
type channel interface {
	ExchangeDeclarePassive(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	ExchangeDeclare(name, kind string, durable, autoDelete, internal, noWait bool, args amqp.Table) error
	QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) (amqp.Queue, error)
	QueueBind(name, key, exchange string, noWait bool, args amqp.Table) error
	Qos(prefetchCount, prefetchSize int, global bool) error
	ConsumeWithContext(ctx context.Context, queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	NotifyCancel(c chan string) chan string
	Ack(tag uint64, multiple bool) error
	Nack(tag uint64, multiple, requeue bool) error
	Close() error
}

// connection is the subset of *amqp.Connection this package uses.
type connection interface {
	Channel() (channel, error)
	NotifyClose(c chan *amqp.Error) chan *amqp.Error
	Close() error
}

// DLXName and DLQName are derived from the queue name, so one AMQP_QUEUE names the whole set.
func (c Config) DLXName() string { return c.Queue + ".dlx" }
func (c Config) DLQName() string { return c.Queue + ".dlq" }

func (c Config) quorum() bool { return c.QueueType == "quorum" }

// queueArgs are the arguments of our work queue: bounded in time and length, overflowing into the
// dead-letter exchange (ADR-0006). Both bounds matter: a stopped notifier must never fill the
// broker's disk and block the OpenStack control plane.
func (c Config) queueArgs() amqp.Table {
	args := amqp.Table{
		"x-queue-type":           c.QueueType,
		"x-message-ttl":          ms(c.QueueTTL),
		"x-max-length":           int32(c.QueueMaxLength),
		"x-overflow":             "drop-head",
		"x-dead-letter-exchange": c.DLXName(),
	}
	if c.quorum() {
		args["x-delivery-limit"] = int32(c.DeliveryLimit)
	}
	return args
}

// dlqArgs cap the dead-letter queue itself — TTL expiry and overflow of the work queue are
// dead-lettered, so the DLQ needs its own bounds and has no DLX of its own.
func (c Config) dlqArgs() amqp.Table {
	args := amqp.Table{
		"x-queue-type":  c.QueueType,
		"x-message-ttl": ms(c.DLQTTL),
		"x-max-length":  int32(c.DLQMaxLength),
		"x-overflow":    "drop-head",
	}
	if c.quorum() {
		args["x-delivery-limit"] = int32(c.DeliveryLimit)
	}
	return args
}

func ms(d time.Duration) int32 { return int32(d.Milliseconds()) }

// declareTopology is idempotent and runs on every (re)connect: passive check of Nova's exchange,
// then our own DLX, DLQ, queue, bindings and prefetch.
func declareTopology(ctx context.Context, ch channel, cfg Config) error {
	// Nova owns the exchange: check that it exists, never declare it (ADR-0006).
	if err := ch.ExchangeDeclarePassive(cfg.Exchange, amqp.ExchangeTopic, true, false, false, false, nil); err != nil {
		return fmt.Errorf("checking exchange %q: %w", cfg.Exchange, err)
	}
	if err := ch.ExchangeDeclare(cfg.DLXName(), amqp.ExchangeFanout, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declaring dead-letter exchange %q: %w", cfg.DLXName(), err)
	}
	if _, err := ch.QueueDeclare(cfg.DLQName(), true, false, false, false, cfg.dlqArgs()); err != nil {
		return fmt.Errorf("declaring dead-letter queue %q: %w", cfg.DLQName(), err)
	}
	if err := ch.QueueBind(cfg.DLQName(), "", cfg.DLXName(), false, nil); err != nil {
		return fmt.Errorf("binding %q to %q: %w", cfg.DLQName(), cfg.DLXName(), err)
	}
	if _, err := ch.QueueDeclare(cfg.Queue, true, false, false, false, cfg.queueArgs()); err != nil {
		return fmt.Errorf("declaring queue %q: %w", cfg.Queue, err)
	}
	for _, key := range cfg.BindingKeys {
		if err := ch.QueueBind(cfg.Queue, key, cfg.Exchange, false, nil); err != nil {
			return fmt.Errorf("binding %q to %q with key %q: %w", cfg.Queue, cfg.Exchange, key, err)
		}
	}
	// Per-consumer prefetch: global QoS is not supported on quorum queues.
	if err := ch.Qos(cfg.Prefetch, 0, false); err != nil {
		return fmt.Errorf("setting prefetch %d: %w", cfg.Prefetch, err)
	}
	_ = ctx // reserved: the amqp091 topology calls have no context variant
	return nil
}

// amqpCode returns the AMQP error code of err, or 0.
func amqpCode(err error) int {
	var ae *amqp.Error
	if errors.As(err, &ae) {
		return ae.Code
	}
	return 0
}

// topologyAdvice explains the two failures an operator can act on: Nova's exchange missing (the
// notifier was started before Nova published anything) and our queue existing with different
// arguments (a configuration change needs the queue deleted or migrated).
func topologyAdvice(cfg Config, err error) (string, bool) {
	switch amqpCode(err) {
	case amqp.NotFound:
		return fmt.Sprintf("exchange %q does not exist yet; waiting for Nova to declare it — check nova.conf [oslo_messaging_notifications] driver/topics", cfg.Exchange), true
	case amqp.PreconditionFailed:
		return fmt.Sprintf("queue %q (or %q) exists with different arguments; delete or migrate it, see docs/MESSAGING.md", cfg.Queue, cfg.DLQName()), true
	}
	return "", false
}
