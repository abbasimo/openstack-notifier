// Usage:
//
//	notifier              		run the service (environment variables over CONFIG_FILE, see docs/CONFIG.md)
//	notifier healthcheck   		GET /readyz on HTTP_ADDR; exit 0 when ready, 1 otherwise
//	notifier version       		print the build version

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/config"
	"github.com/abbasimo/openstack-notifier/internal/correlator"
	"github.com/abbasimo/openstack-notifier/internal/dedup"
	"github.com/abbasimo/openstack-notifier/internal/holdback"
	"github.com/abbasimo/openstack-notifier/internal/listener"
	"github.com/abbasimo/openstack-notifier/internal/notify"
	"github.com/abbasimo/openstack-notifier/internal/obs"
	"github.com/abbasimo/openstack-notifier/internal/parser"
	"github.com/abbasimo/openstack-notifier/internal/pipeline"
	"github.com/abbasimo/openstack-notifier/internal/ratelimit"
	"github.com/abbasimo/openstack-notifier/internal/render"

	"github.com/abbasimo/openstack-notifier/internal/store"
	"github.com/abbasimo/openstack-notifier/internal/util/backoff"
)

var version = "dev"

const (
	healthcheckTimeout = 5 * time.Second
	retryInitial       = time.Second
	retryMax           = time.Minute
	mockKeep = 100
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	context.AfterFunc(ctx, stop)
	os.Exit(run(ctx, os.Args[1:], os.LookupEnv, os.Stderr))
}

func run(ctx context.Context, args []string, lookup func(string) (string, bool), stderr io.Writer) int {
	switch {
	case len(args) == 1 && args[0] == "healthcheck":
		return healthcheck(ctx, lookup, stderr)
	case len(args) == 1 && (args[0] == "version" || args[0] == "-version" || args[0] == "--version"):
		fmt.Fprintln(stderr, version)
		return 0
	case len(args) > 0:
		fmt.Fprintf(stderr, "usage: notifier [healthcheck|version]\nconfiguration: see docs/CONFIG.md\n")
		return 2
	}

	cfg, err := config.LoadFile(lookup)
	if err != nil {
		attrs := []any{slog.Any("problems", strings.Split(err.Error(), "\n"))}
		if path, _ := lookup("CONFIG_FILE"); path != "" {
			attrs = append(attrs, slog.String("config_file", path))
		}
		obs.NewLogger(stderr, slog.LevelInfo).Error("invalid configuration", attrs...)
		return 2
	}
	log := obs.NewLogger(stderr, cfg.SlogLevel())
	for _, w := range cfg.Warnings() {
		log.Warn("configuration warning", "warning", w)
	}

	metrics := obs.NewMetrics(cfg.EventTypes, time.Now)

	pipe, memory, err := build(cfg, log, metrics)
	if err != nil {
		// Templates and the parser are built before anything connects: a broken template must
		// stop the process now, not at the first failure email (ARCHITECTURE §9).
		log.Error("cannot start", obs.Err(err))
		return 1
	}
	consumer := listener.New(listenerConfig(cfg), pipe.Handle, log, metrics)

	ln, err := net.Listen("tcp", cfg.HTTPAddr)
	if err != nil {
		log.Error("cannot listen on HTTP_ADDR", obs.Err(err), "http_addr", cfg.HTTPAddr)
		return 1
	}
	srv := obs.NewServer(cfg.HTTPAddr, obs.Handler(metrics, consumer.Ready), log)
	srvErr := make(chan error, 1)
	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			srvErr <- err
		}
	}()
	log.Info("started", "version", version, "http_addr", ln.Addr().String(), slog.Any("config", cfg))

	// The shutdown budget starts when the signal arrives, so drain callbacks and the HTTP
	// shutdown share one SHUTDOWN_TIMEOUT.
	shutdownCtx, cancelShutdown := shutdownContext(ctx, cfg.ShutdownTimeout)
	defer cancelShutdown()

	// Background work, all under the root context so it stops with the service.
	var background sync.WaitGroup
	background.Go(func() { memory.Run(ctx) })    // store sweeper
	background.Go(func() { pipe.Timeouts(ctx) }) // D6 ticker + pending reclaim
	background.Go(func() { pipe.Digest().Run(ctx, pipe.FlushDigest) })
	background.Go(func() { pipe.Hold().Run(ctx, pipe.Release) })

	consumerErr := make(chan error, 1)
	// drain runs after the consumer stopped delivering but before the AMQP channel closes, so
	// deliveries held by holdback/digest can still be settled (ARCHITECTURE §9, rows 14–15).
	go func() { consumerErr <- consumer.Run(ctx, func(context.Context) { pipe.Drain(shutdownCtx) }) }()

	code := 0
	select {
	case err := <-srvErr:
		log.Error("http server failed", obs.Err(err))
		code = 1
	case err := <-consumerErr:
		if err != nil {
			log.Error("consumer failed", obs.Err(err))
			code = 1
		}
	}

	log.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())
	background.Wait()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Error("http shutdown failed", obs.Err(err))
		code = 1
	}
	if errors.Is(context.Cause(shutdownCtx), context.DeadlineExceeded) {
		log.Error("shutdown timed out", "timeout", cfg.ShutdownTimeout.String())
		code = 1
	}
	log.Info("stopped", "exit_code", code)
	return code
}

func healthcheck(ctx context.Context, lookup func(string) (string, bool), stderr io.Writer) int {
	sources, _ := config.Sources(lookup)
	addr, ok := sources("HTTP_ADDR")
	if !ok || addr == "" {
		addr = config.Default("HTTP_ADDR")
	}
	ctx, cancel := context.WithTimeout(ctx, healthcheckTimeout)
	defer cancel()
	if err := obs.Probe(ctx, addr); err != nil {
		fmt.Fprintln(stderr, err)
		return 1
	}
	return 0
}

func build(cfg config.Config, log *slog.Logger, m *obs.Metrics) (*pipeline.Pipeline, *store.Memory, error) {
	p, err := parser.New(cfg.MaxBodyBytes, cfg.EventTypes)
	if err != nil {
		return nil, nil, fmt.Errorf("building the parser: %w", err)
	}
	renderer, err := render.New(cfg.TemplatesDir,
		render.Limits{TracebackBytes: cfg.TracebackMaxBytes, TextBytes: cfg.TextMaxBytes}, time.Now)
	if err != nil {
		return nil, nil, fmt.Errorf("parsing templates: %w", err)
	}
	notifier, err := notifier(cfg, log, m)
	if err != nil {
		return nil, nil, err
	}

	memory := store.NewMemory(cfg.StoreMaxEntries, func(n int) { m.StoreEvictions.Add(uint64(n)) })
	return pipeline.New(pipeline.Deps{
		Parser: p,
		Correlator: correlator.New(memory, correlator.Config{
			TimeoutEnabled: cfg.BuildTimeoutEnabled,
			Timeout:        cfg.BuildTimeout,
		}, log, m),
		Dedup:      dedup.New(memory, m, log, cfg.MessageDeadline),
		Hold:       holdback.New(cfg.FallbackMax, cfg.FallbackGrace, m),
		Bucket:     ratelimit.NewBucket(cfg.RatePerMin, cfg.RateBurst),
		Digest:     ratelimit.NewDigest(cfg.DigestMax, cfg.DigestInterval, m),
		Renderer:   renderer,
		Notifier:   notifier,
		Log:        log,
		Metrics:    m,
		Now:        time.Now,
		EventTypes: cfg.EventTypes,
	}), memory, nil
}

func notifier(cfg config.Config, log *slog.Logger, m *obs.Metrics) (notify.Notifier, error) {
	observe := func(name, result string, latency time.Duration) {
		m.NotificationsSent.With(name, result).Inc()
		m.NotificationLatency.Observe(latency.Seconds())
	}

	var base notify.Notifier
	switch cfg.Notifier {
	case "mock":
		 base = notify.NewMock(mockKeep, cfg.MockEMLDir, log)
	default:
		smtp, err := notify.NewSMTP(notify.SMTPConfig{
			Addr:     cfg.SMTPAddr,
			TLSMode:  cfg.SMTPTLS,
			CAFile:   cfg.SMTPCAFile,
			Username: cfg.SMTPUsername.Reveal(), // the only place the SMTP credentials are read
			Password: cfg.SMTPPassword.Reveal(),
			From:     cfg.MailFrom,
			To:       cfg.MailTo,
			Timeout:  cfg.SMTPTimeout,
		})
		if err != nil {
			return nil, fmt.Errorf("configuring SMTP: %w", err)
		}
		base = smtp
	}

	return notify.Retry(notify.Observe(base, observe), backoff.Policy{
		Initial:    retryInitial,
		Max:        retryMax,
		MaxElapsed: cfg.NotifyRetryMaxElapsed,
	}), nil
}

// listenerConfig is the listener's slice of the configuration.
func listenerConfig(cfg config.Config) listener.Config {
	return listener.Config{
		URL:             cfg.AMQPURL.Reveal(), // the only place the AMQP password is read
		CAFile:          cfg.AMQPCAFile,
		Exchange:        cfg.AMQPExchange,
		Queue:           cfg.AMQPQueue,
		QueueType:       cfg.AMQPQueueType,
		BindingKeys:     cfg.AMQPBindingKeys,
		QueueTTL:        cfg.AMQPQueueTTL,
		DLQTTL:          cfg.AMQPDLQTTL,
		Heartbeat:       cfg.AMQPHeartbeat,
		DialTimeout:     cfg.AMQPDialTimeout,
		MessageDeadline: cfg.MessageDeadline,
		QueueMaxLength:  cfg.AMQPQueueMaxLength,
		DLQMaxLength:    cfg.AMQPDLQMaxLength,
		DeliveryLimit:   cfg.AMQPDeliveryLimit,
		Prefetch:        cfg.AMQPPrefetch,
		Workers:         cfg.Workers,
	}
}

// shutdownContext returns a context that stays alive while parent is alive and is cancelled
// timeout after parent is done — the grace period for draining and stopping.
func shutdownContext(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancelCause(context.WithoutCancel(parent))
	stop := context.AfterFunc(parent, func() {
		time.AfterFunc(timeout, func() { cancel(context.DeadlineExceeded) })
	})
	return ctx, func() {
		stop()
		cancel(context.Canceled)
	}
}
