// Usage:
//
//	notifier              		run the service (environment variables over CONFIG_FILE, see docs/CONFIG.md)
//	notifier healthcheck   		GET /readyz on HTTP_ADDR; exit 0 when ready, 1 otherwise
//	notifier version       		print the build version

package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/abbasimo/openstack-notifier/internal/config"
	"github.com/abbasimo/openstack-notifier/internal/obs"
)

var version = "dev"

const (
	healthcheckTimeout = 5 * time.Second
	retryInitial       = time.Second
	retryMax           = time.Minute
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

	// pipe, memory, err := build(cfg, log, metrics)
	// if err != nil {
	// 	// Templates and the parser are built before anything connects: a broken template must
	// 	// stop the process now, not at the first failure email (ARCHITECTURE §9).
	// 	log.Error("cannot start", obs.Err(err))
	// 	return 1
	// }
	// consumer := listener.New(listenerConfig(cfg), pipe.Handle, log, metrics)

	// ln, err := net.Listen("tcp", cfg.HTTPAddr)
	// if err != nil {
	// 	log.Error("cannot listen on HTTP_ADDR", obs.Err(err), "http_addr", cfg.HTTPAddr)
	// 	return 1
	// }
	// srv := obs.NewServer(cfg.HTTPAddr, obs.Handler(metrics, consumer.Ready), log)
	// srvErr := make(chan error, 1)
	// go func() {
	// 	if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
	// 		srvErr <- err
	// 	}
	// }()
	// log.Info("started", "version", version, "http_addr", ln.Addr().String(), slog.Any("config", cfg))

	// // The shutdown budget starts when the signal arrives, so drain callbacks and the HTTP
	// // shutdown share one SHUTDOWN_TIMEOUT.
	// shutdownCtx, cancelShutdown := shutdownContext(ctx, cfg.ShutdownTimeout)
	// defer cancelShutdown()

	// // Background work, all under the root context so it stops with the service.
	// var background sync.WaitGroup
	// background.Go(func() { memory.Run(ctx) })    // store sweeper
	// background.Go(func() { pipe.Timeouts(ctx) }) // D6 ticker + pending reclaim
	// background.Go(func() { pipe.Digest().Run(ctx, pipe.FlushDigest) })
	// background.Go(func() { pipe.Hold().Run(ctx, pipe.Release) })

	// consumerErr := make(chan error, 1)
	// // drain runs after the consumer stopped delivering but before the AMQP channel closes, so
	// // deliveries held by holdback/digest can still be settled (ARCHITECTURE §9, rows 14–15).
	// go func() { consumerErr <- consumer.Run(ctx, func(context.Context) { pipe.Drain(shutdownCtx) }) }()

	// code := 0
	// select {
	// case err := <-srvErr:
	// 	log.Error("http server failed", obs.Err(err))
	// 	code = 1
	// case err := <-consumerErr:
	// 	if err != nil {
	// 		log.Error("consumer failed", obs.Err(err))
	// 		code = 1
	// 	}
	// }

	// log.Info("shutting down", "timeout", cfg.ShutdownTimeout.String())
	// background.Wait()
	// if err := srv.Shutdown(shutdownCtx); err != nil {
	// 	log.Error("http shutdown failed", obs.Err(err))
	// 	code = 1
	// }
	// if errors.Is(context.Cause(shutdownCtx), context.DeadlineExceeded) {
	// 	log.Error("shutdown timed out", "timeout", cfg.ShutdownTimeout.String())
	// 	code = 1
	// }
	// log.Info("stopped", "exit_code", code)
	// return code
	return 0
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
