package obs

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// HTTP server timeouts. Health endpoints have no long-running handlers.
const (
	readHeaderTimeout = 5 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 30 * time.Second
	idleTimeout       = 60 * time.Second
)

// Handler serves GET /healthz (process alive), GET /readyz (ready reports whether the consumer is
// connected and registered) and GET /metrics. Readiness is never traffic-based: idle is healthy.
func Handler(m *Metrics, ready func() bool) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		plain(w, http.StatusOK, "ok\n")
	})
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if ready != nil && ready() {
			plain(w, http.StatusOK, "ready\n")
			return
		}
		plain(w, http.StatusServiceUnavailable, "not ready\n")
	})
	// Timeout stays below writeTimeout so a slow gather answers 503 instead of being cut off
	// mid-body by the server (ADR-0010).
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Gatherer(), promhttp.HandlerOpts{
		ErrorHandling:       promhttp.HTTPErrorOnError,
		Timeout:             writeTimeout - 5*time.Second,
		MaxRequestsInFlight: 4,
	}))
	return mux
}

func plain(w http.ResponseWriter, code int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(code)
	_, _ = io.WriteString(w, body)
}

// NewServer wraps h in an http.Server with bounded timeouts, logging server errors through log.
func NewServer(addr string, h http.Handler, log *slog.Logger) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: readHeaderTimeout,
		ReadTimeout:       readTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
		ErrorLog:          slog.NewLogLogger(log.Handler(), slog.LevelWarn),
	}
}

// Probe does a local GET /readyz against addr and returns nil when it answers 200. It powers the
// `notifier healthcheck` subcommand, so distroless containers need no shell.
func Probe(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("parsing address %q: %w", addr, err)
	}
	switch host {
	case "", "0.0.0.0", "::", "[::]":
		host = "127.0.0.1"
	}
	url := "http://" + net.JoinHostPort(host, port) + "/readyz"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("building request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("probing %s: %w", url, err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("probing %s: status %s", url, resp.Status)
	}
	return nil
}
