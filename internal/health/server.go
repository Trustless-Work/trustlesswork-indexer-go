package health

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Default HTTP server timeouts. Generous enough for slow scrapers, tight
// enough to bound resource usage on a stuck client.
const (
	defaultReadTimeout     = 5 * time.Second
	defaultWriteTimeout    = 5 * time.Second
	defaultIdleTimeout     = 60 * time.Second
	defaultShutdownTimeout = 5 * time.Second
)

// Config holds the static configuration of the health server.
// Static = doesn't change after construction. Live data (cursor,
// watchlist size) comes from the Snapshotter at request time.
type Config struct {
	// Addr is the listen address, e.g. ":8080" or "0.0.0.0:8080".
	Addr string

	// Version is reported in /status. Inject at build time via
	// -ldflags or read from a build/ package; "dev" is fine in dev.
	Version string

	// Network is the short network label ("testnet", "mainnet").
	Network string

	// SinkType is the configured sink type, surfaced in /status for
	// operators inspecting which destination the Indexer is publishing
	// to ("noop", "rabbitmq").
	SinkType string

	// StartedAt is the wall-clock time at which the Indexer process
	// started. Used to compute the uptime in /status.
	StartedAt time.Time

	// ShutdownTimeout bounds how long graceful shutdown waits for
	// in-flight requests to complete. Defaults to 5s when zero.
	ShutdownTimeout time.Duration
}

// Server is the health/status/metrics HTTP server.
type Server struct {
	cfg         Config
	snapshotter Snapshotter
	pinger      Pinger
	listener    net.Listener
	httpServer  *http.Server
}

// New constructs a Server and synchronously opens its listener. A
// failure to bind (typical cause: port already in use) is returned
// immediately so the caller can fail-fast at boot rather than
// discover the conflict later from a goroutine.
//
// snapshotter is required (used by /status). pinger may be nil; when
// nil, /readyz reports the Indexer as always ready (appropriate for
// sinks like noop that have no probeable failure mode).
func New(cfg Config, snapshotter Snapshotter, pinger Pinger) (*Server, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("health.Config.Addr is required")
	}
	if snapshotter == nil {
		return nil, fmt.Errorf("health.New requires a Snapshotter")
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = defaultShutdownTimeout
	}
	if cfg.StartedAt.IsZero() {
		cfg.StartedAt = time.Now().UTC()
	}

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return nil, fmt.Errorf("listening on %q: %w", cfg.Addr, err)
	}

	s := &Server{
		cfg:         cfg,
		snapshotter: snapshotter,
		pinger:      pinger,
		listener:    ln,
	}
	s.httpServer = &http.Server{
		Handler:      s.handler(),
		ReadTimeout:  defaultReadTimeout,
		WriteTimeout: defaultWriteTimeout,
		IdleTimeout:  defaultIdleTimeout,
	}
	return s, nil
}

// Addr returns the actual address the server is listening on. Useful
// when Config.Addr was ":0" and the OS picked a port (e.g. in tests).
func (s *Server) Addr() string {
	return s.listener.Addr().String()
}

// Serve blocks until ctx is cancelled or the server fails. On ctx
// cancellation it issues a graceful shutdown with a bounded timeout;
// in-flight requests are given that long to complete before the
// listener is closed forcibly.
//
// A nil return means clean shutdown via ctx. An error indicates the
// server stopped serving for another reason (typically listener
// closure unrelated to Shutdown).
func (s *Server) Serve(ctx context.Context) error {
	errCh := make(chan error, 1)
	go func() {
		// Serve returns ErrServerClosed when Shutdown is called; that
		// is the expected clean-exit signal, not a failure.
		err := s.httpServer.Serve(s.listener)
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		close(errCh)
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
		defer cancel()
		return s.httpServer.Shutdown(shutdownCtx)
	case err, ok := <-errCh:
		if !ok {
			return nil
		}
		return err
	}
}

// handler builds the route tree. Exposed as a method on the Server so
// it can be reused by tests via httptest.NewServer(s.handler()).
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/readyz", s.handleReadyz)
	mux.HandleFunc("/status", s.handleStatus)
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

// handleHealthz reports liveness. Always 200 as long as the process
// can answer HTTP. Used by Kubernetes-style liveness probes.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleReadyz reports readiness. 200 when the pinger (typically the
// sink's Ping) succeeds; 503 with the error message otherwise. A nil
// pinger is treated as "always ready".
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	if s.pinger == nil {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
		return
	}
	if err := s.pinger(r.Context()); err != nil {
		http.Error(w, fmt.Sprintf("not ready: %v\n", err), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ready\n"))
}

// handleStatus returns a JSON snapshot for operators. See the Status
// type doc for the field semantics. Errors during encoding are
// effectively unobservable (we cannot rewrite the headers after the
// fact); they're rare and logged elsewhere if needed.
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	status := buildStatus(s.cfg, s.snapshotter.Snapshot(), time.Now().UTC())

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(status)
}
