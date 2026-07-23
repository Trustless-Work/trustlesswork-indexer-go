package health

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/stellar/go-stellar-sdk/support/log"
)

// shutdownGrace bounds how long Serve waits for in-flight requests when
// the context is cancelled. Health responses are tiny; anything longer
// than a couple of seconds is a hung client, not a request worth saving.
const shutdownGrace = 2 * time.Second

// Handler returns the health HTTP routes for t. Split from Serve so
// tests can exercise the endpoints with httptest and no real listener.
func Handler(t *Tracker) http.Handler {
	mux := http.NewServeMux()

	// Liveness: the process is up. Nothing else — a hung loop still
	// answers 200 here, and that is by design (see package doc).
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})

	// Progress: 503 whenever the loop is not demonstrably advancing.
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		ready, reason := t.Ready()
		if !ready {
			http.Error(w, reason, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ready\n"))
	})

	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		if err := enc.Encode(t.Snapshot()); err != nil {
			http.Error(w, "encoding status", http.StatusInternalServerError)
		}
	})

	return mux
}

// Serve runs the health server on addr until ctx is cancelled. It blocks,
// so callers run it in a goroutine alongside the ingest loop.
//
// Failure to serve is logged, not returned: health is a window into the
// pipeline, and losing the window must never take down the pipeline
// itself. The trade-off is explicit — with the server down, external
// monitors see the service as unreachable and alert, which is the
// correct outcome anyway.
func Serve(ctx context.Context, addr string, t *Tracker) {
	srv := &http.Server{Addr: addr, Handler: Handler(t)}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	log.Ctx(ctx).Infof("Health server listening on %s (/healthz /readyz /status)", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Ctx(ctx).Errorf("Health server failed: %v — indexing continues without health endpoints", err)
	}
}
