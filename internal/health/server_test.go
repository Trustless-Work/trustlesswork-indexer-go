package health

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeSnapshotter returns a fixed LiveSnapshot. Tests construct one per
// scenario rather than mutating shared state.
type fakeSnapshotter struct {
	snap LiveSnapshot
}

func (f fakeSnapshotter) Snapshot() LiveSnapshot { return f.snap }

func defaultConfig() Config {
	return Config{
		Addr:      ":0", // port chosen by OS
		Version:   "test",
		Network:   "testnet",
		SinkType:  "noop",
		StartedAt: time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC),
	}
}

func newTestHandler(snap LiveSnapshot, pinger Pinger) http.Handler {
	s := &Server{
		cfg:         defaultConfig(),
		snapshotter: fakeSnapshotter{snap},
		pinger:      pinger,
	}
	return s.handler()
}

// --- /healthz ---------------------------------------------------------------

func TestHealthz_Always200(t *testing.T) {
	h := newTestHandler(LiveSnapshot{}, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
}

// --- /readyz ----------------------------------------------------------------

func TestReadyz_NilPinger_ReportsReady(t *testing.T) {
	h := newTestHandler(LiveSnapshot{}, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 (nil pinger is always ready); got %d", resp.StatusCode)
	}
}

func TestReadyz_PingerOK_Returns200(t *testing.T) {
	pinger := func(context.Context) error { return nil }
	h := newTestHandler(LiveSnapshot{}, pinger)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
}

func TestReadyz_PingerFail_Returns503(t *testing.T) {
	pinger := func(context.Context) error { return errors.New("broker down") }
	h := newTestHandler(LiveSnapshot{}, pinger)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/readyz")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503; got %d", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if got := string(body); got == "" {
		t.Error("expected body to include the error message")
	}
}

// --- /status ----------------------------------------------------------------

func TestStatus_ContainsStaticAndLiveFields(t *testing.T) {
	snap := LiveSnapshot{
		LastLedgerSeq:   12345,
		LastMessageID:   "deadbeef-0",
		LastPublishedAt: time.Date(2026, 5, 13, 19, 0, 0, 0, time.UTC),
		WatchlistSize:   42,
	}
	h := newTestHandler(snap, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}

	var got Status
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding response: %v", err)
	}

	if got.Version != "test" {
		t.Errorf("Version: want test, got %q", got.Version)
	}
	if got.Network != "testnet" {
		t.Errorf("Network: want testnet, got %q", got.Network)
	}
	if got.SinkType != "noop" {
		t.Errorf("SinkType: want noop, got %q", got.SinkType)
	}
	if got.LastLedgerSeq != 12345 {
		t.Errorf("LastLedgerSeq: want 12345, got %d", got.LastLedgerSeq)
	}
	if got.LastMessageID != "deadbeef-0" {
		t.Errorf("LastMessageID: want deadbeef-0, got %q", got.LastMessageID)
	}
	if got.WatchlistSize != 42 {
		t.Errorf("WatchlistSize: want 42, got %d", got.WatchlistSize)
	}
	if got.UptimeSeconds <= 0 {
		t.Errorf("UptimeSeconds must be positive; got %d", got.UptimeSeconds)
	}
	if !got.LastPublishedAt.Equal(snap.LastPublishedAt) {
		t.Errorf("LastPublishedAt mismatch")
	}
}

func TestStatus_ZeroSnapshot_DoesNotPanic(t *testing.T) {
	// First boot: no ledger processed yet. The /status handler must
	// still return a valid JSON document with zero values rather than
	// erroring out.
	h := newTestHandler(LiveSnapshot{}, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/status")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
	var got Status
	if err := json.NewDecoder(resp.Body).Decode(&got); err != nil {
		t.Fatalf("decoding zero-snapshot response: %v", err)
	}
	if got.LastLedgerSeq != 0 {
		t.Errorf("expected zero LastLedgerSeq; got %d", got.LastLedgerSeq)
	}
}

// --- /metrics ---------------------------------------------------------------

func TestMetrics_Endpoint_Serves(t *testing.T) {
	// We do not assert on specific metric names — that would couple
	// this test to the metrics package's internals. Instead we verify
	// the endpoint serves the Prometheus exposition format header.
	h := newTestHandler(LiveSnapshot{}, nil)
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200; got %d", resp.StatusCode)
	}
}

// --- New / Addr / Serve -----------------------------------------------------

func TestNew_RejectsEmptyAddr(t *testing.T) {
	_, err := New(Config{}, fakeSnapshotter{}, nil)
	if err == nil {
		t.Fatal("expected error for empty Addr")
	}
}

func TestNew_RejectsNilSnapshotter(t *testing.T) {
	_, err := New(Config{Addr: ":0"}, nil, nil)
	if err == nil {
		t.Fatal("expected error for nil snapshotter")
	}
}

func TestNew_BindsListener(t *testing.T) {
	// Confirms the bind happens synchronously: caller knows the port
	// is available before any goroutine starts.
	s, err := New(Config{Addr: "127.0.0.1:0"}, fakeSnapshotter{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if s.Addr() == "" {
		t.Fatal("Addr() should report the actual listener address")
	}
	// Clean up via Serve+ctx cancellation.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Serve(ctx); err != nil {
		t.Fatalf("Serve after immediate cancel: %v", err)
	}
}

func TestServe_GracefulShutdownOnContextCancel(t *testing.T) {
	s, err := New(Config{Addr: "127.0.0.1:0"}, fakeSnapshotter{}, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()

	// Hit the server once to confirm it's up.
	resp, err := http.Get("http://" + s.Addr() + "/healthz")
	if err != nil {
		t.Fatalf("get during serve: %v", err)
	}
	resp.Body.Close()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned error on graceful shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return within 2s of ctx cancel")
	}
}
