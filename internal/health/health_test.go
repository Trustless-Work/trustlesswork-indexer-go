package health

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// fakeClock drives the Tracker deterministically.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func newFakeClock() *fakeClock               { return &fakeClock{t: time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)} }
func newTestTracker(c *fakeClock) *Tracker   { return newTrackerAt("testnet", c.now) }
func get(h http.Handler, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func TestReadyz_LifecycleOfTheLoop(t *testing.T) {
	clock := newFakeClock()
	tr := newTestTracker(clock)
	h := Handler(tr)

	// Before the first ledger: not ready, with the reason saying so.
	if rr := get(h, "/readyz"); rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("before first ledger: /readyz = %d, want 503", rr.Code)
	}

	// A processed ledger makes the loop demonstrably alive.
	tr.RecordLedger(Progress{LedgerSeq: 100, LedgerClosedAt: clock.now().Add(-5 * time.Second)})
	if rr := get(h, "/readyz"); rr.Code != http.StatusOK {
		t.Fatalf("after a ledger: /readyz = %d, want 200", rr.Code)
	}

	// Inside the tolerance window it stays ready...
	clock.advance(readyStaleAfter)
	if rr := get(h, "/readyz"); rr.Code != http.StatusOK {
		t.Fatalf("at the tolerance edge: /readyz = %d, want 200", rr.Code)
	}

	// ...and one tick past it, the stall is visible.
	clock.advance(time.Second)
	if rr := get(h, "/readyz"); rr.Code != http.StatusOK {
		if rr.Code != http.StatusServiceUnavailable {
			t.Fatalf("stalled: /readyz = %d, want 503", rr.Code)
		}
	} else {
		t.Fatal("stalled loop still reported ready")
	}
}

func TestHealthz_AlwaysOK(t *testing.T) {
	h := Handler(newTestTracker(newFakeClock()))
	if rr := get(h, "/healthz"); rr.Code != http.StatusOK {
		t.Fatalf("/healthz = %d, want 200", rr.Code)
	}
}

func TestStatus_ReportsProgressNumbers(t *testing.T) {
	clock := newFakeClock()
	tr := newTestTracker(clock)

	closedAt := clock.now().Add(-6 * time.Second)
	tr.RecordLedger(Progress{
		LedgerSeq: 63613979, LedgerClosedAt: closedAt,
		Duration: 12 * time.Millisecond, KnownEscrows: 3, Events: 2, StateChanges: 1, Gaps: 1,
	})
	tr.RecordLedger(Progress{
		LedgerSeq: 63613980, LedgerClosedAt: closedAt.Add(5 * time.Second),
		Duration: 15 * time.Millisecond, KnownEscrows: 3, Events: 1, StateChanges: 1, Gaps: 1,
	})

	rr := get(Handler(tr), "/status")
	if rr.Code != http.StatusOK {
		t.Fatalf("/status = %d, want 200", rr.Code)
	}
	var got Status
	if err := json.Unmarshal(rr.Body.Bytes(), &got); err != nil {
		t.Fatalf("decoding /status: %v", err)
	}

	if got.CurrentLedger != 63613980 {
		t.Errorf("current_ledger = %d, want 63613980", got.CurrentLedger)
	}
	// Totals accumulate across ledgers; point-in-time fields do not.
	if got.EventsPublished != 3 || got.StateChangesPublished != 2 {
		t.Errorf("totals = %d events / %d states, want 3 / 2", got.EventsPublished, got.StateChangesPublished)
	}
	if got.KnownEscrows != 3 || got.Gaps != 1 {
		t.Errorf("known_escrows=%d gaps=%d, want 3 and 1", got.KnownEscrows, got.Gaps)
	}
	if got.LedgerAgeSeconds != 1 { // closed 1s before the (frozen) clock
		t.Errorf("ledger_age_seconds = %v, want 1", got.LedgerAgeSeconds)
	}
	if !got.Ready {
		t.Error("status.ready = false right after processing a ledger")
	}
}
