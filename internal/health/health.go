// Package health exposes the Indexer's liveness and progress over HTTP.
//
// It answers the operational question the log stream cannot: "is the
// service advancing RIGHT NOW, and how far behind the chain is it?" —
// without grepping deploy logs. Three endpoints, one semantic each:
//
//   - /healthz — liveness: the process is up. Always 200. Restarting on
//     this signal alone is never correct; a hung loop still "lives".
//   - /readyz  — progress: a ledger was processed recently. 503 when the
//     loop stalls, whatever the cause (hung RPC, broker backpressure,
//     bug). This is the endpoint for platform healthchecks and uptime
//     monitors.
//   - /status  — the numbers a human wants first in an incident: cursor,
//     ledger age, escrows tracked, publish totals, recorded gaps.
//
// The Tracker is the write side: the ingest loop reports each processed
// ledger and the HTTP handlers read a consistent snapshot. One writer,
// many readers, one mutex — deliberately boring.
package health

import (
	"sync"
	"time"
)

// readyStaleAfter is how long /readyz tolerates no progress before
// reporting 503. Ledgers close every ~5-6s; 60s of silence means at
// least ~10 missed ledgers — a stall, not jitter. Catch-up is never
// misread as a stall: a catching-up loop processes ledgers FASTER than
// the chain produces them, refreshing the timestamp constantly.
const readyStaleAfter = 60 * time.Second

// Progress is one processed ledger's report from the ingest loop.
type Progress struct {
	LedgerSeq      uint32
	LedgerClosedAt time.Time
	Duration       time.Duration
	KnownEscrows   int
	Events         int
	StateChanges   int
	Gaps           int
}

// Status is the read-side snapshot served by /status. Ages are computed
// at snapshot time so the JSON is directly actionable without the reader
// doing clock math.
type Status struct {
	Network string `json:"network"`
	// Ready mirrors /readyz so one /status call tells the whole story.
	Ready       bool   `json:"ready"`
	ReadyReason string `json:"ready_reason,omitempty"`

	CurrentLedger uint32 `json:"current_ledger"`
	// LedgerAgeSeconds is how far behind the chain the last processed
	// ledger's data is — the freshness number downstream consumers care
	// about. At the tip it hovers around one close interval (~5-6s).
	LedgerAgeSeconds       float64 `json:"ledger_age_seconds"`
	SecondsSinceLastLedger float64 `json:"seconds_since_last_ledger"`
	LastLedgerMillis       float64 `json:"last_ledger_ms"`

	KnownEscrows          int `json:"known_escrows"`
	EventsPublished       int `json:"events_published_total"`
	StateChangesPublished int `json:"state_changes_published_total"`
	Gaps                  int `json:"gaps_recorded"`

	UptimeSeconds float64 `json:"uptime_seconds"`
}

// Tracker accumulates progress reports from the ingest loop and produces
// consistent snapshots for the HTTP handlers. Safe for concurrent use.
type Tracker struct {
	// now is injected for tests; time.Now in production.
	now func() time.Time

	mu              sync.Mutex
	network         string
	startedAt       time.Time
	currentLedger   uint32
	ledgerClosedAt  time.Time
	lastProcessedAt time.Time
	lastDuration    time.Duration
	knownEscrows    int
	eventsTotal     int
	statesTotal     int
	gaps            int
}

// NewTracker builds a Tracker for the given network label.
func NewTracker(network string) *Tracker {
	return newTrackerAt(network, time.Now)
}

// newTrackerAt is the test seam: it lets tests drive the clock.
func newTrackerAt(network string, now func() time.Time) *Tracker {
	return &Tracker{now: now, network: network, startedAt: now()}
}

// RecordLedger reports one processed ledger. Called by the ingest loop
// after the ledger's facts were published and the cursor saved.
func (t *Tracker) RecordLedger(p Progress) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.currentLedger = p.LedgerSeq
	t.ledgerClosedAt = p.LedgerClosedAt
	t.lastProcessedAt = t.now()
	t.lastDuration = p.Duration
	t.knownEscrows = p.KnownEscrows
	t.eventsTotal += p.Events
	t.statesTotal += p.StateChanges
	t.gaps = p.Gaps
}

// Snapshot returns the current Status.
func (t *Tracker) Snapshot() Status {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	ready, reason := t.readiness(now)

	s := Status{
		Network:               t.network,
		Ready:                 ready,
		ReadyReason:           reason,
		CurrentLedger:         t.currentLedger,
		LastLedgerMillis:      float64(t.lastDuration) / float64(time.Millisecond),
		KnownEscrows:          t.knownEscrows,
		EventsPublished:       t.eventsTotal,
		StateChangesPublished: t.statesTotal,
		Gaps:                  t.gaps,
		UptimeSeconds:         now.Sub(t.startedAt).Seconds(),
	}
	if !t.ledgerClosedAt.IsZero() {
		s.LedgerAgeSeconds = now.Sub(t.ledgerClosedAt).Seconds()
	}
	if !t.lastProcessedAt.IsZero() {
		s.SecondsSinceLastLedger = now.Sub(t.lastProcessedAt).Seconds()
	}
	return s
}

// Ready reports whether the loop is advancing, with a human-readable
// reason when it is not. This is the /readyz semantic.
func (t *Tracker) Ready() (bool, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readiness(t.now())
}

// readiness holds the actual rule; callers must hold t.mu.
func (t *Tracker) readiness(now time.Time) (bool, string) {
	if t.lastProcessedAt.IsZero() {
		return false, "no ledger processed yet"
	}
	if silent := now.Sub(t.lastProcessedAt); silent > readyStaleAfter {
		return false, "no ledger processed in " + silent.Round(time.Second).String()
	}
	return true, ""
}
