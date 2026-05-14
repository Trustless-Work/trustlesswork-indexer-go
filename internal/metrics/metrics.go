// Package metrics exposes the Indexer's Prometheus metric surface as
// named recorder functions. Other packages call these functions; they
// do NOT import prometheus/client_golang directly. Centralization gives
// us three things:
//
//   1. The full metric surface is auditable in one file. To know
//      everything the Indexer measures, read metrics.go.
//   2. Label cardinality is enforced here. Notably, contract_id is
//      NEVER a label — that would let the metric surface grow without
//      bound. Per-contract activity is observable via logs.
//   3. Tests of caller packages can pass a no-op recorder if needed,
//      though for the current implementation the package-level state
//      is shared (Prometheus collectors are process-global by design).
//
// Registration uses prometheus.DefaultRegisterer at init() time. The
// /metrics endpoint added in Phase 4 of the overhaul will simply call
// promhttp.Handler() to serve from the same registry.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const namespace = "indexer"

// All metrics are package-level so callers can use them via the typed
// recorder functions below. We use promauto.* so registration happens
// inline at init() time without manual MustRegister calls.

var (
	// Ledger lifecycle.
	ledgerProcessedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "ledger_processed_total",
			Help:      "Total ledgers the Indexer has finished processing, by terminal status.",
		},
		[]string{"network", "status"},
	)
	ledgerProcessingDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "ledger_processing_duration_seconds",
			Help:      "End-to-end time spent processing a single ledger (read + detect + publish + save state).",
			// Default buckets are tuned for sub-millisecond to seconds.
			// Soroban ledgers close every ~5s; a healthy Indexer
			// should be well under 1s.
			Buckets: prometheus.DefBuckets,
		},
		[]string{"network"},
	)
	currentLedgerSeq = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "current_ledger_seq",
			Help:      "The most recent ledger sequence the Indexer has finished publishing.",
		},
		[]string{"network"},
	)
	lagLedgers = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "lag_ledgers",
			Help:      "Difference between the network tip and the Indexer's current ledger. 0 means caught up.",
		},
		[]string{"network"},
	)

	// Detection.
	eventsDetectedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "events_detected_total",
			Help:      "Total Indexer-detected events of interest, broken down by event_kind.",
		},
		[]string{"network", "event_kind"},
	)

	// Publish (sink) lifecycle.
	publishTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "publish_total",
			Help:      "Total publish attempts to the configured sink, broken down by event_kind and terminal status.",
		},
		[]string{"network", "event_kind", "status"},
	)
	publishDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "publish_duration_seconds",
			Help:      "Time spent inside a single sink.Publish call, including any broker confirmation wait.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"event_kind"},
	)

	// Sink + watchlist + errors.
	sinkUp = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "sink_up",
			Help:      "1 if the configured sink reports healthy, 0 otherwise.",
		},
		[]string{"type"},
	)
	watchlistSize = promauto.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "watchlist_size",
			Help:      "Number of escrow contract addresses currently in the runtime watchlist.",
		},
		[]string{"network"},
	)
	errorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Total errors observed by the main loop, broken down by classification category.",
		},
		[]string{"category"},
	)
)

// Status enum values used as the "status" label of counters. Kept as
// constants so callers can't drift into typos that would create
// spurious time series.
const (
	StatusOK      = "ok"
	StatusError   = "error"
	StatusSkipped = "skipped"
)

// Error category values used as the "category" label of errors_total.
const (
	CategoryTransient    = "transient"
	CategorySkippable    = "skippable"
	CategoryFatal        = "fatal"
	CategoryUnclassified = "unclassified"
)

// RecordLedgerProcessed increments the per-status counter and records
// the duration of one ledger's processing. Called from the main loop
// once per ledger, after the publish + save-state cycle completes (or
// fails).
func RecordLedgerProcessed(network string, durationSec float64, status string) {
	ledgerProcessedTotal.WithLabelValues(network, status).Inc()
	ledgerProcessingDuration.WithLabelValues(network).Observe(durationSec)
}

// SetCurrentLedger updates the gauge for the most recently processed
// ledger. Called after Save returns nil so the gauge never gets ahead
// of what's actually durable.
func SetCurrentLedger(network string, ledgerSeq uint32) {
	currentLedgerSeq.WithLabelValues(network).Set(float64(ledgerSeq))
}

// SetLag updates the lag gauge. If the RPC tip is unknown (e.g. health
// check not yet run), pass tipLedger=0 to leave the gauge unchanged.
func SetLag(network string, tipLedger, currentLedger uint32) {
	if tipLedger == 0 || tipLedger < currentLedger {
		return
	}
	lagLedgers.WithLabelValues(network).Set(float64(tipLedger - currentLedger))
}

// RecordEventDetected increments the per-kind detection counter.
// Called from the detector once per event that matches the filter.
func RecordEventDetected(network, eventKind string) {
	eventsDetectedTotal.WithLabelValues(network, eventKind).Inc()
}

// RecordPublish increments the per-kind publish counter and records
// the latency of one Publish call. Called from the publisher whether
// the call succeeded or failed.
func RecordPublish(network, eventKind string, durationSec float64, status string) {
	publishTotal.WithLabelValues(network, eventKind, status).Inc()
	publishDuration.WithLabelValues(eventKind).Observe(durationSec)
}

// SetSinkUp updates the boolean gauge for sink reachability. up=true →
// gauge set to 1, false → 0. The argument intentionally takes a bool
// so callers don't accidentally drift the value to non-binary.
func SetSinkUp(sinkType string, up bool) {
	var v float64
	if up {
		v = 1
	}
	sinkUp.WithLabelValues(sinkType).Set(v)
}

// SetWatchlistSize updates the watchlist gauge. Called after the
// watchlist is mutated (typically after the detector's pass-1 finds
// new tw_init events).
func SetWatchlistSize(network string, size int) {
	watchlistSize.WithLabelValues(network).Set(float64(size))
}

// RecordError increments the per-category error counter. Use the
// Category* constants defined above.
func RecordError(category string) {
	errorsTotal.WithLabelValues(category).Inc()
}
