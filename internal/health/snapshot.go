// Package health hosts the Indexer's HTTP observability surface:
// liveness, readiness, Prometheus scrape, and a JSON status summary.
// It is intentionally decoupled from the rest of the pipeline via two
// small interfaces — Snapshotter and Pinger — so the health package
// does NOT import services, state, or sink. Anything that satisfies
// the interfaces can plug in.
//
// The endpoints exposed are:
//
//   GET /healthz   — liveness. Always 200 if the process responds.
//   GET /readyz    — readiness. 200 only if Pinger reports healthy,
//                    503 otherwise. Lag is NOT a readiness signal.
//   GET /metrics   — Prometheus scrape (DefaultRegisterer).
//   GET /status    — JSON summary for ops: cursor, watchlist size,
//                    sink type, uptime, version.
package health

import (
	"context"
	"time"
)

// LiveSnapshot is a point-in-time view of the loop's mutable state.
// It is built and returned by the ingestService and consumed by the
// /status handler. Fields are read-only by convention; callers MUST
// NOT mutate the returned struct.
//
// Zero values are valid (used at first boot before any ledger has
// been processed) — operators see LastLedgerSeq=0 and the zero time,
// which tells them the indexer is up but hasn't yet completed work.
type LiveSnapshot struct {
	LastLedgerSeq   uint32
	LastMessageID   string
	LastPublishedAt time.Time
	WatchlistSize   int
}

// Snapshotter is anything that can produce a LiveSnapshot. The
// ingestService satisfies this by maintaining an atomic.Pointer to
// the latest state.State and returning it at each call.
type Snapshotter interface {
	Snapshot() LiveSnapshot
}

// Pinger is anything that can check downstream reachability for the
// /readyz endpoint. The signature is identical to sink.HealthChecker's
// Ping method, so callers can pass `(hc.Ping)` as a bound method
// value without any adapter.
//
// A nil Pinger is acceptable; /readyz treats nil as "always ready".
// This is the right default for sinks that don't implement a probe
// (e.g. noop) — they cannot fail in a probeable way.
type Pinger func(ctx context.Context) error
