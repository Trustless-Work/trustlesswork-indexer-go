// Package errs provides category predicates over the sentinel errors
// defined throughout the Indexer. The main ingestion loop uses these to
// dispatch on error category — "should I retry, skip, or stop?" — without
// each call site needing to know every sentinel.
//
// Categories are mutually exclusive in spirit but enforced by convention,
// not by type. A given error is classified by at most one of:
//
//   - Transient: recoverable by retrying after a delay (network blip, RPC
//     rate-limit, ledger not yet closed, broker briefly unreachable).
//     Main loop applies backoff and retries.
//   - Skippable: affects a single unit of work (one event, one tx) and the
//     pipeline can advance past it. Behavior depends on STRICT_MODE: when
//     true (default in prod), Skippable escalates to fatal; when false,
//     the unit is logged at ERROR and the cursor advances.
//   - Fatal: permanent inconsistency requiring operator intervention
//     (corrupted state file, version mismatch, lock conflict, malformed
//     envelope built by us — i.e. a bug). The process must refuse to
//     continue; auto-recovery would risk silent data loss.
//
// An error that does not match any predicate is "unclassified". The main
// loop's default for unclassified errors is to FAIL — we'd rather discover
// new error modes loudly than silently progress past them.
//
// Each predicate walks the wrapped chain via errors.Is. Adding a new
// sentinel to its appropriate category is done HERE: keep this file the
// one place that decides how each sentinel is treated.
//
// IsTransient is defined in transient.go and will gain members once the
// RPC and sink packages add their own sentinels in Phase 2.
package errs

import (
	"errors"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/state"
)

// IsFatal reports whether err represents a permanent inconsistency that
// requires operator intervention. The main loop must NOT auto-recover from
// a fatal error; doing so would risk silent data loss.
//
// Fatal sentinels:
//   - state.ErrStateCorrupted: on-disk state is unparseable.
//   - state.ErrStateVersionMismatch: state was written by a newer binary.
//   - state.ErrStateNetworkMismatch: state belongs to a different network.
//   - state.ErrStateLockHeld: another Indexer instance is active.
//   - events.ErrEnvelopeInvalid: we built a malformed envelope (bug).
func IsFatal(err error) bool {
	switch {
	case errors.Is(err, state.ErrStateCorrupted),
		errors.Is(err, state.ErrStateVersionMismatch),
		errors.Is(err, state.ErrStateNetworkMismatch),
		errors.Is(err, state.ErrStateLockHeld),
		errors.Is(err, events.ErrEnvelopeInvalid):
		return true
	}
	return false
}

// IsSkippable reports whether err affects only a single unit of work and
// the rest of the pipeline can advance.
//
// Under STRICT_MODE=true (default in prod), the main loop should escalate
// Skippable errors to fatal so that no event is silently lost. Under
// STRICT_MODE=false (typically dev), the unit is logged at ERROR and the
// cursor advances.
//
// Skippable sentinels:
//   - events.ErrXDRDecodingFail: a single event's payload could not be
//     decoded. Rest of the ledger is fine.
func IsSkippable(err error) bool {
	return errors.Is(err, events.ErrXDRDecodingFail)
}
