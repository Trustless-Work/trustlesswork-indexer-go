package processors

import (
	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
)

// Sweeper walks the whole escrow registry in budgeted, rotating batches
// so the ingest loop can reconcile every tracked escrow against the
// chain WITHOUT depending on the escrow having activity — the only way
// to notice entries that disappeared silently (TTL expiry,
// withdraw_remaining_funds) or that changed during a gap.
//
// Budget discipline is the whole point (audit Sprint 5): one
// getLedgerEntries request worth of keys per processed ledger, only when
// the loop is at the tip. At 200 keys/request and 1 learned key per
// escrow, a full pass over 10k escrows takes ~50 ledgers (~5 minutes);
// with everything unlearned (first pass after boot), 4x that. The sweep
// must NEVER outcompete tip-following or turn an RPC 429 into a crash —
// the caller treats fetch errors as skip-and-continue.
//
// Not goroutine-safe: lives on the single ingest goroutine, like the
// registry writes and the detector's learned keys.
type Sweeper struct {
	reg *registry.Registry
	// pos is the rotating cursor into the registry's sorted snapshot.
	pos int
	// passStart is the ledger at which the current pass began;
	// prevPassStart anchors the "changed since" filter — everything
	// modified before the PREVIOUS pass began was already reported by it.
	// The one-pass overlap this allows is deliberate: a duplicate state
	// is absorbed downstream, a missed one is not recoverable.
	passStart     uint32
	prevPassStart uint32
}

// NewSweeper builds a sweeper over reg. The first pass reports every
// escrow (ModifiedSince 0): after a boot or a gap, full reconciliation
// is exactly what is wanted.
func NewSweeper(reg *registry.Registry) *Sweeper {
	return &Sweeper{reg: reg}
}

// NextBatch returns the next batch of escrow ids whose combined key cost
// (per keyCost) fits budget, and whether this batch COMPLETES a pass
// over the registry. Always returns at least one id when the registry is
// non-empty, even if its cost alone exceeds budget — otherwise one
// expensive escrow could stall the rotation forever.
//
// The registry may grow between calls; ids that land behind the cursor
// are picked up by the next pass. That laxity is fine for a
// reconciliation mechanism — the per-ledger activity path covers new
// escrows immediately.
func (s *Sweeper) NextBatch(currentLedger uint32, keyCost func(string) int, budget int) ([]string, bool) {
	ids := s.reg.Snapshot() // sorted, stable order
	if len(ids) == 0 {
		return nil, false
	}
	if s.pos >= len(ids) {
		s.pos = 0
	}
	if s.pos == 0 {
		s.prevPassStart = s.passStart
		s.passStart = currentLedger
	}

	spent := 0
	batch := make([]string, 0, budget)
	for s.pos < len(ids) {
		cost := keyCost(ids[s.pos])
		if len(batch) > 0 && spent+cost > budget {
			break
		}
		batch = append(batch, ids[s.pos])
		spent += cost
		s.pos++
	}

	completed := s.pos >= len(ids)
	if completed {
		s.pos = 0
	}
	return batch, completed
}

// ModifiedSince is the filter anchor for the current pass: only entries
// modified at/after it need re-publishing (removals always do).
func (s *Sweeper) ModifiedSince() uint32 {
	return s.prevPassStart
}
