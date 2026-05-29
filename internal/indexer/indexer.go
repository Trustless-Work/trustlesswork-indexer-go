// Package indexer is the core of the Indexer: it turns a ledger's
// transactions into the escrow facts the pipeline forwards.
//
// Two sequential passes per ledger:
//
//   - Pass 1 (discovery): scan ledger-entry changes for contract
//     instances whose WASM hash is approved and register them, so the
//     detector knows which contracts are ours. Must run first — a factory
//     deploy and the first event/deposit to that escrow can share a
//     ledger.
//   - Pass 2 (detection): walk contract events and emit the subset
//     concerning known escrows (events + deposits), filtered by the
//     registry.
//
// The indexer does not decode contract-specific data; it identifies facts
// and forwards raw XDR. See docs/event-schema.md for the output contract.
package indexer

import (
	"context"
	"fmt"

	"github.com/Trustless-Work/Indexer/internal/indexer/processors"
	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	"github.com/stellar/go-stellar-sdk/ingest"
)

// Indexer detects escrow facts (events and deposits) in a ledger's
// transactions, using the registry to decide which contracts are ours.
type Indexer struct {
	discovery     *processors.EscrowDiscovery
	eventDetector *processors.EscrowEventDetector
}

// NewIndexer builds an Indexer over the given escrow registry.
func NewIndexer(reg *registry.Registry) *Indexer {
	return &Indexer{
		discovery:     processors.NewEscrowDiscovery(reg),
		eventDetector: processors.NewEscrowEventDetector(reg),
	}
}

// ProcessLedger runs discovery then detection over the ledger's
// transactions and returns the detected escrow events/deposits in the
// order encountered. Discovery (pass 1) completes for the whole ledger
// before detection (pass 2), so escrows deployed in the same ledger they
// first receive events are already known.
func (i *Indexer) ProcessLedger(ctx context.Context, transactions []ingest.LedgerTransaction) ([]processors.EscrowEvent, []string, error) {
	// Pass 1: discovery (registers approved-hash escrows).
	for _, tx := range transactions {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		if _, err := i.discovery.DiscoverFromTransaction(tx); err != nil {
			return nil, nil, fmt.Errorf("escrow discovery at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err)
		}
	}

	// Pass 2: detection (registry-filtered events/deposits). Also collect
	// the unique set of escrow IDs that had activity in this ledger — the
	// driver hands these to the state detector, which fetches their current
	// DataKey::Escrow entry via RPC.
	var events []processors.EscrowEvent
	seen := map[string]struct{}{}
	var activeEscrows []string
	for _, tx := range transactions {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		evs, err := i.eventDetector.DetectFromTransaction(tx)
		if err != nil {
			return nil, nil, fmt.Errorf("detecting escrow events at ledger=%d tx=%d: %w", tx.Ledger.LedgerSequence(), tx.Index, err)
		}
		events = append(events, evs...)
		for _, ev := range evs {
			if _, ok := seen[ev.EscrowID]; !ok {
				seen[ev.EscrowID] = struct{}{}
				activeEscrows = append(activeEscrows, ev.EscrowID)
			}
		}
	}

	return events, activeEscrows, nil
}
