package detector

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/metrics"
	"github.com/Trustless-Work/Indexer/internal/state"
	"github.com/Trustless-Work/Indexer/internal/utils"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// transferTopicSymbol is the first-topic Symbol of a SAC/SEP-41
// transfer event. We special-case this Symbol because matched-by-address
// filtering differs from the standard topic-match path.
const transferTopicSymbol = "transfer"

// Detector identifies events of interest in a Soroban ledger:
//
//  1. Events whose first topic Symbol is in the configured TW topic
//     filter (tw_init, tw_fund, ...). The emitter is the escrow itself.
//  2. SAC/SEP-41 transfer events whose `to` address is in the runtime
//     watchlist. The emitter is the SAC token contract; the watchlist
//     match identifies the recipient escrow.
//
// Detection proceeds in two sequential passes per ledger:
//
//   - Pass 1: walk every event, looking only at the first topic. If
//     it's `tw_init`, add the event's contract_id (== new escrow
//     address) to the watchlist. This pass is the cheapest possible
//     walk and is essential to handle the case where a ledger
//     contains both a tw_init AND a transfer-to-that-new-escrow in
//     different transactions.
//
//   - Pass 2: walk every event again, this time applying the full
//     filter. The watchlist is now complete for the ledger.
//
// Both passes share the same in-memory []ingest.LedgerTransaction; the
// underlying XDR is decoded exactly once.
type Detector struct {
	networkPassphrase string
	networkName       string // short label, for metrics
	filter            *events.TopicFilter
	watchlist         *state.Watchlist
}

// New constructs a Detector. The filter is typically
// events.DefaultTWTopicFilter(); the watchlist is loaded from State at
// boot and shared with the publisher / loop.
//
// networkName is the short label used in metrics ("testnet", "mainnet")
// and is independent of the networkPassphrase used by the SDK.
func New(networkPassphrase, networkName string, filter *events.TopicFilter, watchlist *state.Watchlist) *Detector {
	return &Detector{
		networkPassphrase: networkPassphrase,
		networkName:       networkName,
		filter:            filter,
		watchlist:         watchlist,
	}
}

// Detect walks a ledger's contract events and returns the subset the
// Indexer publishes, in the order encountered.
//
// Side effects:
//   - The watchlist is mutated: any tw_init events discovered are
//     added to it. The mutation is durable only after the caller saves
//     state; until then a crash would lose the additions, but the same
//     tw_init events would be re-discovered on the next run of the
//     same ledger (idempotent).
//
// On any error from the SDK reader the function returns what it
// detected so far AND the error; callers may choose to publish the
// partial result or discard it, but the conventional choice in strict
// mode is to discard and retry the ledger.
func (d *Detector) Detect(ctx context.Context, meta xdr.LedgerCloseMeta) ([]DetectedEvent, error) {
	ledgerSeq := meta.LedgerSequence()
	ledgerClosedAt := time.Unix(meta.LedgerCloseTime(), 0).UTC()

	txs, err := d.readTransactions(ctx, meta)
	if err != nil {
		return nil, fmt.Errorf("reading transactions for ledger %d: %w", ledgerSeq, err)
	}

	// Pass 1: discover new escrow contracts via tw_init events.
	d.discoverEscrows(ctx, txs)

	// Pass 2: apply the full filter and produce DetectedEvent records.
	out, err := d.collectMatches(ctx, txs, ledgerSeq, ledgerClosedAt)
	if err != nil {
		return out, err
	}

	// Update watchlist gauge once per ledger.
	metrics.SetWatchlistSize(d.networkName, d.watchlist.Size())

	return out, nil
}

// readTransactions slurps the ledger's transactions into memory. The
// reader is consumed once; both detection passes operate on the
// returned slice without re-decoding XDR.
func (d *Detector) readTransactions(ctx context.Context, meta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	reader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(d.networkPassphrase, meta)
	if err != nil {
		return nil, fmt.Errorf("creating tx reader: %w", err)
	}
	defer utils.DeferredClose(ctx, reader, "closing tx reader")

	out := make([]ingest.LedgerTransaction, 0, 64)
	for {
		tx, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return out, fmt.Errorf("reading tx: %w", err)
		}
		out = append(out, tx)
	}
	return out, nil
}

// discoverEscrows walks every event looking only at topic[0]. When it
// finds a tw_init, it adds the emitter (== the new escrow address) to
// the watchlist. This is the cheapest possible walk; it does not
// extract anything else.
func (d *Detector) discoverEscrows(ctx context.Context, txs []ingest.LedgerTransaction) {
	for _, tx := range txs {
		opCount := uint32(len(tx.Envelope.Operations()))
		for opIdx := uint32(0); opIdx < opCount; opIdx++ {
			evs, err := tx.GetContractEventsForOperation(opIdx)
			if err != nil {
				// Pass-1 errors are surfaced via the second pass;
				// here we simply skip. The pass-2 error path
				// reports the same issue with full context.
				continue
			}
			for _, ev := range evs {
				sym, ok := events.FirstTopicSymbol(ev)
				if !ok {
					continue
				}
				if sym != string(events.EventKindTWInit) {
					continue
				}
				contractID, ok := contractIDFromEvent(ev)
				if !ok {
					continue
				}
				d.watchlist.Add(contractID)
			}
		}
	}
	_ = ctx
}

// collectMatches walks every event a second time and emits
// DetectedEvent records for those that match the filter (by topic) or
// the watchlist-against-transfer (by destination address).
func (d *Detector) collectMatches(
	ctx context.Context,
	txs []ingest.LedgerTransaction,
	ledgerSeq uint32,
	ledgerClosedAt time.Time,
) ([]DetectedEvent, error) {
	_ = ctx

	out := make([]DetectedEvent, 0)
	for _, tx := range txs {
		txHash := tx.Result.TransactionHash.HexString()

		// EventIndex is a per-transaction running counter across all
		// operations' events, matching how Soroban consumers number
		// events within a transaction.
		var eventIdx uint32

		opCount := uint32(len(tx.Envelope.Operations()))
		for opIdx := uint32(0); opIdx < opCount; opIdx++ {
			evs, err := tx.GetContractEventsForOperation(opIdx)
			if err != nil {
				return out, fmt.Errorf("getting events for tx=%s op=%d: %w", txHash, opIdx, err)
			}
			for _, ev := range evs {
				detected, kind, ok := d.matchEvent(ev)
				if !ok {
					eventIdx++
					continue
				}

				rawXDR, err := encodeEventXDR(ev)
				if err != nil {
					// Skippable in strict mode (loop decides);
					// surface to caller wrapped with the
					// underlying sentinel.
					return out, fmt.Errorf("encoding event tx=%s idx=%d: %w", txHash, eventIdx, err)
				}

				out = append(out, DetectedEvent{
					EscrowID:       detected,
					TxHash:         txHash,
					EventIndex:     eventIdx,
					EventKind:      kind,
					RawXDR:         rawXDR,
					LedgerSeq:      ledgerSeq,
					LedgerClosedAt: ledgerClosedAt,
				})
				metrics.RecordEventDetected(d.networkName, kind)
				eventIdx++
			}
		}
	}
	return out, nil
}

// matchEvent classifies a single contract event against the two-stage
// filter and, if matched, returns (escrowID, eventKind, true).
//
// Order of checks:
//  1. First topic Symbol is in the TW filter set → emit as that kind,
//     escrowID = emitter contract_id.
//  2. First topic Symbol is "transfer" AND `to` is in the watchlist →
//     emit as token_transfer, escrowID = `to`. This requires reading
//     topics[1..2], which is part of the standard SAC layout.
//
// Events that match neither return (_, _, false). matchEvent never
// errors: malformed events are simply not interesting.
func (d *Detector) matchEvent(ev xdr.ContractEvent) (escrowID, kind string, matched bool) {
	if d.filter.Matches(ev) {
		// TW-emitted event. The escrow is the emitter.
		emitter, ok := contractIDFromEvent(ev)
		if !ok {
			return "", "", false
		}
		sym, ok := events.FirstTopicSymbol(ev)
		if !ok {
			return "", "", false
		}
		return emitter, sym, true
	}

	// Not a TW topic. Check if it's a SAC/SEP-41 transfer to/from a
	// known escrow.
	sym, ok := events.FirstTopicSymbol(ev)
	if !ok || sym != transferTopicSymbol {
		return "", "", false
	}
	to, ok := transferToAddress(ev)
	if ok && d.watchlist.IsTracked(to) {
		return to, string(events.EventKindTokenTransfer), true
	}
	// Outgoing-from-an-escrow transfers are intentionally not emitted
	// here. The escrow's own tw_release / tw_withdraw events cover
	// authorized outgoing flows. Direct outgoing without a tw_*
	// counterpart would be a contract-level bug and is deliberately
	// out of scope for the current detector.
	return "", "", false
}
