package processors

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// escrowStateKey is the symbol identifying the escrow's main state entry
// (DataKey::Escrow). Stable across all four contract versions.
const escrowStateKey = "Escrow"

// MaxLedgerEntryKeys is the hard server-side cap of a single
// getLedgerEntries call (Soroban RPC rejects requests over 200 keys).
// Exported so callers (the reconciliation sweep) can budget "one request
// per ledger" in the same currency the detector spends.
const MaxLedgerEntryKeys = 200

// maxLedgerEntryKeysPerRequest bounds a single getLedgerEntries call. We
// build up to 4 candidate keys per escrow — so a ledger touching >50
// escrows would otherwise produce an over-limit request that fails
// DETERMINISTICALLY, and since a state-fetch error halts the loop and the
// same ledger is reprocessed, it crash-loops forever. Chunking keeps
// every request within the cap.
const maxLedgerEntryKeysPerRequest = MaxLedgerEntryKeys

// candidateKeysPerEscrow is how many LedgerKeys escrowStateLedgerKeys
// builds for an escrow whose storage shape we have not learned yet.
const candidateKeysPerEscrow = 4

// EscrowStateChange is the current state of one known escrow's
// DataKey::Escrow entry as fetched from the Soroban RPC right after a
// ledger that had activity for that escrow. RawXDR is the ContractData
// LedgerEntry; the consumer decodes it.
type EscrowStateChange struct {
	EscrowID        string
	StateChangeType string // "updated" today; "removed" when the entry no longer exists
	LedgerSeq       uint32
	LedgerClosedAt  time.Time
	RawXDR          string
	// LastModifiedLedger is when the entry itself last changed on-chain
	// (from the RPC response). Internal: the sweep uses it to skip
	// re-publishing states that have not moved since its previous pass.
	// Zero for "removed" (there is no entry to date).
	LastModifiedLedger uint32
}

// EscrowStateDetector fetches the current DataKey::Escrow entry of each
// active escrow via the Soroban RPC (getLedgerEntries). This is the
// canonical way to read contract state in Soroban and sidesteps the
// fragility of parsing persistent-data writes out of transaction meta.
type EscrowStateDetector struct {
	rpc      *rpcclient.Client
	registry *registry.Registry
	// learned remembers, per escrow, the exact LedgerKey (base64) that
	// carried its state on the last successful fetch — so later fetches
	// spend 1 key instead of candidateKeysPerEscrow, letting a sweep fit
	// 200 escrows per request instead of 50. In-memory only: a restart
	// relearns during the first pass, which is acceptable and keeps the
	// state file schema out of it. NOT goroutine-safe by design — the
	// detector lives on the single ingest goroutine (same invariant as
	// the registry writes).
	learned map[string]string
}

// NewEscrowStateDetector builds a state detector that queries the given
// RPC client. The registry is used to filter response entries (defence in
// depth) — only entries owned by known escrows are emitted.
func NewEscrowStateDetector(rpc *rpcclient.Client, reg *registry.Registry) *EscrowStateDetector {
	return &EscrowStateDetector{rpc: rpc, registry: reg, learned: make(map[string]string)}
}

// KeyCost is how many getLedgerEntries keys a fetch for id will spend —
// the currency the sweep budgets its batches in.
func (d *EscrowStateDetector) KeyCost(id string) int {
	if _, ok := d.learned[id]; ok {
		return 1
	}
	return candidateKeysPerEscrow
}

// Name identifies the detector in logs/metrics.
func (d *EscrowStateDetector) Name() string { return "escrow_state" }

// FetchStates queries the RPC for each escrow's DataKey::Escrow entry and
// returns a state snapshot per entry that exists (plus a "removed" change
// per escrow that no longer has one). Empty input is a no-op. Returned
// facts share the given (ledgerSeq, ledgerClosedAt) — the ledger we just
// processed and on whose activity we are reacting.
func (d *EscrowStateDetector) FetchStates(ctx context.Context, escrowIDs []string, ledgerSeq uint32, ledgerClosedAt time.Time) ([]EscrowStateChange, error) {
	return d.fetchStates(ctx, escrowIDs, ledgerSeq, ledgerClosedAt, 0)
}

// FetchStatesSince is FetchStates for the reconciliation sweep: "updated"
// changes are only returned when the entry itself changed at or after
// modifiedSince, so a steady-state sweep pass re-publishes nothing.
// "removed" changes always come through — absence has no modification
// ledger to filter on, and missing a removal is the failure mode the
// sweep exists to prevent.
func (d *EscrowStateDetector) FetchStatesSince(ctx context.Context, escrowIDs []string, ledgerSeq uint32, ledgerClosedAt time.Time, modifiedSince uint32) ([]EscrowStateChange, error) {
	return d.fetchStates(ctx, escrowIDs, ledgerSeq, ledgerClosedAt, modifiedSince)
}

func (d *EscrowStateDetector) fetchStates(ctx context.Context, escrowIDs []string, ledgerSeq uint32, ledgerClosedAt time.Time, modifiedSince uint32) ([]EscrowStateChange, error) {
	if len(escrowIDs) == 0 {
		return nil, nil
	}

	entries, requested, err := d.queryEntries(ctx, escrowIDs)
	if err != nil {
		return nil, err
	}
	changes := d.buildStateChanges(entries, ledgerSeq, ledgerClosedAt)

	// An escrow queried through its LEARNED single key that answered
	// nothing may have changed storage shape rather than disappeared.
	// Forget the shortcut and re-query those with the full candidate set
	// BEFORE judging anyone removed — a false "removed" is the one
	// mistake this detector must never make.
	if relearn := d.forgetLearnedAmong(missingFrom(requested, changes)); len(relearn) > 0 {
		extraEntries, _, err := d.queryEntries(ctx, relearn)
		if err != nil {
			return nil, err
		}
		changes = append(changes, d.buildStateChanges(extraEntries, ledgerSeq, ledgerClosedAt)...)
	}

	// Order matters: judge absence over the UNFILTERED set (an unchanged
	// escrow dropped by the filter must not look removed), then filter.
	all := appendRemoved(requested, changes, ledgerSeq, ledgerClosedAt)
	return filterUnchanged(all, modifiedSince), nil
}

// queryEntries turns escrow ids into getLedgerEntries calls — one learned
// key per known-shape escrow, candidateKeysPerEscrow otherwise — chunked
// within the RPC cap. requested reports the ids that actually went on the
// wire: only those can be judged "removed" (an id that failed
// key-building was never asked about).
func (d *EscrowStateDetector) queryEntries(ctx context.Context, escrowIDs []string) ([]protocol.LedgerEntryResult, []string, error) {
	keys := make([]string, 0, len(escrowIDs)*candidateKeysPerEscrow)
	requested := make([]string, 0, len(escrowIDs))
	for _, id := range escrowIDs {
		if learned, ok := d.learned[id]; ok {
			requested = append(requested, id)
			keys = append(keys, learned)
			continue
		}
		candidates, err := escrowStateLedgerKeys(id)
		if err != nil {
			continue
		}
		requested = append(requested, id)
		for _, key := range candidates {
			b64, err := xdr.MarshalBase64(key)
			if err != nil {
				continue
			}
			keys = append(keys, b64)
		}
	}
	if len(keys) == 0 {
		return nil, nil, nil
	}

	// Chunk into requests within the RPC key cap and merge the results.
	// Splitting an escrow's candidate keys across chunks is harmless:
	// buildStateChanges dedups per escrow over the full merged entry set.
	entries := make([]protocol.LedgerEntryResult, 0, len(keys))
	for _, chunk := range chunkStrings(keys, maxLedgerEntryKeysPerRequest) {
		resp, err := d.rpc.GetLedgerEntries(ctx, protocol.GetLedgerEntriesRequest{Keys: chunk})
		if err != nil {
			return nil, nil, fmt.Errorf("rpc getLedgerEntries (%d keys of %d): %w", len(chunk), len(keys), err)
		}
		entries = append(entries, resp.Entries...)
	}
	return entries, requested, nil
}

// missingFrom returns the requested ids that have no change in present.
func missingFrom(requested []string, present []EscrowStateChange) []string {
	seen := make(map[string]struct{}, len(present))
	for _, sc := range present {
		seen[sc.EscrowID] = struct{}{}
	}
	var missing []string
	for _, id := range requested {
		if _, ok := seen[id]; !ok {
			missing = append(missing, id)
		}
	}
	return missing
}

// forgetLearnedAmong drops the learned key of every listed escrow that
// had one, returning those ids (the ones worth a full re-query).
func (d *EscrowStateDetector) forgetLearnedAmong(ids []string) []string {
	var had []string
	for _, id := range ids {
		if _, ok := d.learned[id]; ok {
			delete(d.learned, id)
			had = append(had, id)
		}
	}
	return had
}

// filterUnchanged drops "updated" changes whose entry has not moved since
// modifiedSince. Zero means no filtering (the per-ledger activity path
// and a sweep's first pass, which IS the full reconciliation). "removed"
// always survives — see FetchStatesSince.
func filterUnchanged(changes []EscrowStateChange, modifiedSince uint32) []EscrowStateChange {
	if modifiedSince == 0 {
		return changes
	}
	out := changes[:0]
	for _, sc := range changes {
		if sc.StateChangeType == "removed" || sc.LastModifiedLedger >= modifiedSince {
			out = append(out, sc)
		}
	}
	return out
}

// appendRemoved adds a "removed" state change for every requested escrow
// that produced NO entry at all in the RPC response — not even its
// instance sentinel — meaning the contract's data is gone from the ledger
// (TTL expiry, or withdraw_remaining_funds which emits no Soroban event).
// Schema 1.1: a removed change carries an empty RawXDR; the signal IS the
// payload. Pure so it can be unit-tested without a live RPC.
func appendRemoved(requested []string, present []EscrowStateChange, ledgerSeq uint32, ledgerClosedAt time.Time) []EscrowStateChange {
	seen := make(map[string]struct{}, len(present))
	for _, sc := range present {
		seen[sc.EscrowID] = struct{}{}
	}
	out := present
	for _, id := range requested {
		if _, ok := seen[id]; ok {
			continue
		}
		out = append(out, EscrowStateChange{
			EscrowID:        id,
			StateChangeType: "removed",
			LedgerSeq:       ledgerSeq,
			LedgerClosedAt:  ledgerClosedAt,
		})
	}
	// Keep the stable by-EscrowID order buildStateChanges promises.
	sort.Slice(out, func(i, j int) bool { return out[i].EscrowID < out[j].EscrowID })
	return out
}

// chunkStrings splits s into consecutive slices of at most size elements.
// size <= 0 yields a single chunk. The concatenation of the result equals s.
func chunkStrings(s []string, size int) [][]string {
	if size <= 0 || len(s) <= size {
		return [][]string{s}
	}
	chunks := make([][]string, 0, (len(s)+size-1)/size)
	for start := 0; start < len(s); start += size {
		chunks = append(chunks, s[start:min(start+size, len(s))])
	}
	return chunks
}

// buildStateChanges turns a getLedgerEntries response into at most one
// state change per known escrow. It is the pure core of FetchStates,
// split out so it can be unit-tested with captured XDR fixtures and no
// live RPC.
func (d *EscrowStateDetector) buildStateChanges(entries []protocol.LedgerEntryResult, ledgerSeq uint32, ledgerClosedAt time.Time) []EscrowStateChange {
	// Dedup: one state per escrow. Prefer a dedicated data entry (keyed by
	// Vec[Symbol("Escrow")] or bare Symbol("Escrow")) over the instance
	// entry: with .persistent()/.temporary() storage the data entry carries
	// the escrow state directly. When no dedicated entry exists the contract
	// uses .instance() storage and the instance entry IS the state carrier
	// (DataKey::Escrow lives in its ScContractInstance.storage map).
	type pick struct {
		raw          string
		isData       bool
		keyXDR       string
		lastModified uint32
	}
	picks := map[string]pick{}
	for _, e := range entries {
		// IMPORTANT: getLedgerEntries returns the LedgerEntryData union
		// (the data body alone), not a full LedgerEntry with
		// LastModifiedLedgerSeq/Ext. Unmarshalling into LedgerEntry would
		// misread the type discriminant.
		var data xdr.LedgerEntryData
		if err := xdr.SafeUnmarshalBase64(e.DataXDR, &data); err != nil {
			continue
		}
		cd, ok := data.GetContractData()
		if !ok {
			continue
		}
		id, err := cd.Contract.String()
		if err != nil || !d.registry.IsEscrow(id) {
			continue
		}
		isData := cd.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance
		if existing, seen := picks[id]; seen && existing.isData {
			continue
		}
		picks[id] = pick{
			raw:          e.DataXDR,
			isData:       isData,
			keyXDR:       e.KeyXDR,
			lastModified: e.LastModifiedLedger,
		}
	}

	out := make([]EscrowStateChange, 0, len(picks))
	for id, p := range picks {
		// Two on-wire shapes, both forwarded as the raw ContractData entry
		// (thin indexer; the consumer navigates the value):
		//   - .persistent()/.temporary(): a dedicated ContractData entry
		//     keyed by Vec[Symbol("Escrow")] whose value IS the Escrow map.
		//   - .instance(): no dedicated entry exists; DataKey::Escrow lives
		//     inside the instance entry's ScContractInstance.storage map,
		//     keyed by Vec[Symbol("Escrow")]. We forward the instance entry
		//     and the consumer reaches into storage. The pick map already
		//     prefers a dedicated data entry when one is present.
		//
		// The pick's echoed request key IS this escrow's storage shape:
		// remember it so future fetches spend 1 key instead of 4. Guarded
		// on non-empty because the echo field is optional on the wire.
		if p.keyXDR != "" {
			d.learned[id] = p.keyXDR
		}
		out = append(out, EscrowStateChange{
			EscrowID:           id,
			StateChangeType:    "updated",
			LedgerSeq:          ledgerSeq,
			LedgerClosedAt:     ledgerClosedAt,
			RawXDR:             p.raw,
			LastModifiedLedger: p.lastModified,
		})
	}

	// Stable output order: by EscrowID.
	sort.Slice(out, func(i, j int) bool { return out[i].EscrowID < out[j].EscrowID })
	return out
}

// escrowStateLedgerKeys returns several candidate LedgerKeys for an
// escrow's state entry, so we cover the likely storage shapes regardless
// of which contract version the escrow runs:
//   - Vec[Symbol("Escrow")] Persistent — canonical soroban-sdk encoding
//     of a unit enum variant in .persistent() storage.
//   - Symbol("Escrow") Persistent — defensive fallback (bare symbol).
//   - Vec[Symbol("Escrow")] Temporary — in case the contract uses .temporary().
//   - LedgerKeyContractInstance Persistent — the instance entry itself,
//     useful as a sentinel and for contracts using .instance() storage.
func escrowStateLedgerKeys(contractID string) ([]xdr.LedgerKey, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return nil, fmt.Errorf("decoding contract id %s: %w", contractID, err)
	}
	if len(raw) != 32 {
		return nil, fmt.Errorf("contract id has unexpected length %d", len(raw))
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	addr := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &cid,
	}

	sym := xdr.ScSymbol(escrowStateKey)
	keyVec := xdr.ScVec{xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}}
	keyVecPtr := &keyVec // xdr.ScVal.Vec is **ScVec
	vecKey := xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &keyVecPtr}
	symKey := xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
	instKey := xdr.ScVal{Type: xdr.ScValTypeScvLedgerKeyContractInstance}

	mk := func(key xdr.ScVal, dur xdr.ContractDataDurability) xdr.LedgerKey {
		return xdr.LedgerKey{
			Type: xdr.LedgerEntryTypeContractData,
			ContractData: &xdr.LedgerKeyContractData{
				Contract:   addr,
				Key:        key,
				Durability: dur,
			},
		}
	}

	return []xdr.LedgerKey{
		mk(vecKey, xdr.ContractDataDurabilityPersistent),
		mk(symKey, xdr.ContractDataDurabilityPersistent),
		mk(vecKey, xdr.ContractDataDurabilityTemporary),
		mk(instKey, xdr.ContractDataDurabilityPersistent),
	}, nil
}
