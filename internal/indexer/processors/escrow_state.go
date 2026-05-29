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
}

// EscrowStateDetector fetches the current DataKey::Escrow entry of each
// active escrow via the Soroban RPC (getLedgerEntries). This is the
// canonical way to read contract state in Soroban and sidesteps the
// fragility of parsing persistent-data writes out of transaction meta.
type EscrowStateDetector struct {
	rpc      *rpcclient.Client
	registry *registry.Registry
}

// NewEscrowStateDetector builds a state detector that queries the given
// RPC client. The registry is used to filter response entries (defence in
// depth) — only entries owned by known escrows are emitted.
func NewEscrowStateDetector(rpc *rpcclient.Client, reg *registry.Registry) *EscrowStateDetector {
	return &EscrowStateDetector{rpc: rpc, registry: reg}
}

// Name identifies the detector in logs/metrics.
func (d *EscrowStateDetector) Name() string { return "escrow_state" }

// FetchStates queries the RPC for each escrow's DataKey::Escrow entry and
// returns a state snapshot per entry that exists. Empty input is a no-op.
// Returned facts share the given (ledgerSeq, ledgerClosedAt) — the ledger
// we just processed and on whose activity we are reacting.
func (d *EscrowStateDetector) FetchStates(ctx context.Context, escrowIDs []string, ledgerSeq uint32, ledgerClosedAt time.Time) ([]EscrowStateChange, error) {
	if len(escrowIDs) == 0 {
		return nil, nil
	}

	// Build several candidate LedgerKeys per escrow so we cover the
	// likely storage shapes regardless of contract version: Vec[Symbol]
	// Persistent (canonical), bare Symbol Persistent (some sdk
	// serializations), Vec[Symbol] Temporary, and the instance entry
	// itself (always exists — useful as a sentinel for matching responses).
	keys := make([]string, 0, len(escrowIDs)*4)
	for _, id := range escrowIDs {
		candidates, err := escrowStateLedgerKeys(id)
		if err != nil {
			continue
		}
		for _, key := range candidates {
			b64, err := xdr.MarshalBase64(key)
			if err != nil {
				continue
			}
			keys = append(keys, b64)
		}
	}
	if len(keys) == 0 {
		return nil, nil
	}

	resp, err := d.rpc.GetLedgerEntries(ctx, protocol.GetLedgerEntriesRequest{Keys: keys})
	if err != nil {
		return nil, fmt.Errorf("rpc getLedgerEntries: %w", err)
	}

	return d.buildStateChanges(resp.Entries, ledgerSeq, ledgerClosedAt), nil
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
		raw    string
		isData bool
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
		picks[id] = pick{raw: e.DataXDR, isData: isData}
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
		out = append(out, EscrowStateChange{
			EscrowID:        id,
			StateChangeType: "updated",
			LedgerSeq:       ledgerSeq,
			LedgerClosedAt:  ledgerClosedAt,
			RawXDR:          p.raw,
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
