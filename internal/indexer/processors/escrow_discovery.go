package processors

import (
	"fmt"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// EscrowDiscovery learns which contracts are TW escrows by scanning a
// ledger's entry changes for contract *instance* entries whose WASM code
// hash is approved, and registering them in the registry.
//
// This is identity by code, not by event topic: it is agnostic to the
// deploy path (a direct CreateContract or a factory deploy_v2 both create
// the instance entry) and to which events the contract emits. Shipping a
// new contract version only requires adding its hash to the approved set
// (see config.EscrowConfig); no change here.
//
// It must run before the consumption processors within a ledger, so that
// a deploy and the first event/deposit to that escrow in the same ledger
// are handled correctly.
type EscrowDiscovery struct {
	registry *registry.Registry
}

// NewEscrowDiscovery builds a discovery pass over the given registry.
func NewEscrowDiscovery(reg *registry.Registry) *EscrowDiscovery {
	return &EscrowDiscovery{registry: reg}
}

// Name identifies the processor in logs/metrics.
func (d *EscrowDiscovery) Name() string { return "escrow_discovery" }

// DiscoverFromTransaction inspects a transaction's ledger-entry changes
// and registers any contract instance created/updated/restored whose
// executable is an approved WASM hash. Returns the number of escrows
// newly added to the registry.
func (d *EscrowDiscovery) DiscoverFromTransaction(tx ingest.LedgerTransaction) (int, error) {
	changes, err := tx.GetChanges()
	if err != nil {
		return 0, fmt.Errorf("getting ledger changes: %w", err)
	}

	discovered := 0
	for _, ch := range changes {
		contractID, wasmHash, ok := instanceFromChange(ch)
		if !ok {
			continue
		}
		// Register is a no-op when the hash is not approved or the
		// contract is already known, so we can call it unconditionally.
		if d.registry.Register(contractID, wasmHash) {
			discovered++
		}
	}
	return discovered, nil
}

// instanceFromChange returns the contract id and WASM hash when the change
// is a present (created/updated/restored) contract-instance entry backed
// by a Wasm executable. Removals (Post == nil), non-contract-data entries,
// non-instance contract-data entries, and Stellar-Asset executables all
// return ok=false.
func instanceFromChange(ch ingest.Change) (contractID string, wasmHash xdr.Hash, ok bool) {
	var zero xdr.Hash
	if ch.Type != xdr.LedgerEntryTypeContractData || ch.Post == nil {
		return "", zero, false
	}

	cd, ok := ch.Post.Data.GetContractData()
	if !ok {
		return "", zero, false
	}
	// Only the contract instance entry carries the executable; ignore
	// ordinary contract storage entries.
	if cd.Key.Type != xdr.ScValTypeScvLedgerKeyContractInstance {
		return "", zero, false
	}

	inst, ok := cd.Val.GetInstance()
	if !ok {
		return "", zero, false
	}
	hash, ok := inst.Executable.GetWasmHash()
	if !ok {
		return "", zero, false // Stellar-Asset contract, not a Wasm contract
	}

	id, err := cd.Contract.String()
	if err != nil || id == "" {
		return "", zero, false
	}
	return id, hash, true
}
