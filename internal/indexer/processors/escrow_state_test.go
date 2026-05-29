package processors

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// fixturePath holds a real getLedgerEntries `xdr` payload for the instance
// entry of a testnet escrow compiled with .instance() storage
// (CDERRB6...). Captured 2026-05-28; the escrow carries Caleb's
// "Impacta-Bootcamp" engagement. It is the exact base64 the Indexer
// forwards in a state envelope's raw_xdr, so it doubles as a contract
// fixture for the NestJS consumer's decode path.
const fixturePath = "testdata/escrow_instance_state.b64"

// loadFixture returns the captured raw_xdr, the escrow's ScAddress, and
// its C... strkey id.
func loadFixture(t *testing.T) (raw string, contract xdr.ScAddress, cid string) {
	t.Helper()
	b, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	raw = strings.TrimSpace(string(b))

	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(raw, &data); err != nil {
		t.Fatalf("fixture is not valid LedgerEntryData: %v", err)
	}
	cd, ok := data.GetContractData()
	if !ok {
		t.Fatalf("fixture is not a ContractData entry")
	}
	id, err := cd.Contract.String()
	if err != nil {
		t.Fatalf("contract id: %v", err)
	}
	return raw, cd.Contract, id
}

func newDetector(t *testing.T, knownEscrows ...string) *EscrowStateDetector {
	t.Helper()
	reg, err := registry.New(nil)
	if err != nil {
		t.Fatalf("registry.New: %v", err)
	}
	reg.Seed(knownEscrows)
	// rpc is nil: buildStateChanges never touches it.
	return NewEscrowStateDetector(nil, reg)
}

// TestBuildStateChanges_InstanceStorage is the golden test. An escrow that
// uses .instance() storage returns ONLY its instance entry from
// getLedgerEntries; the detector must still emit it as the state carrier.
// This is the exact regression that left state_changes=0.
func TestBuildStateChanges_InstanceStorage(t *testing.T) {
	raw, _, cid := loadFixture(t)
	d := newDetector(t, cid)
	closedAt := time.Date(2026, 5, 28, 12, 0, 0, 0, time.UTC)

	out := d.buildStateChanges(
		[]protocol.LedgerEntryResult{{DataXDR: raw}}, 2800595, closedAt,
	)

	if len(out) != 1 {
		t.Fatalf("want 1 state change, got %d", len(out))
	}
	got := out[0]
	if got.EscrowID != cid {
		t.Errorf("EscrowID = %q, want %q", got.EscrowID, cid)
	}
	if got.RawXDR != raw {
		t.Errorf("RawXDR was not forwarded verbatim")
	}
	if got.StateChangeType != "updated" {
		t.Errorf("StateChangeType = %q, want %q", got.StateChangeType, "updated")
	}
	if got.LedgerSeq != 2800595 {
		t.Errorf("LedgerSeq = %d, want 2800595", got.LedgerSeq)
	}
	if !got.LedgerClosedAt.Equal(closedAt) {
		t.Errorf("LedgerClosedAt = %v, want %v", got.LedgerClosedAt, closedAt)
	}
}

// TestBuildStateChanges_UnknownContractFiltered guards defence-in-depth:
// entries for contracts not in the registry are dropped.
func TestBuildStateChanges_UnknownContractFiltered(t *testing.T) {
	raw, _, _ := loadFixture(t)
	d := newDetector(t) // empty registry

	out := d.buildStateChanges(
		[]protocol.LedgerEntryResult{{DataXDR: raw}}, 1, time.Now(),
	)
	if len(out) != 0 {
		t.Fatalf("want 0 state changes for unknown contract, got %d", len(out))
	}
}

// TestBuildStateChanges_PrefersDedicatedDataEntry verifies that when both a
// dedicated DataKey::Escrow entry (.persistent()/.temporary()) and the
// instance entry are returned for the same escrow, the dedicated one wins
// regardless of response order.
func TestBuildStateChanges_PrefersDedicatedDataEntry(t *testing.T) {
	instanceRaw, contract, cid := loadFixture(t)
	dedicatedRaw := dedicatedEntryB64(t, contract)
	d := newDetector(t, cid)

	orders := map[string][]protocol.LedgerEntryResult{
		"instance first":  {{DataXDR: instanceRaw}, {DataXDR: dedicatedRaw}},
		"dedicated first": {{DataXDR: dedicatedRaw}, {DataXDR: instanceRaw}},
	}
	for name, entries := range orders {
		t.Run(name, func(t *testing.T) {
			out := d.buildStateChanges(entries, 1, time.Now())
			if len(out) != 1 {
				t.Fatalf("want 1 state change, got %d", len(out))
			}
			if out[0].RawXDR != dedicatedRaw {
				t.Errorf("expected the dedicated data entry to be preferred over the instance entry")
			}
		})
	}
}

// TestEmittedPayloadDecodesToEscrowStruct proves the forwarded raw_xdr is
// not just well-formed but actually carries the escrow state where the
// consumer expects it: inside ScContractInstance.storage under
// Vec[Symbol("Escrow")]. This is the contract the NestJS consumer relies
// on; if a future change forwards the wrong entry, this breaks.
func TestEmittedPayloadDecodesToEscrowStruct(t *testing.T) {
	raw, _, cid := loadFixture(t)
	d := newDetector(t, cid)

	out := d.buildStateChanges(
		[]protocol.LedgerEntryResult{{DataXDR: raw}}, 1, time.Now(),
	)
	if len(out) != 1 {
		t.Fatalf("want 1 state change, got %d", len(out))
	}

	v, ok := escrowField(t, out[0].RawXDR, "engagement_id")
	if !ok {
		t.Fatal("engagement_id not found in storage[Vec[Symbol(\"Escrow\")]]")
	}
	if v.Type != xdr.ScValTypeScvString {
		t.Fatalf("engagement_id type = %v, want String", v.Type)
	}
	if got := string(v.MustStr()); got != "Impacta-Bootcamp" {
		t.Errorf("engagement_id = %q, want %q", got, "Impacta-Bootcamp")
	}
}

// escrowField navigates a forwarded raw_xdr exactly as the consumer must:
// LedgerEntryData -> ContractData -> instance storage -> the
// Vec[Symbol("Escrow")] entry -> the named field of the Escrow struct map.
func escrowField(t *testing.T, raw, field string) (xdr.ScVal, bool) {
	t.Helper()
	var data xdr.LedgerEntryData
	if err := xdr.SafeUnmarshalBase64(raw, &data); err != nil {
		t.Fatalf("decode raw_xdr: %v", err)
	}
	cd, ok := data.GetContractData()
	if !ok {
		t.Fatalf("raw_xdr is not ContractData")
	}
	inst, ok := cd.Val.GetInstance()
	if !ok {
		t.Fatalf("ContractData value is not a contract instance")
	}
	if inst.Storage == nil {
		t.Fatalf("instance storage is empty")
	}
	for _, me := range *inst.Storage {
		if me.Key.Type != xdr.ScValTypeScvVec {
			continue
		}
		vec := me.Key.MustVec()
		if vec == nil || len(*vec) != 1 {
			continue
		}
		k := (*vec)[0]
		if k.Type != xdr.ScValTypeScvSymbol || string(k.MustSym()) != "Escrow" {
			continue
		}
		escrow := me.Val.MustMap()
		for _, f := range *escrow {
			if f.Key.Type == xdr.ScValTypeScvSymbol && string(f.Key.MustSym()) == field {
				return f.Val, true
			}
		}
	}
	return xdr.ScVal{}, false
}

// dedicatedEntryB64 builds a synthetic .persistent() DataKey::Escrow ledger
// entry (key Vec[Symbol("Escrow")], empty map value) for the given
// contract, marshalled as the RPC returns it (LedgerEntryData base64).
func dedicatedEntryB64(t *testing.T, contract xdr.ScAddress) string {
	t.Helper()
	sym := xdr.ScSymbol("Escrow")
	vec := xdr.ScVec{{Type: xdr.ScValTypeScvSymbol, Sym: &sym}}
	vecPtr := &vec
	key := xdr.ScVal{Type: xdr.ScValTypeScvVec, Vec: &vecPtr}

	m := xdr.ScMap{}
	mPtr := &m
	val := xdr.ScVal{Type: xdr.ScValTypeScvMap, Map: &mPtr}

	data := xdr.LedgerEntryData{
		Type: xdr.LedgerEntryTypeContractData,
		ContractData: &xdr.ContractDataEntry{
			Contract:   contract,
			Key:        key,
			Durability: xdr.ContractDataDurabilityPersistent,
			Val:        val,
		},
	}
	b64, err := xdr.MarshalBase64(data)
	if err != nil {
		t.Fatalf("marshal synthetic dedicated entry: %v", err)
	}
	return b64
}
