package detector

import (
	"encoding/base64"
	"testing"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/state"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// --- helpers to construct fake events without a real ledger -------

// makeContractEvent builds a Soroban ContractEvent with the given
// emitter contract address and topic Symbols. Pass data=nil for a
// void data field.
func makeContractEvent(emitterContractID string, topicSymbols []string) xdr.ContractEvent {
	contractHash, err := strkey.Decode(strkey.VersionByteContract, emitterContractID)
	if err != nil {
		panic(err)
	}
	var cid xdr.ContractId
	copy(cid[:], contractHash)

	topics := make(xdr.ScVec, 0, len(topicSymbols))
	for _, s := range topicSymbols {
		sym := xdr.ScSymbol(s)
		topics = append(topics, xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym})
	}

	return xdr.ContractEvent{
		Type:       xdr.ContractEventTypeContract,
		ContractId: &cid,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: topics,
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
}

// makeTransferEvent builds a Soroban transfer event (SAC/SEP-41 layout)
// emitted by tokenContractID with the given from and to addresses
// (both contract C... strkeys for simplicity).
func makeTransferEvent(tokenContractID, fromContractID, toContractID string) xdr.ContractEvent {
	ev := makeContractEvent(tokenContractID, []string{"transfer"})

	addrFromVal, err := contractAddressScVal(fromContractID)
	if err != nil {
		panic(err)
	}
	addrToVal, err := contractAddressScVal(toContractID)
	if err != nil {
		panic(err)
	}
	ev.Body.V0.Topics = append(ev.Body.V0.Topics, addrFromVal, addrToVal)
	return ev
}

func contractAddressScVal(contractID string) (xdr.ScVal, error) {
	hash, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return xdr.ScVal{}, err
	}
	var cid xdr.ContractId
	copy(cid[:], hash)
	addr := xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &cid,
	}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}, nil
}

// Three valid contract addresses built from deterministic 32-byte
// payloads. Real strkey encoding (with its CRC checksum) is required —
// strkey.Decode panics on invalid checksums, so hard-coded fake
// addresses do not work. These are computed once via mustContractAddress
// so we get well-formed C... strings without depending on any external
// fixture data.
var (
	contractA = mustContractAddress(1)
	contractB = mustContractAddress(2)
	contractC = mustContractAddress(3)
)

// mustContractAddress builds a deterministic, well-formed C...
// contract address from a small seed. The seed populates the first byte
// of the 32-byte payload; the rest is zeros. This is enough variation
// for tests that compare addresses for equality.
func mustContractAddress(seed byte) string {
	payload := make([]byte, 32)
	payload[0] = seed
	s, err := strkey.Encode(strkey.VersionByteContract, payload)
	if err != nil {
		panic(err)
	}
	return s
}

func newDetector(t *testing.T, watchlistSeed []string) *Detector {
	t.Helper()
	wl := state.NewWatchlist(watchlistSeed)
	return New("Test SDF Network ; September 2015", "testnet", events.DefaultTWTopicFilter(), wl)
}

// --- matchEvent ---------------------------------------------------

func TestMatchEvent_TWTopic_Matches(t *testing.T) {
	d := newDetector(t, nil)
	ev := makeContractEvent(contractA, []string{"tw_init"})
	escrowID, kind, ok := d.matchEvent(ev)
	if !ok {
		t.Fatal("expected match for tw_init event")
	}
	if escrowID != contractA {
		t.Errorf("escrowID: want %q, got %q", contractA, escrowID)
	}
	if kind != "tw_init" {
		t.Errorf("kind: want tw_init, got %q", kind)
	}
}

func TestMatchEvent_NonTWNonTransfer_Rejected(t *testing.T) {
	d := newDetector(t, nil)
	ev := makeContractEvent(contractA, []string{"unknown_topic"})
	if _, _, ok := d.matchEvent(ev); ok {
		t.Fatal("non-TW, non-transfer event must not match")
	}
}

func TestMatchEvent_TransferToWatchlistEscrow_Matches(t *testing.T) {
	d := newDetector(t, []string{contractB}) // B is the tracked escrow
	ev := makeTransferEvent(contractA, contractC, contractB) // token=A, from=C, to=B
	escrowID, kind, ok := d.matchEvent(ev)
	if !ok {
		t.Fatal("expected match: transfer to tracked escrow")
	}
	if escrowID != contractB {
		t.Errorf("escrowID: want %q (the recipient escrow), got %q", contractB, escrowID)
	}
	if kind != "token_transfer" {
		t.Errorf("kind: want token_transfer, got %q", kind)
	}
}

func TestMatchEvent_TransferToNonWatchlist_Rejected(t *testing.T) {
	d := newDetector(t, []string{contractB}) // B is tracked
	ev := makeTransferEvent(contractA, contractC, contractC) // to=C, not tracked
	if _, _, ok := d.matchEvent(ev); ok {
		t.Fatal("transfer to non-watchlist must not match")
	}
}

func TestMatchEvent_TransferFromWatchlist_IsNotEmitted(t *testing.T) {
	// Outgoing transfers from an escrow are intentionally out of scope —
	// they're covered by the escrow's own tw_release/tw_withdraw events.
	// This test pins the current behavior.
	d := newDetector(t, []string{contractB}) // B is tracked
	ev := makeTransferEvent(contractA, contractB, contractC) // from=B (tracked) to=C
	if _, _, ok := d.matchEvent(ev); ok {
		t.Fatal("outgoing transfer from a watchlist escrow is intentionally not emitted")
	}
}

func TestMatchEvent_AllTWTopics_Match(t *testing.T) {
	d := newDetector(t, nil)
	for _, topic := range events.AllTWTopics() {
		t.Run(string(topic), func(t *testing.T) {
			ev := makeContractEvent(contractA, []string{string(topic)})
			_, kind, ok := d.matchEvent(ev)
			if !ok {
				t.Fatalf("expected match for topic %q", topic)
			}
			if kind != string(topic) {
				t.Fatalf("kind must pass through: want %q, got %q", topic, kind)
			}
		})
	}
}

// --- extract helpers ----------------------------------------------

func TestContractIDFromEvent_HappyPath(t *testing.T) {
	ev := makeContractEvent(contractA, []string{"tw_init"})
	got, ok := contractIDFromEvent(ev)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != contractA {
		t.Fatalf("want %q, got %q", contractA, got)
	}
}

func TestContractIDFromEvent_NilContractID(t *testing.T) {
	ev := makeContractEvent(contractA, []string{"tw_init"})
	ev.ContractId = nil
	if _, ok := contractIDFromEvent(ev); ok {
		t.Fatal("expected ok=false when ContractId is nil")
	}
}

func TestTransferToAddress_HappyPath(t *testing.T) {
	ev := makeTransferEvent(contractA, contractB, contractC)
	got, ok := transferToAddress(ev)
	if !ok {
		t.Fatal("expected ok=true")
	}
	if got != contractC {
		t.Fatalf("want to=%q, got %q", contractC, got)
	}
}

func TestTransferToAddress_RejectsShortTopics(t *testing.T) {
	// A transfer event with only [Symbol("transfer")] and nothing else
	// is malformed for SAC purposes; transferToAddress must reject.
	ev := makeContractEvent(contractA, []string{"transfer"})
	if _, ok := transferToAddress(ev); ok {
		t.Fatal("expected ok=false for malformed transfer event")
	}
}

func TestEncodeEventXDR_RoundTrip(t *testing.T) {
	ev := makeContractEvent(contractA, []string{"tw_init"})
	encoded, err := encodeEventXDR(ev)
	if err != nil {
		t.Fatalf("encodeEventXDR: %v", err)
	}
	if encoded == "" {
		t.Fatal("expected non-empty base64")
	}

	// Decode back to verify the round-trip.
	raw, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		t.Fatalf("base64 decode: %v", err)
	}
	var decoded xdr.ContractEvent
	if err := decoded.UnmarshalBinary(raw); err != nil {
		t.Fatalf("xdr unmarshal: %v", err)
	}
	gotID, _ := contractIDFromEvent(decoded)
	if gotID != contractA {
		t.Fatalf("round-tripped event lost contract_id; got %q", gotID)
	}
}
