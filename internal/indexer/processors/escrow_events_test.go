package processors

import (
	"testing"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// contractIDBytes builds a deterministic ContractId from a seed byte.
func contractIDBytes(seed byte) xdr.ContractId {
	var id xdr.ContractId
	for i := range id {
		id[i] = seed
	}
	return id
}

func contractStrkey(t *testing.T, id xdr.ContractId) string {
	t.Helper()
	s, err := strkey.Encode(strkey.VersionByteContract, id[:])
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func symVal(s string) xdr.ScVal {
	sym := xdr.ScSymbol(s)
	return xdr.ScVal{Type: xdr.ScValTypeScvSymbol, Sym: &sym}
}

func contractAddrVal(id xdr.ContractId) xdr.ScVal {
	addr := xdr.ScAddress{Type: xdr.ScAddressTypeScAddressTypeContract, ContractId: &id}
	return xdr.ScVal{Type: xdr.ScValTypeScvAddress, Address: &addr}
}

func eventWith(emitter xdr.ContractId, topics ...xdr.ScVal) xdr.ContractEvent {
	return xdr.ContractEvent{
		ContractId: &emitter,
		Type:       xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V:  0,
			V0: &xdr.ContractEventV0{Topics: topics},
		},
	}
}

func eventsRegistry(t *testing.T, escrows ...string) *registry.Registry {
	t.Helper()
	reg, err := registry.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	reg.Seed(escrows)
	return reg
}

func TestClassify_DepositAttributionMatchesTransferTo(t *testing.T) {
	// TW-03 origin-side invariant: the envelope's escrow id IS the raw
	// event's `to` (topics[2]) — the consumer re-checks the same thing
	// against the XDR and DLQs a mismatch.
	token := contractIDBytes(0x01)
	from := contractIDBytes(0x02)
	escrow := contractIDBytes(0x03)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(from), contractAddrVal(escrow))

	facts := d.classify(ev)
	if len(facts) != 1 {
		t.Fatalf("a transfer into a tracked escrow must classify as one deposit, got %+v", facts)
	}
	fact := facts[0]
	if fact.Type != EscrowEventTypeDeposit || fact.EscrowID != escrowID {
		t.Fatalf("fact = %+v, want deposit attributed to %s", fact, escrowID)
	}
	if to, _ := transferToAddress(ev); to != fact.EscrowID {
		t.Fatalf("attribution invariant broken: envelope says %s, raw event says %s", fact.EscrowID, to)
	}
}

func TestClassify_FabricatedTransferWithoutFromAddressIsDropped(t *testing.T) {
	// Any contract can emit an event whose first topic spells
	// "transfer". A real SEP-41/SAC transfer carries an Address in
	// topics[1]; one that does not is not a transfer and must not
	// become a deposit row downstream.
	scam := contractIDBytes(0x0A)
	escrow := contractIDBytes(0x03)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(scam, symVal("transfer"), symVal("junk"), contractAddrVal(escrow))

	if facts := d.classify(ev); len(facts) != 0 {
		t.Fatalf("transfer-shaped event without a from Address was forwarded: %+v", facts)
	}
}

func TestClassify_TransferToStrangerIsIgnored(t *testing.T) {
	token := contractIDBytes(0x01)
	from := contractIDBytes(0x02)
	stranger := contractIDBytes(0x0F)

	d := NewEscrowEventDetector(eventsRegistry(t, contractStrkey(t, contractIDBytes(0x03))))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(from), contractAddrVal(stranger))

	if facts := d.classify(ev); len(facts) != 0 {
		t.Fatalf("transfer to a stranger was forwarded: %+v", facts)
	}
}

func TestClassify_EscrowEmittedEventForwardsAnyKind(t *testing.T) {
	escrow := contractIDBytes(0x03)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(escrow, symVal("brand_new_kind"))

	facts := d.classify(ev)
	if len(facts) != 1 || facts[0].Type != EscrowEventTypeEvent || facts[0].EscrowID != escrowID || facts[0].EventKind != "brand_new_kind" {
		t.Fatalf("facts = %+v, want one escrow event with open-ended kind", facts)
	}
}

func TestClassify_TransferFromEscrowIsWithdrawal(t *testing.T) {
	token := contractIDBytes(0x01)
	escrow := contractIDBytes(0x03)
	recipient := contractIDBytes(0x04)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(escrow), contractAddrVal(recipient))

	facts := d.classify(ev)
	if len(facts) != 1 {
		t.Fatalf("a transfer out of a tracked escrow must classify as one withdrawal, got %+v", facts)
	}
	fact := facts[0]
	if fact.Type != EscrowEventTypeWithdrawal || fact.EscrowID != escrowID || fact.EventKind != "token_transfer" {
		t.Fatalf("fact = %+v, want withdrawal attributed to %s", fact, escrowID)
	}
	if from, _ := transferFromAddress(ev); from != fact.EscrowID {
		t.Fatalf("attribution invariant broken: envelope says %s, raw event says %s", fact.EscrowID, from)
	}
}

func TestClassify_EscrowToEscrowYieldsDepositThenWithdrawal(t *testing.T) {
	token := contractIDBytes(0x01)
	src := contractIDBytes(0x03)
	dst := contractIDBytes(0x05)
	srcID := contractStrkey(t, src)
	dstID := contractStrkey(t, dst)

	d := NewEscrowEventDetector(eventsRegistry(t, srcID, dstID))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(src), contractAddrVal(dst))

	facts := d.classify(ev)
	if len(facts) != 2 {
		t.Fatalf("escrow → escrow must yield two facts, got %+v", facts)
	}
	if facts[0].Type != EscrowEventTypeDeposit || facts[0].EscrowID != dstID {
		t.Errorf("first fact = %+v, want deposit into %s", facts[0], dstID)
	}
	if facts[1].Type != EscrowEventTypeWithdrawal || facts[1].EscrowID != srcID {
		t.Errorf("second fact = %+v, want withdrawal from %s", facts[1], srcID)
	}
}

func TestClassify_EscrowToItselfYieldsTwoFacts(t *testing.T) {
	token := contractIDBytes(0x01)
	escrow := contractIDBytes(0x03)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(escrow), contractAddrVal(escrow))

	facts := d.classify(ev)
	if len(facts) != 2 ||
		facts[0].Type != EscrowEventTypeDeposit || facts[0].EscrowID != escrowID ||
		facts[1].Type != EscrowEventTypeWithdrawal || facts[1].EscrowID != escrowID {
		t.Fatalf("facts = %+v, want deposit then withdrawal for %s", facts, escrowID)
	}
}

func TestClassify_EscrowEmitterYieldsOnlyEvent(t *testing.T) {
	// An escrow emitting a transfer-shaped event naming itself is still
	// exactly one `event` fact — rule 1 wins and returns.
	escrow := contractIDBytes(0x03)
	other := contractIDBytes(0x05)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID, contractStrkey(t, other)))
	ev := eventWith(escrow, symVal("transfer"), contractAddrVal(escrow), contractAddrVal(other))

	facts := d.classify(ev)
	if len(facts) != 1 || facts[0].Type != EscrowEventTypeEvent || facts[0].EscrowID != escrowID {
		t.Fatalf("facts = %+v, want one event fact", facts)
	}
}

func TestClassify_TransferWithoutToAddressIsDropped(t *testing.T) {
	token := contractIDBytes(0x01)
	escrow := contractIDBytes(0x03)
	escrowID := contractStrkey(t, escrow)

	d := NewEscrowEventDetector(eventsRegistry(t, escrowID))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(escrow), symVal("junk"))

	if facts := d.classify(ev); len(facts) != 0 {
		t.Fatalf("transfer without a to Address was forwarded: %+v", facts)
	}
}

func TestClassify_UnrelatedTransferIsIgnored(t *testing.T) {
	token := contractIDBytes(0x01)
	a := contractIDBytes(0x0E)
	b := contractIDBytes(0x0F)

	d := NewEscrowEventDetector(eventsRegistry(t, contractStrkey(t, contractIDBytes(0x03))))
	ev := eventWith(token, symVal("transfer"), contractAddrVal(a), contractAddrVal(b))

	if facts := d.classify(ev); len(facts) != 0 {
		t.Fatalf("unrelated transfer was forwarded: %+v", facts)
	}
}

// txWithEvents builds a one-operation transaction whose meta carries evs.
// Each event gets a Void data value so its XDR marshals.
func txWithEvents(evs ...xdr.ContractEvent) ingest.LedgerTransaction {
	for _, ev := range evs {
		ev.Body.V0.Data = xdr.ScVal{Type: xdr.ScValTypeScvVoid}
	}
	return ingest.LedgerTransaction{
		Index: 1,
		Envelope: xdr.TransactionEnvelope{
			Type: xdr.EnvelopeTypeEnvelopeTypeTx,
			V1: &xdr.TransactionV1Envelope{
				Tx: xdr.Transaction{Operations: []xdr.Operation{{}}},
			},
		},
		UnsafeMeta: xdr.TransactionMeta{
			V: 4,
			V4: &xdr.TransactionMetaV4{
				Operations: []xdr.OperationMetaV2{{Events: evs}},
			},
		},
		Ledger: xdr.LedgerCloseMeta{
			V: 1,
			V1: &xdr.LedgerCloseMetaV1{
				LedgerHeader: xdr.LedgerHeaderHistoryEntry{
					Header: xdr.LedgerHeader{
						LedgerSeq: 100,
						ScpValue:  xdr.StellarValue{CloseTime: 1700000000},
					},
				},
			},
		},
	}
}

func TestDetectFromTransaction_DoubleFactSharesEventIndex(t *testing.T) {
	token := contractIDBytes(0x01)
	src := contractIDBytes(0x03)
	dst := contractIDBytes(0x05)
	stranger := contractIDBytes(0x0F)
	srcID := contractStrkey(t, src)
	dstID := contractStrkey(t, dst)

	d := NewEscrowEventDetector(eventsRegistry(t, srcID, dstID))
	tx := txWithEvents(
		// idx 0: escrow → escrow (two facts)
		eventWith(token, symVal("transfer"), contractAddrVal(src), contractAddrVal(dst)),
		// idx 1: unrelated
		eventWith(token, symVal("transfer"), contractAddrVal(stranger), contractAddrVal(stranger)),
		// idx 2: plain deposit
		eventWith(token, symVal("transfer"), contractAddrVal(stranger), contractAddrVal(dst)),
	)

	facts, err := d.DetectFromTransaction(tx)
	if err != nil {
		t.Fatal(err)
	}
	if len(facts) != 3 {
		t.Fatalf("got %d facts, want 3: %+v", len(facts), facts)
	}
	if facts[0].Type != EscrowEventTypeDeposit || facts[1].Type != EscrowEventTypeWithdrawal {
		t.Fatalf("double fact order = %s, %s; want deposit, withdrawal", facts[0].Type, facts[1].Type)
	}
	if facts[0].EventIndex != 0 || facts[1].EventIndex != 0 {
		t.Errorf("double fact event indexes = %d, %d; want 0, 0", facts[0].EventIndex, facts[1].EventIndex)
	}
	if facts[0].RawXDR == "" || facts[0].RawXDR != facts[1].RawXDR {
		t.Error("both facts of one physical event must carry the same raw XDR")
	}
	// The counter advanced once per PHYSICAL event, not per fact.
	if facts[2].Type != EscrowEventTypeDeposit || facts[2].EventIndex != 2 {
		t.Errorf("later fact = %+v, want deposit at event index 2", facts[2])
	}
}
