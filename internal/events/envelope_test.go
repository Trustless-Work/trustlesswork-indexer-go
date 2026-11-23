package events

import (
	"errors"
	"testing"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer/processors"
)

func sampleEvent() processors.EscrowEvent {
	return processors.EscrowEvent{
		Type:           processors.EscrowEventTypeEvent,
		EscrowID:       "CESCROW",
		EventKind:      "tw_fund",
		EventIndex:     3,
		TxHash:         "abc",
		TxIndex:        7,
		LedgerSeq:      100,
		LedgerClosedAt: time.Unix(1700000000, 0).UTC(),
		RawXDR:         "AAAA",
	}
}

func TestFromEscrowEvent(t *testing.T) {
	env := FromEscrowEvent("testnet", sampleEvent())

	if env.SchemaVersion != CurrentSchemaVersion {
		t.Errorf("schema_version = %q, want %q", env.SchemaVersion, CurrentSchemaVersion)
	}
	if env.Type != "event" {
		t.Errorf("type = %q, want event", env.Type)
	}
	if env.ContractID != "CESCROW" {
		t.Errorf("contract_id = %q", env.ContractID)
	}
	if env.MessageID != "abc:3" {
		t.Errorf("message_id = %q, want abc:3", env.MessageID)
	}
	if env.PublishedAt.IsZero() {
		t.Error("published_at must be stamped")
	}
	if err := env.Validate(); err != nil {
		t.Errorf("a fully-mapped envelope should be valid, got %v", err)
	}
}

func TestRoutingKey(t *testing.T) {
	env := FromEscrowEvent("testnet", sampleEvent())
	if got, want := env.RoutingKey(), "stellar.testnet.escrow.event.tw_fund"; got != want {
		t.Errorf("RoutingKey() = %q, want %q", got, want)
	}

	dep := sampleEvent()
	dep.Type = processors.EscrowEventTypeDeposit
	dep.EventKind = "token_transfer"
	if got, want := FromEscrowEvent("mainnet", dep).RoutingKey(), "stellar.mainnet.escrow.deposit.token_transfer"; got != want {
		t.Errorf("RoutingKey() = %q, want %q", got, want)
	}
}

func TestValidate_missingFields(t *testing.T) {
	var env Envelope
	if err := env.Validate(); !errors.Is(err, ErrEnvelopeInvalid) {
		t.Errorf("empty envelope should fail with ErrEnvelopeInvalid, got %v", err)
	}
}

func TestFromGap_ValidatesAndRoutes(t *testing.T) {
	detected := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	env := FromGap("testnet", 100, 199, "rpc_retention", detected)

	if err := env.Validate(); err != nil {
		t.Fatalf("gap envelope should validate: %v", err)
	}
	if env.MessageID != "gap:testnet:100:199" {
		t.Errorf("message_id = %q, want deterministic gap key", env.MessageID)
	}
	if got, want := env.RoutingKey(), "stellar.testnet.escrow.control.gap_detected"; got != want {
		t.Errorf("routing key = %q, want %q", got, want)
	}
	// LedgerSeq anchors to the ledger AFTER the gap (where processing resumed).
	if env.LedgerSeq != 200 {
		t.Errorf("ledger_seq = %d, want 200", env.LedgerSeq)
	}
}

func TestValidate_removedStateNeedsNoXDR(t *testing.T) {
	env := Envelope{
		SchemaVersion:   CurrentSchemaVersion,
		Type:            "state",
		Network:         "testnet",
		ContractID:      "CESCROW",
		LedgerSeq:       10,
		StateChangeType: StateChangeRemoved,
		MessageID:       "CESCROW:10",
	}
	if err := env.Validate(); err != nil {
		t.Fatalf("removed state with empty raw_xdr should validate: %v", err)
	}

	// Every OTHER state change still requires the XDR payload.
	env.StateChangeType = "updated"
	if err := env.Validate(); err == nil {
		t.Fatal("updated state without raw_xdr must not validate")
	}
}

func TestValidate_controlRejectsInvertedRange(t *testing.T) {
	env := FromGap("testnet", 200, 100, "rpc_retention", time.Now())
	if err := env.Validate(); err == nil {
		t.Fatal("inverted gap range must not validate")
	}
}

func sampleWithdrawal() processors.EscrowEvent {
	ev := sampleEvent()
	ev.Type = processors.EscrowEventTypeWithdrawal
	ev.EventKind = "token_transfer"
	return ev
}

func TestSchemaVersion(t *testing.T) {
	if CurrentSchemaVersion != "1.3" {
		t.Errorf("CurrentSchemaVersion = %q, want 1.3", CurrentSchemaVersion)
	}
}

func TestFromEscrowEvent_Withdrawal(t *testing.T) {
	env := FromEscrowEvent("testnet", sampleWithdrawal())

	if env.Type != "withdrawal" {
		t.Errorf("type = %q, want withdrawal", env.Type)
	}
	if env.ContractID != "CESCROW" || env.EventKind != "token_transfer" ||
		env.TxHash != "abc" || env.TxIndex != 7 || env.EventIndex != 3 ||
		env.LedgerSeq != 100 || env.RawXDR != "AAAA" {
		t.Errorf("withdrawal envelope fields not mapped like a deposit: %+v", env)
	}
	if env.MessageID != "abc:3:withdrawal" {
		t.Errorf("message_id = %q, want abc:3:withdrawal", env.MessageID)
	}
	if got, want := env.RoutingKey(), "stellar.testnet.escrow.withdrawal.token_transfer"; got != want {
		t.Errorf("RoutingKey() = %q, want %q", got, want)
	}
	if err := env.Validate(); err != nil {
		t.Errorf("a fully-mapped withdrawal should be valid, got %v", err)
	}
}

func TestMessageIDs_DepositAndEventUnchanged(t *testing.T) {
	if got := FromEscrowEvent("testnet", sampleEvent()).MessageID; got != "abc:3" {
		t.Errorf("event message_id = %q, want abc:3", got)
	}
	dep := sampleEvent()
	dep.Type = processors.EscrowEventTypeDeposit
	dep.EventKind = "token_transfer"
	if got := FromEscrowEvent("testnet", dep).MessageID; got != "abc:3" {
		t.Errorf("deposit message_id = %q, want abc:3", got)
	}
	if got := NewWithdrawalMessageID("abc", 3); got != "abc:3:withdrawal" {
		t.Errorf("NewWithdrawalMessageID = %q, want abc:3:withdrawal", got)
	}
}

func TestValidate_WithdrawalRequirements(t *testing.T) {
	cases := map[string]func(*Envelope){
		"contract_id": func(e *Envelope) { e.ContractID = "" },
		"raw_xdr":     func(e *Envelope) { e.RawXDR = "" },
		"event_kind":  func(e *Envelope) { e.EventKind = "" },
		"tx_hash":     func(e *Envelope) { e.TxHash = "" },
	}
	for field, strip := range cases {
		env := FromEscrowEvent("testnet", sampleWithdrawal())
		strip(&env)
		if err := env.Validate(); !errors.Is(err, ErrEnvelopeInvalid) {
			t.Errorf("withdrawal without %s should fail with ErrEnvelopeInvalid, got %v", field, err)
		}
	}
}
