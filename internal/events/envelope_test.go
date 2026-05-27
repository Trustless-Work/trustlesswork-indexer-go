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
