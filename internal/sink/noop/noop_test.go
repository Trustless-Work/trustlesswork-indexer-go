package noop

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Trustless-Work/Indexer/internal/events"
)

func validEnvelope() events.Envelope {
	return events.Envelope{
		SchemaVersion:  events.CurrentSchemaVersion,
		MessageID:      "deadbeef-0",
		Network:        "testnet",
		EventKind:      string(events.EventKindTWInit),
		ContractID:     "CAQA",
		TxHash:         "deadbeef",
		LedgerSeq:      100,
		EventIndex:     0,
		LedgerClosedAt: time.Unix(1700000000, 0),
		PublishedAt:    time.Unix(1700000005, 0),
		RawXDR:         "AAAA==",
	}
}

func TestNoop_Publish_Succeeds(t *testing.T) {
	s := New()
	if err := s.Publish(context.Background(), validEnvelope()); err != nil {
		t.Fatalf("expected nil error; got %v", err)
	}
}

func TestNoop_Publish_RejectsInvalidEnvelope(t *testing.T) {
	// Even the noop sink validates — caller bugs (forgot a required
	// field) must surface in dev where the noop sink is used most.
	s := New()
	env := validEnvelope()
	env.MessageID = ""
	err := s.Publish(context.Background(), env)
	if !errors.Is(err, events.ErrEnvelopeInvalid) {
		t.Fatalf("expected ErrEnvelopeInvalid; got %v", err)
	}
}

func TestNoop_Ping_AlwaysHealthy(t *testing.T) {
	s := New()
	if err := s.Ping(context.Background()); err != nil {
		t.Fatalf("Ping must always succeed; got %v", err)
	}
}

func TestNoop_Close_Idempotent(t *testing.T) {
	s := New()
	if err := s.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}
