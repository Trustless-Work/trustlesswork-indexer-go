package events

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func validEnvelope() Envelope {
	return Envelope{
		SchemaVersion:  CurrentSchemaVersion,
		MessageID:      "abc123-0",
		Network:        "testnet",
		EventKind:      string(EventKindTWInit),
		ContractID:     "CAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
		TxHash:         "deadbeef",
		LedgerSeq:      100,
		EventIndex:     0,
		LedgerClosedAt: time.Unix(1700000000, 0),
		PublishedAt:    time.Unix(1700000005, 0),
		RawXDR:         "AAAAA==",
	}
}

func TestEnvelope_Validate_OK(t *testing.T) {
	e := validEnvelope()
	if err := e.Validate(); err != nil {
		t.Fatalf("expected valid envelope to pass; got %v", err)
	}
}

func TestEnvelope_Validate_EventIndexZero_IsAllowed(t *testing.T) {
	// EventIndex == 0 is the first event in a tx — must be valid.
	e := validEnvelope()
	e.EventIndex = 0
	if err := e.Validate(); err != nil {
		t.Fatalf("event_index=0 must be valid (first event in tx); got %v", err)
	}
}

func TestEnvelope_Validate_RejectsMissingFields(t *testing.T) {
	cases := map[string]func(*Envelope){
		"schema_version": func(e *Envelope) { e.SchemaVersion = "" },
		"message_id":     func(e *Envelope) { e.MessageID = "" },
		"network":        func(e *Envelope) { e.Network = "" },
		"event_kind":     func(e *Envelope) { e.EventKind = "" },
		"contract_id":    func(e *Envelope) { e.ContractID = "" },
		"tx_hash":        func(e *Envelope) { e.TxHash = "" },
		"ledger_seq":     func(e *Envelope) { e.LedgerSeq = 0 },
		"ledger_closed":  func(e *Envelope) { e.LedgerClosedAt = time.Time{} },
		"published_at":   func(e *Envelope) { e.PublishedAt = time.Time{} },
		"raw_xdr":        func(e *Envelope) { e.RawXDR = "" },
	}

	for field, mutate := range cases {
		t.Run(field, func(t *testing.T) {
			e := validEnvelope()
			mutate(&e)
			err := e.Validate()
			if err == nil {
				t.Fatalf("expected validation to fail when %s is missing", field)
			}
			if !errors.Is(err, ErrEnvelopeInvalid) {
				t.Fatalf("expected error to wrap ErrEnvelopeInvalid; got %v", err)
			}
		})
	}
}

func TestEnvelope_Validate_ErrorMentionsField(t *testing.T) {
	// The validation message should name the offending field so operators
	// can debug quickly without reading source.
	e := validEnvelope()
	e.LedgerSeq = 0
	err := e.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "ledger_seq") {
		t.Fatalf("error should mention 'ledger_seq'; got %q", err.Error())
	}
}
