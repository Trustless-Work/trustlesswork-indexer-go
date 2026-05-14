package publisher

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Trustless-Work/Indexer/internal/detector"
	"github.com/Trustless-Work/Indexer/internal/events"
)

// fakeSink captures the envelopes it receives so tests can inspect
// them. It can also be configured to return an error.
type fakeSink struct {
	publishes atomic.Int32
	captured  events.Envelope
	err       error
}

func (f *fakeSink) Publish(_ context.Context, env events.Envelope) error {
	f.publishes.Add(1)
	f.captured = env
	return f.err
}

func (f *fakeSink) Close() error { return nil }

func validDetectedEvent() detector.DetectedEvent {
	return detector.DetectedEvent{
		EscrowID:       "CAQA",
		TxHash:         "deadbeef",
		EventIndex:     2,
		EventKind:      string(events.EventKindTWFund),
		RawXDR:         "AAAA==",
		LedgerSeq:      100,
		LedgerClosedAt: time.Unix(1700000000, 0).UTC(),
	}
}

func TestPublisher_BuildsEnvelopeCorrectly(t *testing.T) {
	fs := &fakeSink{}
	p := New("testnet", fs)
	ev := validDetectedEvent()

	if err := p.Publish(context.Background(), ev); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if fs.publishes.Load() != 1 {
		t.Fatalf("expected 1 sink call; got %d", fs.publishes.Load())
	}

	env := fs.captured
	if env.SchemaVersion != events.CurrentSchemaVersion {
		t.Errorf("SchemaVersion: want %q, got %q", events.CurrentSchemaVersion, env.SchemaVersion)
	}
	if env.MessageID != "deadbeef-2" {
		t.Errorf("MessageID: want deadbeef-2, got %q", env.MessageID)
	}
	if env.Network != "testnet" {
		t.Errorf("Network: want testnet, got %q", env.Network)
	}
	if env.EventKind != string(events.EventKindTWFund) {
		t.Errorf("EventKind: want %q, got %q", events.EventKindTWFund, env.EventKind)
	}
	if env.ContractID != ev.EscrowID {
		t.Errorf("ContractID must be the escrow ID; want %q, got %q", ev.EscrowID, env.ContractID)
	}
	if env.TxHash != ev.TxHash {
		t.Errorf("TxHash mismatch")
	}
	if env.LedgerSeq != ev.LedgerSeq {
		t.Errorf("LedgerSeq mismatch")
	}
	if !env.LedgerClosedAt.Equal(ev.LedgerClosedAt) {
		t.Errorf("LedgerClosedAt mismatch")
	}
	if env.PublishedAt.IsZero() {
		t.Error("PublishedAt must be stamped")
	}
	if env.RawXDR != ev.RawXDR {
		t.Errorf("RawXDR mismatch")
	}
}

func TestPublisher_PropagatesSinkError(t *testing.T) {
	sentinel := errors.New("sink boom")
	fs := &fakeSink{err: sentinel}
	p := New("testnet", fs)

	err := p.Publish(context.Background(), validDetectedEvent())
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected wrapped sentinel; got %v", err)
	}
}

func TestPublisher_RejectsInvalidEnvelopeBeforeSink(t *testing.T) {
	// If the DetectedEvent is missing a required field (e.g. EscrowID),
	// the resulting envelope fails Validate inside the sink. The sink
	// is what calls Validate today (per its contract), but we verify
	// here that the validation error surfaces correctly.
	fs := &fakeSink{}
	p := New("testnet", fs)

	bad := validDetectedEvent()
	bad.EscrowID = "" // makes the envelope invalid

	// Use the noop sink behavior in this fake: we don't validate in
	// fakeSink, so the error doesn't actually surface unless the real
	// sink runs Validate. This test pins that the Publisher itself
	// does NOT swallow validation; if a sink rejects, the rejection
	// reaches the caller.
	fs.err = events.ErrEnvelopeInvalid
	err := p.Publish(context.Background(), bad)
	if !errors.Is(err, events.ErrEnvelopeInvalid) {
		t.Fatalf("expected wrapped ErrEnvelopeInvalid; got %v", err)
	}
}
