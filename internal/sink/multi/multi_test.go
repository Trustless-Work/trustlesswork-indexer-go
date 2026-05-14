package multi

import (
	"context"
	"errors"
	"sync/atomic"
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

// fakeSink is a controllable sink.Sink for tests. It records every
// Publish call and can be configured to fail, succeed, or sleep.
type fakeSink struct {
	publishes atomic.Int32
	closes    atomic.Int32
	err       error
	sleep     time.Duration
}

func (f *fakeSink) Publish(ctx context.Context, _ events.Envelope) error {
	f.publishes.Add(1)
	if f.sleep > 0 {
		select {
		case <-time.After(f.sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func (f *fakeSink) Close() error {
	f.closes.Add(1)
	return nil
}

func TestMulti_Publish_FansOutToAll(t *testing.T) {
	a, b, c := &fakeSink{}, &fakeSink{}, &fakeSink{}
	m := New(a, b, c)

	if err := m.Publish(context.Background(), validEnvelope()); err != nil {
		t.Fatalf("expected nil error; got %v", err)
	}
	for i, fs := range []*fakeSink{a, b, c} {
		if got := fs.publishes.Load(); got != 1 {
			t.Errorf("sink %d: expected 1 publish; got %d", i, got)
		}
	}
}

func TestMulti_Publish_AggregatesRequiredErrors(t *testing.T) {
	errA := errors.New("a failed")
	errC := errors.New("c failed")
	a := &fakeSink{err: errA}
	b := &fakeSink{} // ok
	c := &fakeSink{err: errC}

	m := New(a, b, c)
	err := m.Publish(context.Background(), validEnvelope())
	if err == nil {
		t.Fatal("expected aggregated error")
	}
	if !errors.Is(err, errA) || !errors.Is(err, errC) {
		t.Fatalf("expected aggregated error to wrap both errA and errC; got %v", err)
	}
}

func TestMulti_Publish_OptionalSinkErrorsAreSilent(t *testing.T) {
	required := &fakeSink{}                              // ok
	optional := &fakeSink{err: errors.New("opt fails")} // failing, optional

	m := New(required).WithOptional(optional)
	if err := m.Publish(context.Background(), validEnvelope()); err != nil {
		t.Fatalf("optional failure must not propagate; got %v", err)
	}
	if optional.publishes.Load() != 1 {
		t.Fatal("optional sink must still be called")
	}
}

func TestMulti_Close_ClosesAll(t *testing.T) {
	a, b, c := &fakeSink{}, &fakeSink{}, &fakeSink{}
	m := New(a, b).WithOptional(c)
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, fs := range []*fakeSink{a, b, c} {
		if got := fs.closes.Load(); got != 1 {
			t.Errorf("sink %d: expected 1 Close call; got %d", i, got)
		}
	}
}

func TestMulti_RespectsContextCancellation(t *testing.T) {
	// A child sink that respects ctx must be unblocked when ctx is
	// cancelled. MultiSink propagates ctx as-is.
	slow := &fakeSink{sleep: 5 * time.Second}
	m := New(slow)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	start := time.Now()
	err := m.Publish(ctx, validEnvelope())
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("Publish should have returned quickly after ctx cancel; took %v", time.Since(start))
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled; got %v", err)
	}
}
