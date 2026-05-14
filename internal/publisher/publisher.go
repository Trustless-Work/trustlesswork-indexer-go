// Package publisher translates the detector's output (DetectedEvent)
// into the wire contract (events.Envelope) and hands it to the
// configured Sink. The publisher is a deliberately thin adapter: its
// job is to decouple "what the detector produces" from "what the wire
// requires", so each layer can evolve independently.
//
// The publisher does NOT do retries, batching, or backoff — those are
// the responsibility of the main loop above it. This keeps the
// publisher trivially testable (one DetectedEvent in, one Publish call
// out) and avoids spreading retry logic across multiple layers.
package publisher

import (
	"context"
	"fmt"
	"time"

	"github.com/Trustless-Work/Indexer/internal/detector"
	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/metrics"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// Publisher emits envelopes to a Sink. One Publisher per Indexer
// instance; it is stateless beyond holding references to its
// collaborators (the sink and the network label).
type Publisher struct {
	network string // value of envelope.Network and metrics label
	sink    sink.Sink
}

// New constructs a Publisher.
//
// network is the short network name (e.g. "testnet"). It populates
// Envelope.Network and the "network" label of the publish metrics; it
// must match the network the detector was configured with.
func New(network string, snk sink.Sink) *Publisher {
	return &Publisher{network: network, sink: snk}
}

// Publish converts ev to an envelope, validates it, and hands it to
// the sink. PublishedAt is stamped here; LedgerClosedAt comes from the
// chain (set by the detector).
//
// Errors from the sink propagate unwrapped (they already carry the
// sink's sentinel via errors.Is). Errors from envelope.Validate
// (caller bug — missing field) propagate wrapping
// events.ErrEnvelopeInvalid and should be treated as fatal by the loop.
//
// The duration of the underlying Publish call is recorded in the
// indexer_publish_duration_seconds histogram regardless of success.
// The status label distinguishes the two outcomes.
func (p *Publisher) Publish(ctx context.Context, ev detector.DetectedEvent) error {
	env := events.Envelope{
		SchemaVersion:  events.CurrentSchemaVersion,
		MessageID:      events.NewMessageID(ev.TxHash, ev.EventIndex),
		Network:        p.network,
		EventKind:      ev.EventKind,
		ContractID:     ev.EscrowID,
		TxHash:         ev.TxHash,
		LedgerSeq:      ev.LedgerSeq,
		EventIndex:     ev.EventIndex,
		LedgerClosedAt: ev.LedgerClosedAt,
		PublishedAt:    time.Now().UTC(),
		RawXDR:         ev.RawXDR,
	}

	start := time.Now()
	err := p.sink.Publish(ctx, env)
	duration := time.Since(start).Seconds()

	if err != nil {
		metrics.RecordPublish(p.network, ev.EventKind, duration, metrics.StatusError)
		return fmt.Errorf("publishing event_kind=%s message_id=%s: %w", env.EventKind, env.MessageID, err)
	}
	metrics.RecordPublish(p.network, ev.EventKind, duration, metrics.StatusOK)
	return nil
}
