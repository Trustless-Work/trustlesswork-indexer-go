// Package sink is the output abstraction for the Indexer pipeline. A Sink
// receives fully-formed Envelopes (one per detected event) and delivers
// them to a transport-specific destination (RabbitMQ today; Kafka or
// Postgres could be added without changing the contract).
//
// The interface is intentionally minimal: one Publish call carries one
// envelope, atomic with respect to delivery. This shape matches the
// "one message per detected entity" decision (see project_indexer_design_decisions.md)
// and makes idempotency the consumer's responsibility — every envelope
// carries a deterministic MessageID for that purpose.
//
// Implementations MUST:
//   - Be safe for sequential calls from the Indexer's publish goroutine.
//     They are NOT required to be safe for concurrent Publish from
//     multiple goroutines; the pipeline always serializes publishes per
//     sink instance.
//   - Return ErrSinkUnavailable (wrapped) when the transport is
//     unreachable.
//   - Return ErrSinkPublishRejected (wrapped) when the broker explicitly
//     rejects the publish (e.g. a Nack under publisher confirms).
//   - Return events.ErrEnvelopeInvalid (wrapped) when the envelope fails
//     validation. Sinks should call envelope.Validate() before any
//     transport-level work.
//   - Honour ctx for cancellation and timeouts.
package sink

import (
	"context"

	"github.com/Trustless-Work/Indexer/internal/events"
)

// Sink delivers one envelope at a time to a transport-specific destination.
// Concrete implementations live under internal/sink/{noop,multi,rabbitmq,...}.
type Sink interface {
	// Publish delivers a single envelope. Returns nil only when the
	// implementation can claim at-least-once delivery semantics for
	// that envelope (for RabbitMQ, that means a positive publisher
	// confirm from the broker). Otherwise returns a wrapped sentinel
	// from this package or events.ErrEnvelopeInvalid.
	//
	// The caller (typically the Publisher in the main loop) is
	// responsible for retry and cursor advancement; Publish itself
	// does no retry, so the caller can implement a coherent policy.
	Publish(ctx context.Context, env events.Envelope) error

	// Close releases held resources (network connections, channels,
	// goroutines). Publish must not be called after Close. Idempotent.
	Close() error
}

// HealthChecker is an optional interface. Sinks that can cheaply verify
// liveness (e.g. RabbitMQ connection still open) implement it so that
// the /readyz endpoint can include sink reachability in its decision.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Flusher is an optional interface. For sinks that internally buffer
// envelopes for batched delivery (none today; placeholder for future
// Kafka-with-batching). Calling Flush forces in-flight buffer to drain
// before returning.
type Flusher interface {
	Flush(ctx context.Context) error
}
