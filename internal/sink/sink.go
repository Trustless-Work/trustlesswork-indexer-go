// Package sink is the Indexer's output abstraction. A Sink receives
// fully-formed envelopes (one per detected fact) and delivers them to a
// transport-specific destination (RabbitMQ today; Kafka, Postgres, etc.
// could be added without changing callers).
//
// Implementations MUST:
//   - be safe for sequential Publish calls from the ingest loop;
//   - return ErrSinkUnavailable (wrapped) when the transport is
//     unreachable;
//   - return ErrSinkPublishRejected (wrapped) when the broker rejects or
//     fails to confirm a publish;
//   - return events.ErrEnvelopeInvalid (wrapped) when the envelope fails
//     validation;
//   - honour ctx for cancellation/timeouts.
package sink

import (
	"context"
	"errors"

	"github.com/Trustless-Work/Indexer/internal/events"
)

// Sink delivers one envelope at a time to a transport destination.
type Sink interface {
	// Publish delivers a single envelope. Returns nil only when delivery
	// can be claimed at-least-once (for RabbitMQ, a positive publisher
	// confirm). The caller owns retry policy; Publish itself does none.
	Publish(ctx context.Context, env events.Envelope) error
	// Close releases held resources. Idempotent; Publish must not be
	// called after Close.
	Close() error
}

var (
	// ErrSinkUnavailable signals the transport is unreachable (dial /
	// channel / publish failure). Typically transient.
	ErrSinkUnavailable = errors.New("sink unavailable")
	// ErrSinkPublishRejected signals the broker explicitly rejected the
	// publish (a nack, or a confirm timeout).
	ErrSinkPublishRejected = errors.New("sink publish rejected")
)
