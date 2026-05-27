// Package noop provides a Sink that discards every envelope. It is the
// default in dev so the Indexer runs without a broker.
package noop

import (
	"context"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
)

// Sink discards everything it receives.
type Sink struct{}

var _ sink.Sink = (*Sink)(nil)

// New constructs a noop sink.
func New() *Sink { return &Sink{} }

// Publish discards env and reports success.
func (*Sink) Publish(_ context.Context, _ events.Envelope) error { return nil }

// Close is a no-op.
func (*Sink) Close() error { return nil }
