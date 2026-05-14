// Package noop provides a Sink that discards every envelope silently.
// It is the default sink in development (avoids requiring a broker) and
// in tests that only exercise the detection path.
package noop

import (
	"context"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/sirupsen/logrus"
)

// NoopSink discards envelopes silently. It always reports success and is
// always healthy. Useful for development and for tests where the focus is
// the detection path, not delivery.
type NoopSink struct{}

// Compile-time check that NoopSink satisfies sink.Sink.
var _ sink.Sink = (*NoopSink)(nil)

// New constructs a NoopSink. No configuration is needed.
func New() *NoopSink {
	return &NoopSink{}
}

// Publish accepts the envelope, validates it (so caller bugs surface even
// in dev), logs at debug level, and returns nil.
//
// Validation is included because the noop sink is often used in early
// pipeline development, and we want missing-field bugs to fail loudly in
// dev rather than only when someone wires the rabbitmq sink in staging.
func (n *NoopSink) Publish(_ context.Context, env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	logrus.WithFields(logrus.Fields{
		"ledger_seq":  env.LedgerSeq,
		"event_kind":  env.EventKind,
		"contract_id": env.ContractID,
		"message_id":  env.MessageID,
		"component":   "sink.noop",
	}).Debug("envelope discarded by noop sink")
	return nil
}

// Close is a no-op.
func (n *NoopSink) Close() error { return nil }

// Ping always reports the noop sink as healthy. Implementing
// sink.HealthChecker keeps /readyz semantics uniform across sink types.
func (n *NoopSink) Ping(_ context.Context) error { return nil }
