package noop

import (
	"context"

	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/sirupsen/logrus"
)

// NoopSink discards all data silently. Use it during development or when no
// output destination is needed.
type NoopSink struct{}

var _ sink.Sink = (*NoopSink)(nil)

func New() *NoopSink {
	return &NoopSink{}
}

func (n *NoopSink) Write(_ context.Context, _ sink.LedgerBuffer, ledgerSeq uint32) error {
	logrus.WithField("ledger", ledgerSeq).Debug("noop sink: discarding buffer")
	return nil
}

func (n *NoopSink) Close() error { return nil }