// Package multi fans an envelope out to several Sinks. It implements
// sink.Sink itself so the caller is unaware that there are multiple
// destinations.
//
// A typical use case is "publish to RabbitMQ for the Core, and to a
// noop sink for development comparison". Each downstream sink can be
// marked required (errors propagate) or optional (errors are logged
// only and do not fail the publish).
//
// Concurrency: MultiSink fans out concurrently to its children. Each
// child is responsible for its own internal synchronization (per the
// Sink contract, children are NOT required to be safe for concurrent
// Publish, so MultiSink calls each child's Publish from exactly one
// goroutine — a separate goroutine per child, but only one goroutine
// per child).
package multi

import (
	"context"
	"errors"
	"sync"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/sink"
	"github.com/sirupsen/logrus"
)

// MultiSink delivers each envelope to several downstream sinks in
// parallel. Required-sink errors propagate; optional-sink errors are
// logged.
type MultiSink struct {
	sinks []entry
}

// entry pairs a sink with a "required" bit. Required sinks must succeed
// for Publish to succeed; optional sinks are best-effort.
type entry struct {
	s        sink.Sink
	required bool
}

// Compile-time check that MultiSink satisfies sink.Sink.
var _ sink.Sink = (*MultiSink)(nil)

// New constructs a MultiSink where every provided sink is required. A
// Publish error from any of them is aggregated and returned.
func New(sinks ...sink.Sink) *MultiSink {
	entries := make([]entry, len(sinks))
	for i, s := range sinks {
		entries[i] = entry{s: s, required: true}
	}
	return &MultiSink{sinks: entries}
}

// WithOptional appends a sink whose Publish failures are logged but not
// returned. Useful for secondary destinations that must not block the
// primary pipeline (e.g. an audit log copy).
func (m *MultiSink) WithOptional(s sink.Sink) *MultiSink {
	m.sinks = append(m.sinks, entry{s: s, required: false})
	return m
}

// Publish fans the envelope out to every configured sink concurrently.
// Returns the joined errors of all required sinks that failed, or nil
// if every required sink succeeded. Optional-sink errors are logged at
// WARN and never propagate.
//
// Publish blocks until every child has returned (or ctx is cancelled
// inside a child). It does not enforce a global timeout; rely on ctx.
func (m *MultiSink) Publish(ctx context.Context, env events.Envelope) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, e := range m.sinks {
		wg.Add(1)
		go func(e entry) {
			defer wg.Done()
			if err := e.s.Publish(ctx, env); err != nil {
				if e.required {
					mu.Lock()
					errs = append(errs, err)
					mu.Unlock()
					return
				}
				logrus.WithError(err).WithFields(logrus.Fields{
					"message_id": env.MessageID,
					"component":  "sink.multi",
				}).Warn("optional sink publish failed; continuing")
			}
		}(e)
	}

	wg.Wait()
	return errors.Join(errs...)
}

// Close closes every child sink. Errors are aggregated; Close attempts
// every child even if some fail, so a single misbehaving child cannot
// leak resources from the others.
func (m *MultiSink) Close() error {
	errs := make([]error, 0, len(m.sinks))
	for _, e := range m.sinks {
		if err := e.s.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}
