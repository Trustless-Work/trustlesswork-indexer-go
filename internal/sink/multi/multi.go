package multi

import (
	"context"
	"errors"
	"sync"

	"github.com/Trustless-Work/Indexer/internal/sink"
)

// MultiSink writes to multiple sinks in parallel. It implements sink.Sink so it is
// fully transparent to the pipeline — the caller does not know there are multiple destinations.
type MultiSink struct {
	sinks []entry
}

type entry struct {
	s        sink.Sink
	required bool // if false, errors are logged but do not fail the Write
}

var _ sink.Sink = (*MultiSink)(nil)

// New creates a MultiSink where all provided sinks are required.
// A write error from any of them propagates to the caller.
func New(sinks ...sink.Sink) *MultiSink {
	entries := make([]entry, len(sinks))
	for i, s := range sinks {
		entries[i] = entry{s: s, required: true}
	}
	return &MultiSink{sinks: entries}
}

// WithOptional adds a sink whose write failures are ignored.
// Useful for secondary destinations (e.g. metrics, audit logs) that must not
// block the primary pipeline.
func (m *MultiSink) WithOptional(s sink.Sink) *MultiSink {
	m.sinks = append(m.sinks, entry{s: s, required: false})
	return m
}

func (m *MultiSink) Write(ctx context.Context, buffer sink.LedgerBuffer, ledgerSeq uint32) error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, e := range m.sinks {
		wg.Add(1)
		go func(e entry) {
			defer wg.Done()
			if err := e.s.Write(ctx, buffer, ledgerSeq); err != nil && e.required {
				mu.Lock()
				errs = append(errs, err)
				mu.Unlock()
			}
		}(e)
	}

	wg.Wait()
	return errors.Join(errs...)
}

func (m *MultiSink) Close() error {
	var errs []error
	for _, e := range m.sinks {
		errs = append(errs, e.s.Close())
	}
	return errors.Join(errs...)
}