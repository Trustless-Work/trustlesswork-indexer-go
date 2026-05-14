// Package services hosts the Indexer's runtime orchestration: the main
// ledger-by-ledger loop that fetches a ledger, runs the detector,
// hands matched events to the publisher, and saves state.
//
// This file is the heart of the new filter-and-forward pipeline (Phase
// 3 of the 2026-05-13 overhaul). Old buffer-based processing remains
// in internal/indexer/ as dead code reachable only from tests; the live
// path goes detector → publisher → sink, with state.Store tracking
// cursor + watchlist.
package services

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/Trustless-Work/Indexer/internal/detector"
	"github.com/Trustless-Work/Indexer/internal/errs"
	"github.com/Trustless-Work/Indexer/internal/metrics"
	"github.com/Trustless-Work/Indexer/internal/publisher"
	indexerrpc "github.com/Trustless-Work/Indexer/internal/rpc"
	"github.com/Trustless-Work/Indexer/internal/state"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	// maxLedgerFetchRetries caps how many transient failures we accept
	// when fetching a single ledger before propagating to the caller.
	maxLedgerFetchRetries = 10

	// initialRetryBackoff is the wait before the first retry. Doubles
	// each attempt up to maxRetryBackoff.
	initialRetryBackoff = time.Second

	// maxRetryBackoff caps the per-attempt wait. Prevents an unbounded
	// growth that would make shutdown sluggish.
	maxRetryBackoff = 30 * time.Second
)

// LedgerBackendFactory creates new LedgerBackend instances. Kept for
// future parallel-backfill support; not used by the current sequential
// loop.
type LedgerBackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

// IngestServiceConfig holds the dependencies the ingestService needs to
// run. All fields are required unless documented otherwise.
type IngestServiceConfig struct {
	// NetworkName is the short label for metrics ("testnet", etc.) and
	// the value that ends up in Envelope.Network.
	NetworkName string

	// NetworkPassphrase is the cryptographic identifier the SDK needs
	// to deserialize ledger meta.
	NetworkPassphrase string

	// LedgerBackend is the source of ledger meta.
	LedgerBackend ledgerbackend.LedgerBackend

	// LedgerBackendFactory is optional; for parallel-backfill paths
	// not yet wired in.
	LedgerBackendFactory LedgerBackendFactory

	// Detector is responsible for identifying events of interest in a
	// ledger.
	Detector *detector.Detector

	// Publisher emits envelopes to the configured sink.
	Publisher *publisher.Publisher

	// StateStore persists cursor + watchlist. Save is called after
	// every successful ledger.
	StateStore state.Store

	// Watchlist is the runtime set of escrow contracts. The detector
	// mutates it; we snapshot it into State on Save.
	Watchlist *state.Watchlist

	// StrictMode controls behavior on skippable errors: true → halt
	// the loop with the error; false → log at ERROR and advance.
	StrictMode bool
}

// IngestService is the orchestrator interface. Run blocks until the
// loop completes (bounded backfill), ctx is cancelled, or an
// unrecoverable error occurs.
type IngestService interface {
	Run(ctx context.Context, startLedger, endLedger uint32, initial state.State) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	networkName       string
	networkPassphrase string
	ledgerBackend     ledgerbackend.LedgerBackend
	detector          *detector.Detector
	publisher         *publisher.Publisher
	stateStore        state.Store
	watchlist         *state.Watchlist
	strictMode        bool
}

// NewIngestService validates cfg and constructs the orchestrator. It
// does NOT call any of its collaborators; the caller is responsible
// for opening connections etc. before passing them in.
func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	if cfg.LedgerBackend == nil {
		return nil, errors.New("ledger backend is required")
	}
	if cfg.Detector == nil {
		return nil, errors.New("detector is required")
	}
	if cfg.Publisher == nil {
		return nil, errors.New("publisher is required")
	}
	if cfg.StateStore == nil {
		return nil, errors.New("state store is required")
	}
	if cfg.Watchlist == nil {
		return nil, errors.New("watchlist is required")
	}
	if cfg.NetworkName == "" {
		return nil, errors.New("network name is required")
	}
	if cfg.NetworkPassphrase == "" {
		return nil, errors.New("network passphrase is required")
	}

	return &ingestService{
		networkName:       cfg.NetworkName,
		networkPassphrase: cfg.NetworkPassphrase,
		ledgerBackend:     cfg.LedgerBackend,
		detector:          cfg.Detector,
		publisher:         cfg.Publisher,
		stateStore:        cfg.StateStore,
		watchlist:         cfg.Watchlist,
		strictMode:        cfg.StrictMode,
	}, nil
}

// Run drives the ledger loop. Semantics:
//
//   - endLedger == 0: unbounded (live mode); loop runs until ctx is
//     cancelled or an unrecoverable error fires.
//   - endLedger != 0: bounded (backfill); loop exits after processing
//     endLedger inclusive.
//
// initial is the State as loaded at boot (cursor + watchlist already
// reflected in the *Watchlist passed via config). Run uses it as the
// starting point for the cursor and re-saves the updated value after
// each successful ledger.
func (s *ingestService) Run(ctx context.Context, startLedger, endLedger uint32, initial state.State) error {
	if err := s.prepareBackendRange(ctx, startLedger, endLedger); err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	current := initial
	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger %d (end=%d)", startLedger, endLedger)

	for endLedger == 0 || currentLedger <= endLedger {
		if err := ctx.Err(); err != nil {
			log.Ctx(ctx).Infof("Ingestion loop stopped at ledger %d: %v", currentLedger, err)
			return nil
		}

		meta, err := s.fetchLedgerWithRetry(ctx, currentLedger)
		if err != nil {
			return s.classifyAndReport(ctx, currentLedger, fmt.Errorf("fetching ledger %d: %w", currentLedger, err))
		}

		started := time.Now()
		newState, err := s.processLedger(ctx, meta, current)
		duration := time.Since(started).Seconds()

		if err != nil {
			metrics.RecordLedgerProcessed(s.networkName, duration, metrics.StatusError)
			if handled := s.classifyAndReport(ctx, currentLedger, err); handled != nil {
				return handled
			}
			// classifyAndReport returned nil → we are in non-strict
			// mode and the error was skippable; advance the cursor
			// with the same state we had so we don't replay forever.
			newState = current
		}

		if err := s.stateStore.Save(ctx, newState); err != nil {
			metrics.RecordLedgerProcessed(s.networkName, duration, metrics.StatusError)
			return fmt.Errorf("saving state for ledger %d: %w", currentLedger, err)
		}

		current = newState
		metrics.SetCurrentLedger(s.networkName, currentLedger)
		metrics.RecordLedgerProcessed(s.networkName, duration, metrics.StatusOK)
		log.Ctx(ctx).Infof("Processed ledger %d in %.3fs", currentLedger, duration)

		currentLedger++
	}

	log.Ctx(ctx).Infof("Backfill complete: processed ledgers %d to %d", startLedger, endLedger)
	return nil
}

// processLedger runs the per-ledger detect → publish → state-update
// pipeline. Returns the new State that should be persisted (cursor
// advanced + watchlist snapshot). Errors are returned as-is for the
// caller to classify.
func (s *ingestService) processLedger(ctx context.Context, meta xdr.LedgerCloseMeta, current state.State) (state.State, error) {
	ledgerSeq := meta.LedgerSequence()
	ledgerClosedAt := time.Unix(meta.LedgerCloseTime(), 0).UTC()

	detected, err := s.detector.Detect(ctx, meta)
	if err != nil {
		return current, fmt.Errorf("detecting in ledger %d: %w", ledgerSeq, err)
	}

	lastMessageID := current.LastMessageID
	for _, ev := range detected {
		if err := ctx.Err(); err != nil {
			return current, err
		}
		if err := s.publisher.Publish(ctx, ev); err != nil {
			return current, fmt.Errorf("publishing event tx=%s idx=%d: %w", ev.TxHash, ev.EventIndex, err)
		}
		// MessageID format matches publisher's NewMessageID.
		lastMessageID = fmt.Sprintf("%s-%d", ev.TxHash, ev.EventIndex)
	}

	// Update state: cursor advance + watchlist snapshot from the
	// runtime watchlist (which the detector may have mutated during
	// Pass 1).
	next := current.
		WithCursor(ledgerSeq, lastMessageID, ledgerClosedAt).
		WithWatchlist(s.watchlist.Snapshot())

	return next, nil
}

// classifyAndReport inspects err, records a metrics counter, and
// decides whether to halt the loop (return err) or continue (return
// nil after logging).
//
//   - context.Canceled / DeadlineExceeded: return as-is for clean exit.
//   - Fatal: return err.
//   - Transient: return err; the caller already exhausted retries.
//   - Skippable + StrictMode=true: return err.
//   - Skippable + StrictMode=false: log ERROR, record skipped metric,
//     return nil (caller advances cursor).
//   - Unclassified: return err. We prefer to fail loudly on novel
//     errors so they get triaged.
func (s *ingestService) classifyAndReport(ctx context.Context, ledger uint32, err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		log.Ctx(ctx).Infof("Loop stopped at ledger %d: %v", ledger, err)
		return err
	case errs.IsFatal(err):
		metrics.RecordError(metrics.CategoryFatal)
		log.Ctx(ctx).Errorf("Fatal error at ledger %d: %v", ledger, err)
		return err
	case errs.IsTransient(err):
		metrics.RecordError(metrics.CategoryTransient)
		log.Ctx(ctx).Errorf("Transient error exhausted at ledger %d: %v", ledger, err)
		return err
	case errs.IsSkippable(err):
		metrics.RecordError(metrics.CategorySkippable)
		if s.strictMode {
			log.Ctx(ctx).Errorf("Skippable error at ledger %d (strict mode → halting): %v", ledger, err)
			return err
		}
		log.Ctx(ctx).Errorf("Skippable error at ledger %d (advancing): %v", ledger, err)
		return nil
	default:
		metrics.RecordError(metrics.CategoryUnclassified)
		log.Ctx(ctx).Errorf("Unclassified error at ledger %d: %v", ledger, err)
		return err
	}
}

// fetchLedgerWithRetry wraps the backend call with a bounded
// exponential backoff. RPC errors are run through rpc.Classify so the
// retry decision sees stable sentinels.
func (s *ingestService) fetchLedgerWithRetry(ctx context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	backoff := initialRetryBackoff
	var lastErr error

	for attempt := 1; attempt <= maxLedgerFetchRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return xdr.LedgerCloseMeta{}, err
		}

		meta, err := s.ledgerBackend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return meta, nil
		}

		classified := indexerrpc.Classify(err)

		// Context errors flow through unchanged — Classify already
		// preserves them, but we double-check to keep the retry loop
		// honest.
		if errors.Is(classified, context.Canceled) || errors.Is(classified, context.DeadlineExceeded) {
			return xdr.LedgerCloseMeta{}, classified
		}
		// Fatal errors (e.g. ledger out of retention) must not be
		// retried — propagate immediately.
		if errs.IsFatal(classified) {
			return xdr.LedgerCloseMeta{}, classified
		}

		lastErr = classified
		log.Ctx(ctx).Warnf("Fetching ledger %d failed (attempt %d/%d): %v — retrying in %v",
			ledgerSeq, attempt, maxLedgerFetchRetries, classified, backoff)

		select {
		case <-ctx.Done():
			return xdr.LedgerCloseMeta{}, ctx.Err()
		case <-time.After(backoff):
		}

		if backoff < maxRetryBackoff {
			backoff *= 2
			if backoff > maxRetryBackoff {
				backoff = maxRetryBackoff
			}
		}
	}

	return xdr.LedgerCloseMeta{}, fmt.Errorf("giving up after %d attempts: %w", maxLedgerFetchRetries, lastErr)
}

// prepareBackendRange tells the LedgerBackend what range we plan to
// fetch, which lets buffered backends like the RPC reader pre-warm.
func (s *ingestService) prepareBackendRange(ctx context.Context, startLedger, endLedger uint32) error {
	var ledgerRange ledgerbackend.Range
	if endLedger == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
		log.Ctx(ctx).Infof("Backend prepared with unbounded range starting from ledger %d", startLedger)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(startLedger, endLedger)
		log.Ctx(ctx).Infof("Backend prepared with bounded range [%d, %d]", startLedger, endLedger)
	}

	if err := s.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing backend range from %d: %w", startLedger, err)
	}
	return nil
}
