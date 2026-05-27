// Package ingest is the composition root and live loop for the Indexer.
//
// State after the 2026-05-21 cleanup: the filter-and-forward pipeline
// (detector / events / publisher / envelope-sink / state) was removed,
// and the processor-based core in internal/indexer is once again the
// single source of truth. This loop fetches ledgers from the configured
// RPC backend, runs each one through the processor pipeline, and logs a
// per-ledger summary of the populated buffer.
//
// Delivery to a sink is intentionally NOT wired yet. This is a clean
// starting point to refine the processor core from: the buffer is built
// every ledger and summarized; routing it to RabbitMQ is the next step.
//
// The caller (cmd/ingest.go) owns ctx, signal handling and config
// loading. Ingest itself never reads env vars.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/indexer"
	"github.com/Trustless-Work/Indexer/internal/indexer/processors"
	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	sinkfactory "github.com/Trustless-Work/Indexer/internal/sink/factory"
	"github.com/Trustless-Work/Indexer/internal/state"
	"github.com/Trustless-Work/Indexer/internal/utils"
	"github.com/stellar/go-stellar-sdk/clients/rpcclient"
	sdkingest "github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	// maxLedgerFetchRetries caps how many transient failures we accept
	// when fetching a single ledger before giving up.
	maxLedgerFetchRetries = 10
	// initialRetryBackoff is the wait before the first retry. It doubles
	// on every failure up to maxRetryBackoff.
	initialRetryBackoff = time.Second
	// maxRetryBackoff caps the per-attempt wait so shutdown stays snappy.
	maxRetryBackoff = 30 * time.Second
)

// Ingest is the entry point of the Indexer pipeline. It blocks until ctx
// cancellation or a terminal error.
//
// Semantics:
//   - INDEXER_END_LEDGER == 0: unbounded (live mode); the loop runs until
//     ctx is cancelled or a terminal error fires.
//   - INDEXER_END_LEDGER != 0: bounded (backfill); the loop exits after
//     processing end inclusive.
func Ingest(ctx context.Context, cfg *config.Config) error {
	log.Ctx(ctx).Info(cfg.String())

	// State store: persists the cursor (and, later, the escrow set). The
	// flock makes a second instance fail fast instead of double-publishing.
	store, err := state.NewFileStore(cfg.State.Path)
	if err != nil {
		return fmt.Errorf("opening state store: %w", err)
	}
	defer func() {
		if cerr := store.Close(); cerr != nil {
			log.Ctx(ctx).Warnf("closing state store: %v", cerr)
		}
	}()

	backend, err := NewLedgerBackend(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}
	defer func() {
		if cerr := backend.Close(); cerr != nil {
			log.Ctx(ctx).Warnf("closing ledger backend: %v", cerr)
		}
	}()

	startLedger := cfg.Indexer.StartLedger
	endLedger := cfg.Indexer.EndLedger

	// Resume from the persisted cursor if we have one. A network mismatch
	// is fatal — the operator must point at the right state file.
	switch loaded, lerr := store.Load(ctx); {
	case errors.Is(lerr, state.ErrStateNotFound):
		log.Ctx(ctx).Info("No state file — starting fresh")
	case lerr != nil:
		return fmt.Errorf("loading state: %w", lerr)
	default:
		if loaded.Network != "" && loaded.Network != cfg.Network.Passphrase {
			return fmt.Errorf("state network mismatch: state=%q, configured=%q", loaded.Network, cfg.Network.Passphrase)
		}
		startLedger = loaded.LastLedgerSeq + 1
		log.Ctx(ctx).Infof("Resuming from persisted cursor: next ledger %d", startLedger)
	}

	// START_LEDGER unset (0) means "start from the network tip". Resolve it
	// via a direct RPC call: the RPC rejects a PrepareRange starting at 0,
	// and the backend's own GetLatestLedgerSequence requires PrepareRange
	// first (chicken-and-egg), so we ask the RPC straight up.
	if startLedger == 0 {
		rpc := rpcclient.NewClient(cfg.RPC.URL, &http.Client{Timeout: cfg.RPC.RequestTimeout})
		latest, err := rpc.GetLatestLedger(ctx)
		_ = rpc.Close()
		if err != nil {
			return fmt.Errorf("resolving latest ledger from RPC: %w", err)
		}
		log.Ctx(ctx).Infof("START_LEDGER unset; starting from network tip %d", latest.Sequence)
		startLedger = latest.Sequence
	}

	if err := prepareBackendRange(ctx, backend, startLedger, endLedger); err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	// Escrow registry: identifies "our" contracts by approved WASM hash.
	// Populated by the discovery pass (and, later, an API seed).
	reg, err := registry.New(cfg.Escrow.ApprovedWasmHashes)
	if err != nil {
		return fmt.Errorf("building escrow registry: %w", err)
	}

	// Sink: where detected facts are delivered (noop or rabbitmq).
	outSink, err := sinkfactory.New(cfg)
	if err != nil {
		return fmt.Errorf("building sink: %w", err)
	}
	defer func() {
		if cerr := outSink.Close(); cerr != nil {
			log.Ctx(ctx).Warnf("closing sink: %v", cerr)
		}
	}()

	ledgerIndexer := indexer.NewIndexer(reg)

	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger %d (end=%d)", startLedger, endLedger)

	for endLedger == 0 || currentLedger <= endLedger {
		if err := ctx.Err(); err != nil {
			log.Ctx(ctx).Infof("Ingestion loop stopped at ledger %d: %v", currentLedger, err)
			return nil
		}

		meta, err := fetchLedgerWithRetry(ctx, backend, currentLedger)
		if err != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, err)
		}

		started := time.Now()
		facts, err := processLedger(ctx, ledgerIndexer, cfg.Network.Passphrase, cfg.Indexer.GetLedgersLimit, meta)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}

		// Publish each detected fact. On failure we return (strict): a
		// dropped publish would be silent data loss. Cursor persistence
		// (resume without gaps) is a later step.
		for _, ev := range facts {
			if err := outSink.Publish(ctx, events.FromEscrowEvent(cfg.Network.Name, ev)); err != nil {
				return fmt.Errorf("publishing %s:%d at ledger %d: %w", ev.TxHash, ev.EventIndex, currentLedger, err)
			}
		}

		// Persist the cursor only after every fact in this ledger was
		// published. On a crash between publish and save we reprocess the
		// ledger and the consumer dedupes by message_id (at-least-once).
		if err := store.Save(ctx, state.State{
			Network:       cfg.Network.Passphrase,
			LastLedgerSeq: currentLedger,
		}); err != nil {
			return fmt.Errorf("saving state at ledger %d: %w", currentLedger, err)
		}

		log.Ctx(ctx).Infof("Processed ledger %d in %v — known_escrows=%d escrow_events=%d",
			currentLedger, time.Since(started), reg.Size(), len(facts))

		currentLedger++
	}

	log.Ctx(ctx).Infof("Backfill complete: processed ledgers %d to %d", startLedger, endLedger)
	return nil
}

// processLedger reads a ledger's transactions and runs them through the
// indexer, returning the detected escrow events. Delivery is the caller's
// concern (none today).
func processLedger(
	ctx context.Context,
	ledgerIndexer *indexer.Indexer,
	networkPassphrase string,
	limitHint int,
	meta xdr.LedgerCloseMeta,
) ([]processors.EscrowEvent, error) {
	transactions, err := readLedgerTransactions(ctx, networkPassphrase, limitHint, meta)
	if err != nil {
		return nil, fmt.Errorf("reading transactions: %w", err)
	}

	facts, err := ledgerIndexer.ProcessLedger(ctx, transactions)
	if err != nil {
		return nil, err
	}
	return facts, nil
}

// readLedgerTransactions slurps a ledger's transactions into memory using
// the SDK reader. The limit hint pre-sizes the slice to avoid repeated
// growth on busy ledgers.
func readLedgerTransactions(
	ctx context.Context,
	networkPassphrase string,
	limitHint int,
	meta xdr.LedgerCloseMeta,
) ([]sdkingest.LedgerTransaction, error) {
	reader, err := sdkingest.NewLedgerTransactionReaderFromLedgerCloseMeta(networkPassphrase, meta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer utils.DeferredClose(ctx, reader, "closing ledger transaction reader")

	if limitHint <= 0 {
		limitHint = 64
	}
	transactions := make([]sdkingest.LedgerTransaction, 0, limitHint)
	for {
		tx, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading transaction: %w", err)
		}
		transactions = append(transactions, tx)
	}
	return transactions, nil
}

// prepareBackendRange tells the backend which range we plan to fetch so
// buffered backends (like the RPC reader) can pre-warm.
func prepareBackendRange(ctx context.Context, backend ledgerbackend.LedgerBackend, startLedger, endLedger uint32) error {
	var ledgerRange ledgerbackend.Range
	if endLedger == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
		log.Ctx(ctx).Infof("Prepared backend with unbounded range from ledger %d", startLedger)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(startLedger, endLedger)
		log.Ctx(ctx).Infof("Prepared backend with bounded range [%d, %d]", startLedger, endLedger)
	}

	if err := backend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing range from %d: %w", startLedger, err)
	}
	return nil
}

// fetchLedgerWithRetry wraps GetLedger with bounded exponential backoff.
// It honours ctx cancellation between attempts and gives up after
// maxLedgerFetchRetries failures.
func fetchLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	backoff := initialRetryBackoff
	var lastErr error

	for attempt := 1; attempt <= maxLedgerFetchRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return xdr.LedgerCloseMeta{}, err
		}

		meta, err := backend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return meta, nil
		}

		// Context cancellation is not transient — surface immediately.
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return xdr.LedgerCloseMeta{}, err
		}

		lastErr = err
		log.Ctx(ctx).Warnf("Error fetching ledger %d (attempt %d/%d): %v, retrying in %v",
			ledgerSeq, attempt, maxLedgerFetchRetries, err, backoff)

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
