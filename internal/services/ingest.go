package services

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer"
	"github.com/Trustless-Work/Indexer/internal/utils"
	"github.com/alitto/pond/v2"
	"github.com/stellar/go-stellar-sdk/historyarchive"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
	"github.com/stellar/go-stellar-sdk/xdr"
)

const (
	// maxLedgerFetchRetries is the maximum number of retry attempts when fetching a ledger fails.
	maxLedgerFetchRetries = 10
	// initialRetryBackoff is the initial backoff between retry attempts. It doubles
	// on every failure up to maxRetryBackoff.
	initialRetryBackoff = time.Second
	// maxRetryBackoff is the maximum backoff duration between retry attempts.
	maxRetryBackoff = 30 * time.Second
	// IngestionModeLive represents continuous ingestion from the latest ledger onwards.
	IngestionModeLive = "live"
	// IngestionModeBackfill represents historical ledger ingestion for a specified range.
	IngestionModeBackfill = "backfill"
)

// LedgerBackendFactory creates new LedgerBackend instances for parallel batch processing.
// Each batch needs its own backend because LedgerBackend is not thread-safe.
type LedgerBackendFactory func(ctx context.Context) (ledgerbackend.LedgerBackend, error)

// IngestServiceConfig holds the configuration for creating an IngestService.
type IngestServiceConfig struct {
	// === Core ===
	IngestionMode string
	//Models        *data.Models

	// === Stellar Network ===
	Network           string
	NetworkPassphrase string
	Archive           historyarchive.ArchiveInterface
	RPCService        RPCService

	// === Ledger Backend ===
	LedgerBackend        ledgerbackend.LedgerBackend
	LedgerBackendFactory LedgerBackendFactory

	// === Output ===
	// NOTE (Phase 2 of overhaul, 2026-05-13): the sink field was
	// removed. Sink emission is now per-envelope via a new Publisher
	// abstraction that will be wired in the next overhaul phase. Until
	// then this service processes ledgers and discards the buffer.

	// === Cursors ===
	LatestLedgerCursorName string
	OldestLedgerCursorName string

	// === Processing Options ===
	GetLedgersLimit            int
	SkipTxMeta                 bool
	SkipTxEnvelope             bool
	EnableParticipantFiltering bool

	// === Backfill Tuning ===
	BackfillWorkers           int
	BackfillBatchSize         int
	BackfillDBInsertBatchSize int
	CatchupThreshold          int
}

type IngestService interface {
	Run(ctx context.Context, startLedger uint32, endLedger uint32) error
}

var _ IngestService = (*ingestService)(nil)

type ingestService struct {
	rpcService           RPCService
	ledgerBackend        ledgerbackend.LedgerBackend
	ledgerBackendFactory LedgerBackendFactory
	networkPassphrase    string
	getLedgersLimit      int
	ledgerIndexer        *indexer.Indexer
}

func NewIngestService(cfg IngestServiceConfig) (*ingestService, error) {
	if cfg.RPCService == nil {
		return nil, errors.New("rpc service is required")
	}
	if cfg.LedgerBackend == nil {
		return nil, errors.New("ledger backend is required")
	}

	// Create worker pool for the ledger indexer (parallel transaction processing within a ledger)
	ledgerIndexerPool := pond.NewPool(0)

	return &ingestService{
		rpcService:           cfg.RPCService,
		ledgerBackend:        cfg.LedgerBackend,
		ledgerBackendFactory: cfg.LedgerBackendFactory,
		networkPassphrase:    cfg.NetworkPassphrase,
		getLedgersLimit:      cfg.GetLedgersLimit,
		ledgerIndexer:        indexer.NewIndexer(cfg.NetworkPassphrase, ledgerIndexerPool, cfg.SkipTxMeta, cfg.SkipTxEnvelope),
	}, nil
}

func (m *ingestService) Run(ctx context.Context, startLedger uint32, endLedger uint32) error {

	// Prepare backend range
	if err := m.prepareBackendRange(ctx, startLedger, endLedger); err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	currentLedger := startLedger
	log.Ctx(ctx).Infof("Starting ingestion loop from ledger: %d", currentLedger)
	for endLedger == 0 || currentLedger <= endLedger {
		if err := ctx.Err(); err != nil {
			log.Ctx(ctx).Infof("Ingestion loop stopped at ledger %d: %v", currentLedger, err)
			return nil
		}

		ledgerMeta, err := m.fetchLedgerWithRetry(ctx, currentLedger)
		if err != nil {
			return fmt.Errorf("fetching ledger %d: %w", currentLedger, err)
		}

		totalStart := time.Now()
		if err := m.processLedger(ctx, ledgerMeta); err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}

		log.Ctx(ctx).Infof("Processed ledger %d in %v", currentLedger, time.Since(totalStart))
		currentLedger++
	}

	log.Ctx(ctx).Infof("Backfill complete: processed ledgers %d to %d", startLedger, endLedger)
	return nil
}

// fetchLedgerWithRetry calls GetLedger with bounded exponential backoff. It
// honours ctx cancellation between attempts and gives up after
// maxLedgerFetchRetries failures.
func (m *ingestService) fetchLedgerWithRetry(ctx context.Context, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	backoff := initialRetryBackoff
	var lastErr error

	for attempt := 1; attempt <= maxLedgerFetchRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return xdr.LedgerCloseMeta{}, err
		}

		ledgerMeta, err := m.ledgerBackend.GetLedger(ctx, ledgerSeq)
		if err == nil {
			return ledgerMeta, nil
		}

		// Surface context cancellation immediately — it is not transient.
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

// prepareBackendRange prepares the ledger backend with the appropriate range type.
// Returns the operating mode (livestreaming vs backfill).
func (m *ingestService) prepareBackendRange(ctx context.Context, startLedger, endLedger uint32) error {
	var ledgerRange ledgerbackend.Range
	if endLedger == 0 {
		ledgerRange = ledgerbackend.UnboundedRange(startLedger)
		log.Ctx(ctx).Infof("Prepared backend with unbounded range starting from ledger %d", startLedger)
	} else {
		ledgerRange = ledgerbackend.BoundedRange(startLedger, endLedger)
		log.Ctx(ctx).Infof("Prepared backend with bounded range [%d, %d]", startLedger, endLedger)
	}

	if err := m.ledgerBackend.PrepareRange(ctx, ledgerRange); err != nil {
		return fmt.Errorf("preparing datastore backend unbounded range from %d: %w", startLedger, err)
	}
	return nil
}

// processLedger processes a single ledger through the ingestion phases.
// Phase 1: Get transactions from ledger
// Phase 2: Process transactions using Indexer (parallel within ledger)
//
// NOTE (Phase 2 of overhaul, 2026-05-13): the previous Phase 3 ("hand
// the buffer to the sink") was removed because the sink interface
// switched to a per-envelope contract (sink.Sink.Publish). The new
// publisher path that walks the buffer, builds Envelopes and calls
// Publish lives in Phase 3 of the overhaul. Until then this function
// processes a ledger and discards the buffer — same behavior the
// pipeline had before Sprint 1's wiring. The cursor stays in lock-step
// with this no-op while the new path is being built.
func (m *ingestService) processLedger(ctx context.Context, ledgerMeta xdr.LedgerCloseMeta) error {
	ledgerSeq := ledgerMeta.LedgerSequence()

	// Phase 1: Get transactions from ledger
	transactions, err := m.getLedgerTransactions(ctx, ledgerMeta)
	if err != nil {
		return fmt.Errorf("getting transactions for ledger %d: %w", ledgerSeq, err)
	}

	// Phase 2: Process transactions using Indexer (parallel within ledger).
	// The buffer is populated but intentionally discarded until the new
	// Publisher is wired up in the next phase of the overhaul.
	buffer := indexer.NewIndexerBuffer()
	if _, err := m.ledgerIndexer.ProcessLedgerTransactions(ctx, transactions, buffer); err != nil {
		return fmt.Errorf("processing transactions for ledger %d: %w", ledgerSeq, err)
	}
	_ = buffer

	return nil
}

func (m *ingestService) getLedgerTransactions(ctx context.Context, xdrLedgerCloseMeta xdr.LedgerCloseMeta) ([]ingest.LedgerTransaction, error) {
	ledgerTxReader, err := ingest.NewLedgerTransactionReaderFromLedgerCloseMeta(m.networkPassphrase, xdrLedgerCloseMeta)
	if err != nil {
		return nil, fmt.Errorf("creating ledger transaction reader: %w", err)
	}
	defer utils.DeferredClose(ctx, ledgerTxReader, "closing ledger transaction reader")

	// Pre-allocate with the limit hint to avoid repeated slice growth on busy ledgers.
	initialCapacity := m.getLedgersLimit
	if initialCapacity <= 0 {
		initialCapacity = 64
	}
	transactions := make([]ingest.LedgerTransaction, 0, initialCapacity)
	for {
		tx, err := ledgerTxReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("reading ledger: %w", err)
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}
