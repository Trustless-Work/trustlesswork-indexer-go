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
	"regexp"
	"strconv"
	"time"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/health"
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
	// tipWaitInterval is the pause between polls when the RPC rejects a
	// request for a ledger it has not closed yet (beyond-tip). Kept short:
	// freshness at the tip is the product requirement.
	tipWaitInterval = time.Second
	// tipWaitWarnEvery is how many consecutive beyond-tip waits pass
	// between warnings. At 1s per wait, 30 waits ≈ 30s of a stalled tip —
	// worth surfacing, since downstream freshness depends on it.
	tipWaitWarnEvery = 30
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

	// Resume from the persisted cursor + escrow set if we have one. A
	// network mismatch is fatal — point at the right state file.
	// Gaps ride along unchanged: they are append-only evidence and every
	// Save below must carry them forward or the record is lost.
	var persistedEscrows []string
	var gaps []state.Gap
	resumed := false
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
		persistedEscrows = loaded.EscrowContracts
		gaps = loaded.Gaps
		resumed = true
		log.Ctx(ctx).Infof("Resuming from persisted cursor: next ledger %d", startLedger)
	}

	// Long-lived Soroban RPC client: used for tip resolution at boot and
	// for state fetches in the loop (the canonical way to read contract
	// state in Soroban).
	rpc := rpcclient.NewClient(cfg.RPC.URL, &http.Client{Timeout: cfg.RPC.RequestTimeout})
	defer func() { _ = rpc.Close() }()

	// Fail fast if the RPC serves a different network than configured.
	// The passphrase feeds transaction hashing in the ledger reader, so a
	// mismatch would produce silently wrong tx hashes on every ledger —
	// the worst kind of "works but lies" failure when switching networks.
	netInfo, err := rpc.GetNetwork(ctx)
	if err != nil {
		return fmt.Errorf("verifying RPC network: %w", err)
	}
	if netInfo.Passphrase != cfg.Network.Passphrase {
		return fmt.Errorf("RPC network mismatch: %s serves %q but NETWORK_PASSPHRASE is %q — fix RPC_URL or NETWORK_PASSPHRASE",
			cfg.RPC.URL, netInfo.Passphrase, cfg.Network.Passphrase)
	}

	// START_LEDGER unset (0) means "start from the network tip". Resolve it
	// via a direct RPC call: the RPC rejects a PrepareRange starting at 0,
	// and the backend's own GetLatestLedgerSequence requires PrepareRange
	// first (chicken-and-egg), so we ask the RPC straight up.
	if startLedger == 0 {
		latest, err := rpc.GetLatestLedger(ctx)
		if err != nil {
			return fmt.Errorf("resolving latest ledger from RPC: %w", err)
		}
		log.Ctx(ctx).Infof("START_LEDGER unset; starting from network tip %d", latest.Sequence)
		startLedger = latest.Sequence
	}

	// Escrow registry: identifies "our" contracts by approved WASM hash.
	// Populated by the discovery pass (and, later, an API seed). Built
	// before the start-ledger clamp so a clamp can persist the full
	// state (cursor + escrows + gap) in one atomic write.
	reg, err := registry.New(cfg.Escrow.ApprovedWasmHashes)
	if err != nil {
		return fmt.Errorf("building escrow registry: %w", err)
	}

	// Repopulate the registry: from persisted state on resume, otherwise
	// from the optional seed file (escrows created before the indexed
	// range). Discovery keeps adding new escrows from here on.
	if resumed {
		reg.Seed(persistedEscrows)
		log.Ctx(ctx).Infof("Restored %d escrows from state", reg.Size())
	} else {
		seedIDs, serr := state.LoadSeed(cfg.Escrow.SeedPath)
		if serr != nil {
			return fmt.Errorf("loading escrow seed: %w", serr)
		}
		if len(seedIDs) > 0 {
			reg.Seed(seedIDs)
			log.Ctx(ctx).Infof("Seeded %d escrows from %s", reg.Size(), cfg.Escrow.SeedPath)
		}
	}

	// Clamp the start ledger against the window this RPC actually serves.
	// Without this, a cursor that fell out of retention makes PrepareRange
	// fail deterministically and the process crash-loops until a human
	// intervenes (the 2026-07-22 incident). Skipping forward loses data,
	// so the skipped range is recorded as a Gap and persisted BEFORE any
	// ledger is processed — evidence for a later backfill.
	rpcWindow, err := rpc.GetHealth(ctx)
	if err != nil {
		return fmt.Errorf("fetching RPC health for start-ledger clamp: %w", err)
	}
	log.Ctx(ctx).Infof("RPC window: oldest=%d latest=%d retention=%d ledgers",
		rpcWindow.OldestLedger, rpcWindow.LatestLedger, rpcWindow.LedgerRetentionWindow)
	if clamped, gap, cerr := clampStartLedger(startLedger, rpcWindow.OldestLedger, rpcWindow.LatestLedger, time.Now()); cerr != nil {
		return cerr
	} else if gap != nil {
		log.Ctx(ctx).Warnf("Start ledger %d is below RPC retention (oldest %d): clamping forward — ledgers [%d, %d] are SKIPPED and recorded as a gap for later backfill",
			startLedger, rpcWindow.OldestLedger, gap.FromLedger, gap.ToLedger)
		gaps = append(gaps, *gap)
		startLedger = clamped
		if err := store.Save(ctx, state.State{
			Network:         cfg.Network.Passphrase,
			LastLedgerSeq:   startLedger - 1,
			EscrowContracts: reg.Snapshot(),
			Gaps:            gaps,
		}); err != nil {
			return fmt.Errorf("persisting clamp gap: %w", err)
		}
	}

	// A bounded backfill whose entire range fell out of retention cannot
	// run at all — say so instead of letting PrepareRange fail cryptically.
	if endLedger != 0 && startLedger > endLedger {
		return fmt.Errorf("backfill range is unservable: clamped start %d exceeds INDEXER_END_LEDGER %d (range fell out of RPC retention) — use an archive RPC for this range", startLedger, endLedger)
	}

	if err := prepareBackendRange(ctx, backend, startLedger, endLedger); err != nil {
		return fmt.Errorf("preparing backend range: %w", err)
	}

	// State detector: fetches the current DataKey::Escrow entry of each
	// escrow that had activity in a ledger via getLedgerEntries — the
	// canonical Soroban way to read contract state.
	stateDetector := processors.NewEscrowStateDetector(rpc, reg)

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

	// Progress tracker + health server. The tracker always exists (the
	// loop reports to it unconditionally — no nil checks in the hot
	// path); the HTTP server only binds when enabled. Its lifetime is
	// tied to ctx, same as the loop it observes.
	tracker := health.NewTracker(cfg.Network.Name)
	if cfg.Health.Enabled {
		go health.Serve(ctx, fmt.Sprintf(":%d", cfg.Health.Port), tracker)
	}

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
		facts, activeEscrows, err := processLedger(ctx, ledgerIndexer, cfg.Network.Passphrase, cfg.Indexer.GetLedgersLimit, meta)
		if err != nil {
			return fmt.Errorf("processing ledger %d: %w", currentLedger, err)
		}

		// Publish each detected event/deposit. On failure we return
		// (strict): a dropped publish would be silent data loss.
		for _, ev := range facts {
			if err := outSink.Publish(ctx, events.FromEscrowEvent(cfg.Network.Name, ev)); err != nil {
				return fmt.Errorf("publishing event %s:%d at ledger %d: %w", ev.TxHash, ev.EventIndex, currentLedger, err)
			}
		}

		// Fetch current state for each escrow with activity, then publish.
		ledgerClosedAt := time.Unix(meta.LedgerCloseTime(), 0).UTC()
		states, err := stateDetector.FetchStates(ctx, activeEscrows, currentLedger, ledgerClosedAt)
		if err != nil {
			return fmt.Errorf("fetching state at ledger %d: %w", currentLedger, err)
		}
		for _, sc := range states {
			if err := outSink.Publish(ctx, events.FromStateChange(cfg.Network.Name, sc)); err != nil {
				return fmt.Errorf("publishing state %s at ledger %d: %w", sc.EscrowID, currentLedger, err)
			}
		}

		// Persist the cursor only after every fact in this ledger was
		// published. On a crash between publish and save we reprocess the
		// ledger and the consumer dedupes by message_id (at-least-once).
		if err := store.Save(ctx, state.State{
			Network:         cfg.Network.Passphrase,
			LastLedgerSeq:   currentLedger,
			EscrowContracts: reg.Snapshot(),
			Gaps:            gaps,
		}); err != nil {
			return fmt.Errorf("saving state at ledger %d: %w", currentLedger, err)
		}

		// age is how far behind the chain this ledger's data is by the time
		// we finished publishing it — the freshness number the API depends
		// on. At the tip it should sit around one close interval (~5-6s);
		// a growing age means we are falling behind (or catching up).
		log.Ctx(ctx).Infof("Processed ledger %d in %v (age %s) — known_escrows=%d escrow_events=%d state_changes=%d",
			currentLedger, time.Since(started), time.Since(ledgerClosedAt).Round(100*time.Millisecond), reg.Size(), len(facts), len(states))

		tracker.RecordLedger(health.Progress{
			LedgerSeq:      currentLedger,
			LedgerClosedAt: ledgerClosedAt,
			Duration:       time.Since(started),
			KnownEscrows:   reg.Size(),
			Events:         len(facts),
			StateChanges:   len(states),
			Gaps:           len(gaps),
		})

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
) ([]processors.EscrowEvent, []string, error) {
	transactions, err := readLedgerTransactions(ctx, networkPassphrase, limitHint, meta)
	if err != nil {
		return nil, nil, fmt.Errorf("reading transactions: %w", err)
	}

	facts, activeEscrows, err := ledgerIndexer.ProcessLedger(ctx, transactions)
	if err != nil {
		return nil, nil, err
	}
	return facts, activeEscrows, nil
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
//
// Window errors get special handling because some RPC providers reject a
// request for the next ledger with a "must be between oldest and latest"
// error instead of blocking until it closes (observed live on mainnet
// providers). Beyond-tip is normal tip-following — wait briefly without
// consuming an attempt. Below-retention is deterministic — no number of
// retries can fix it, so fail immediately with an actionable message
// instead of burning ~4 minutes and a Railway restart cycle.
func fetchLedgerWithRetry(ctx context.Context, backend ledgerbackend.LedgerBackend, ledgerSeq uint32) (xdr.LedgerCloseMeta, error) {
	backoff := initialRetryBackoff
	var lastErr error
	attempt := 1
	tipWaits := 0

	for attempt <= maxLedgerFetchRetries {
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

		switch class, oldest, latest := classifyWindowError(err, ledgerSeq); class {
		case windowBeyondTip:
			tipWaits++
			if tipWaits%tipWaitWarnEvery == 0 {
				log.Ctx(ctx).Warnf("Ledger %d still beyond RPC tip (%d) after ~%s — tip may be stalled",
					ledgerSeq, latest, time.Duration(tipWaits)*tipWaitInterval)
			}
			select {
			case <-ctx.Done():
				return xdr.LedgerCloseMeta{}, ctx.Err()
			case <-time.After(tipWaitInterval):
			}
			continue
		case windowBelowRetention:
			return xdr.LedgerCloseMeta{}, fmt.Errorf(
				"ledger %d is below the RPC retention window (oldest available: %d): the cursor is out of range and retrying cannot help — reset the state file, adjust INDEXER_START_LEDGER, or backfill via an archive RPC: %w",
				ledgerSeq, oldest, err)
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
		attempt++
	}

	return xdr.LedgerCloseMeta{}, fmt.Errorf("giving up after %d attempts: %w", maxLedgerFetchRetries, lastErr)
}

// windowErrorClass classifies an RPC "requested ledger outside my window"
// error relative to the ledger we asked for.
type windowErrorClass int

const (
	// windowNone: not a window error (or unparseable) — treat as transient.
	windowNone windowErrorClass = iota
	// windowBeyondTip: the ledger has not closed yet on this RPC.
	windowBeyondTip
	// windowBelowRetention: the ledger fell out of the RPC's retention.
	windowBelowRetention
)

// windowErrRe matches the stellar-rpc window error, e.g.:
//
//	[-32600] start ledger (63613791) must be between the oldest ledger: 2
//	and the latest ledger: 63613790 for this rpc instance
//
// The requested sequence is taken from the caller, not the message, so the
// match tolerates format drift around it.
var windowErrRe = regexp.MustCompile(`oldest ledger:?\s*(\d+).*?latest ledger:?\s*(\d+)`)

// classifyWindowError inspects err and, when it is a window error, returns
// where the requested ledger sits relative to the advertised window.
func classifyWindowError(err error, requested uint32) (windowErrorClass, uint32, uint32) {
	m := windowErrRe.FindStringSubmatch(err.Error())
	if m == nil {
		return windowNone, 0, 0
	}
	oldest64, oerr := strconv.ParseUint(m[1], 10, 32)
	latest64, lerr := strconv.ParseUint(m[2], 10, 32)
	if oerr != nil || lerr != nil {
		return windowNone, 0, 0
	}
	oldest, latest := uint32(oldest64), uint32(latest64)
	switch {
	case requested > latest:
		return windowBeyondTip, oldest, latest
	case requested < oldest:
		return windowBelowRetention, oldest, latest
	}
	return windowNone, oldest, latest
}
