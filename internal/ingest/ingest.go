// Package ingest is the composition root for the Indexer pipeline. It
// wires together the durable state, the sink, the detector, the
// publisher and the orchestration service from a single *config.Config.
//
// Boot sequence on Ingest():
//
//  1. Open the state store (acquires the advisory lock).
//  2. Load or initialize State (cursor + watchlist). On STATE_RESET=true
//     the existing state is ignored. On first boot the watchlist seed
//     (WATCHLIST_SEED_PATH) is loaded if present.
//  3. Validate the loaded state against the configured network. A
//     mismatch is fatal — operator must intervene.
//  4. Resolve the starting ledger:
//       - persistent cursor present → resume at LastLedgerSeq + 1
//       - else INDEXER_START_LEDGER > 0 → use it
//       - else fetch the current network tip from the RPC health
//         endpoint and start there.
//  5. Construct the sink, publisher, detector, ledger backend, and
//     ingest service.
//  6. Run the loop until ctx cancellation or terminal error.
//
// The caller (cmd/ingest.go) is responsible for ctx, signal handling,
// and config loading. Ingest itself never reads env vars.
package ingest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/detector"
	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/health"
	"github.com/Trustless-Work/Indexer/internal/metrics"
	"github.com/Trustless-Work/Indexer/internal/publisher"
	"github.com/Trustless-Work/Indexer/internal/services"
	sinkpkg "github.com/Trustless-Work/Indexer/internal/sink"
	sinkfactory "github.com/Trustless-Work/Indexer/internal/sink/factory"
	"github.com/Trustless-Work/Indexer/internal/state"
	"github.com/Trustless-Work/Indexer/internal/utils"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// Version is the Indexer's reported version. Override at build time via
//
//	go build -ldflags "-X github.com/Trustless-Work/Indexer/internal/ingest.Version=v1.2.3"
//
// "dev" is the safe default for unstamped builds.
var Version = "dev"

// httpClientTimeout is the per-request timeout for the auxiliary HTTP
// client used to call the RPC health endpoint at boot. Generous because
// it's a one-off; the per-ledger fetch path uses the SDK's own client.
const httpClientTimeout = 30 * time.Second

// Ingest is the entry point of the Indexer pipeline. It blocks until
// ctx cancellation or terminal error.
//
// Errors are returned with %w wrapping the underlying sentinel; the
// caller can distinguish operator-correctable errors (network
// mismatch, lock conflict, missing required config) from runtime ones.
func Ingest(ctx context.Context, cfg *config.Config) error {
	log.Ctx(ctx).Info(cfg.String())

	// --- State + watchlist -----------------------------------------
	store, err := state.NewFileStore(cfg.State.Path)
	if err != nil {
		return fmt.Errorf("opening state store: %w", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			log.Ctx(ctx).Warnf("closing state store: %v", err)
		}
	}()

	current, startLedger, err := loadOrInitState(ctx, cfg, store)
	if err != nil {
		return err
	}

	watchlist := state.NewWatchlist(current.EscrowContracts)
	metrics.SetWatchlistSize(cfg.Network.Name, watchlist.Size())

	log.Ctx(ctx).Infof("State ready: start_ledger=%d, watchlist_size=%d", startLedger, watchlist.Size())

	// --- Sink + publisher ------------------------------------------
	outSink, err := sinkfactory.New(cfg)
	if err != nil {
		return fmt.Errorf("building sink: %w", err)
	}
	defer utils.DeferredClose(ctx, outSink, "closing sink")
	updateSinkUp(ctx, cfg.Sink.Type, outSink)

	pub := publisher.New(cfg.Network.Name, outSink)

	// --- Detector --------------------------------------------------
	det := detector.New(cfg.Network.Passphrase, cfg.Network.Name, events.DefaultTWTopicFilter(), watchlist)

	// --- Ledger backend --------------------------------------------
	ledgerBackend, err := NewLedgerBackend(ctx, cfg)
	if err != nil {
		return fmt.Errorf("creating ledger backend: %w", err)
	}

	// --- Ingest service --------------------------------------------
	svc, err := services.NewIngestService(services.IngestServiceConfig{
		NetworkName:       cfg.Network.Name,
		NetworkPassphrase: cfg.Network.Passphrase,
		LedgerBackend:     ledgerBackend,
		Detector:          det,
		Publisher:         pub,
		StateStore:        store,
		Watchlist:         watchlist,
		StrictMode:        cfg.StrictMode,
	})
	if err != nil {
		return fmt.Errorf("constructing ingest service: %w", err)
	}

	// --- Health server --------------------------------------------
	// Bound synchronously so port conflicts fail-fast at boot.
	// Construction returns nil when HEALTH_ENABLED=false.
	healthSrv, err := startHealthServer(ctx, cfg, svc, outSink)
	if err != nil {
		return err
	}
	var healthWG sync.WaitGroup
	if healthSrv != nil {
		healthWG.Add(1)
		go func() {
			defer healthWG.Done()
			if err := healthSrv.Serve(ctx); err != nil {
				log.Ctx(ctx).Errorf("health server: %v", err)
			}
		}()
		defer healthWG.Wait()
	}

	log.Ctx(ctx).Infof("Ingest starting (network=%s, start=%d, end=%d, strict=%v)",
		cfg.Network.Name, startLedger, cfg.Indexer.EndLedger, cfg.StrictMode)

	if err := svc.Run(ctx, startLedger, cfg.Indexer.EndLedger, current); err != nil {
		return fmt.Errorf("running ingest from %d to %d: %w", startLedger, cfg.Indexer.EndLedger, err)
	}
	return nil
}

// startHealthServer constructs and binds the health HTTP server. The
// listener is opened synchronously so port conflicts surface as a boot
// error rather than a goroutine warning. If HEALTH_ENABLED=false,
// returns (nil, nil) — the caller skips the goroutine.
//
// The pinger plumbed into /readyz comes from the sink if it implements
// HealthChecker; otherwise nil (the noop sink, for example, has no
// failure mode worth probing — health.Server treats nil as always
// ready, which is the right semantic for it).
func startHealthServer(ctx context.Context, cfg *config.Config, snapper health.Snapshotter, outSink sinkpkg.Sink) (*health.Server, error) {
	if !cfg.Health.Enabled {
		log.Ctx(ctx).Info("HEALTH_ENABLED=false — skipping health server")
		return nil, nil
	}

	var pinger health.Pinger
	if hc, ok := outSink.(sinkpkg.HealthChecker); ok {
		pinger = hc.Ping
	}

	addr := fmt.Sprintf(":%d", cfg.Health.Port)
	srv, err := health.New(health.Config{
		Addr:      addr,
		Version:   Version,
		Network:   cfg.Network.Name,
		SinkType:  cfg.Sink.Type,
		StartedAt: time.Now().UTC(),
	}, snapper, pinger)
	if err != nil {
		return nil, fmt.Errorf("starting health server on %s: %w", addr, err)
	}
	log.Ctx(ctx).Infof("Health server listening on %s (/healthz /readyz /metrics /status)", srv.Addr())
	return srv, nil
}

// loadOrInitState figures out the boot State and the ledger to start
// at. The logic in one place:
//
//   - STATE_RESET=true OR no state file exists → first-boot path:
//     build a fresh State, seed watchlist from WATCHLIST_SEED_PATH,
//     start from INDEXER_START_LEDGER (or RPC tip if 0).
//   - State file exists and matches network → resume from
//     cursor + 1.
//   - State file exists but network mismatches → ErrStateNetworkMismatch
//     (fatal).
//   - State file exists but is corrupted / unsupported version → the
//     underlying state.* sentinel propagates (fatal).
func loadOrInitState(ctx context.Context, cfg *config.Config, store state.Store) (state.State, uint32, error) {
	if cfg.State.Reset {
		log.Ctx(ctx).Info("STATE_RESET=true — ignoring any existing state file")
		return initialState(ctx, cfg)
	}

	loaded, err := store.Load(ctx)
	switch {
	case errors.Is(err, state.ErrStateNotFound):
		log.Ctx(ctx).Info("No state file found — initializing fresh state")
		return initialState(ctx, cfg)
	case err != nil:
		return state.State{}, 0, fmt.Errorf("loading state: %w", err)
	}

	if loaded.Network != cfg.Network.Passphrase {
		return state.State{}, 0, fmt.Errorf("%w: state has %q, configured %q",
			state.ErrStateNetworkMismatch, loaded.Network, cfg.Network.Passphrase)
	}

	startLedger := loaded.LastLedgerSeq + 1
	log.Ctx(ctx).Infof("Resuming from state: last_ledger=%d, next=%d, watchlist_size=%d",
		loaded.LastLedgerSeq, startLedger, len(loaded.EscrowContracts))
	return loaded, startLedger, nil
}

// initialState builds a State for a clean boot: empty cursor, watchlist
// seeded from WATCHLIST_SEED_PATH (if any), and a start_ledger chosen
// from INDEXER_START_LEDGER or the RPC tip.
func initialState(ctx context.Context, cfg *config.Config) (state.State, uint32, error) {
	seedIDs, err := state.LoadSeed(cfg.Watchlist.SeedPath)
	if err != nil {
		return state.State{}, 0, fmt.Errorf("loading watchlist seed: %w", err)
	}
	if len(seedIDs) > 0 {
		log.Ctx(ctx).Infof("Loaded %d entries from watchlist seed %q", len(seedIDs), cfg.Watchlist.SeedPath)
	}

	startLedger := cfg.Indexer.StartLedger
	if startLedger == 0 {
		tip, err := fetchLatestLedger(ctx, cfg)
		if err != nil {
			return state.State{}, 0, fmt.Errorf("resolving start ledger from RPC tip: %w", err)
		}
		log.Ctx(ctx).Infof("START_LEDGER unset; using RPC tip %d", tip)
		startLedger = tip
	}

	s := state.NewState(cfg.Network.Passphrase, events.CurrentSchemaVersion).
		WithWatchlist(seedIDs)
	return s, startLedger, nil
}

// fetchLatestLedger queries the configured RPC's health endpoint for
// its current latest ledger. Used only at first boot to choose a
// reasonable starting point when none was configured.
func fetchLatestLedger(ctx context.Context, cfg *config.Config) (uint32, error) {
	_ = ctx // RPCService methods don't accept ctx yet; reserved for Phase 4.

	httpClient := &http.Client{Timeout: httpClientTimeout}
	rpc, err := services.NewRPCService(cfg.RPC.URL, cfg.Network.Passphrase, httpClient)
	if err != nil {
		return 0, err
	}
	health, err := rpc.GetHealth()
	if err != nil {
		return 0, err
	}
	return health.LatestLedger, nil
}

// updateSinkUp probes the sink (if it supports HealthChecker) and
// updates the indexer_sink_up gauge. Best-effort — a probe failure
// here does not block boot. The gauge tells operators the answer; the
// main loop will surface a real failure on the first Publish attempt.
func updateSinkUp(ctx context.Context, sinkType string, s sinkpkg.Sink) {
	hc, ok := s.(sinkpkg.HealthChecker)
	if !ok {
		// Sinks without health probes are reported as up by default;
		// they cannot fail in a probeable way.
		metrics.SetSinkUp(sinkType, true)
		return
	}
	metrics.SetSinkUp(sinkType, hc.Ping(ctx) == nil)
}
