package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Trustless-Work/Indexer/internal/ingest"
	"github.com/Trustless-Work/Indexer/internal/services"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// rpcHealthTimeout bounds the initial RPC health probe used to discover the
// latest ledger when START_LEDGER is not provided.
const rpcHealthTimeout = 10 * time.Second

// envLogLevel selects the logrus level (e.g. "info", "debug", "trace").
const envLogLevel = "LOG_LEVEL"

func main() {
	preConfigureLogger()
	log.Info("Starting ingest...")

	// Cancel ctx on SIGINT/SIGTERM so the ingestion loop and sink shut down
	// gracefully instead of being killed mid-ledger.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		// Context cancellation is the expected exit path on SIGTERM and is not
		// a failure — exit cleanly.
		if errors.Is(err, context.Canceled) {
			log.Info("Ingest stopped by signal")
			return
		}
		log.Fatalf("ingest: %v", err)
	}
}

// run wires configuration, resolves the starting ledger if needed, and
// delegates to ingest.Ingest. Returning errors (rather than calling Fatal)
// keeps main testable and ensures deferred Close() calls in callees run.
func run(ctx context.Context) error {
	cfg, err := ingest.LoadConfigFromEnv()
	if err != nil {
		return err
	}

	if cfg.StartLedger == 0 {
		latest, err := fetchLatestLedger(ctx, cfg)
		if err != nil {
			return err
		}
		log.Infof("START_LEDGER unset; using latest ledger from RPC: %d", latest)
		cfg.StartLedger = latest
	}

	return ingest.Ingest(ctx, cfg)
}

// fetchLatestLedger queries the RPC health endpoint to discover the current
// network tip. Used when no explicit START_LEDGER is provided.
func fetchLatestLedger(ctx context.Context, cfg ingest.Config) (uint32, error) {
	httpClient := &http.Client{Timeout: rpcHealthTimeout}

	rpcClient, err := services.NewRPCService(cfg.RPCURL, cfg.NetworkPassphrase, httpClient)
	if err != nil {
		return 0, err
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	health, err := rpcClient.GetHealth()
	if err != nil {
		return 0, err
	}

	return health.LatestLedger, nil
}

// preConfigureLogger installs the default logger using LOG_LEVEL (default
// info). Trace-level was the previous default and was too noisy for
// production deployments.
func preConfigureLogger() {
	logger := log.New()
	level := logrus.InfoLevel
	if raw := os.Getenv(envLogLevel); raw != "" {
		if parsed, err := logrus.ParseLevel(raw); err == nil {
			level = parsed
		}
	}
	logger.SetLevel(level)
	log.DefaultLogger = logger
}
