// Command ingest is the Indexer's entry point. It loads the
// centralized configuration, installs the configured logger, sets up
// graceful shutdown on SIGINT/SIGTERM, and hands control to the
// composition root in internal/ingest.
//
// Concretely, main does as little as possible. Configuration, state
// loading, sink construction and the actual pipeline live behind a
// single ingest.Ingest call. This keeps main free of business logic
// and makes the run() function testable in principle (no os.Exit, no
// log.Fatal).
package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/Trustless-Work/Indexer/internal/ingest"
	"github.com/sirupsen/logrus"
	"github.com/stellar/go-stellar-sdk/support/log"
)

func main() {
	// Cancel ctx on SIGINT/SIGTERM so the pipeline shuts down
	// gracefully instead of being killed mid-ledger.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil {
		// Context cancellation is the expected exit path on SIGTERM
		// and is not a failure — exit cleanly.
		if errors.Is(err, context.Canceled) {
			log.Info("Ingest stopped by signal")
			return
		}
		// Logger may not be configured if config failed before
		// preConfigureLogger(); fall back to stderr in that case.
		if log.DefaultLogger == nil {
			logrus.WithError(err).Fatal("ingest: fatal startup error")
		}
		log.Fatalf("ingest: %v", err)
	}
}

// run loads config, configures the logger, and delegates to
// ingest.Ingest. Returns errors instead of calling Fatal so that
// deferred Close calls in callees run cleanly.
func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	configureLogger(cfg)

	log.Ctx(ctx).Info("Starting Indexer")
	return ingest.Ingest(ctx, cfg)
}

// configureLogger applies cfg.Logging to both the global logrus
// logger (used directly by the sink/* packages via logrus.WithFields)
// and to the Stellar SDK's log wrapper (used by services and rpc via
// log.Ctx). The two loggers are independent instances in the SDK
// version we use, so we configure both to avoid mixed output.
//
// LOG_FORMAT semantics:
//   - "json": JSON formatter always.
//   - "text": text formatter always.
//   - "auto" (default): text if stdout is a TTY, JSON otherwise. The
//     conservative default for containers (stdout piped) is JSON.
func configureLogger(cfg *config.Config) {
	level := logrus.InfoLevel
	if parsed, err := logrus.ParseLevel(cfg.Logging.Level); err == nil {
		level = parsed
	}

	formatter := chooseFormatter(cfg.Logging.Format)

	// Global logrus instance: drives direct logrus.WithFields calls in
	// the sink packages.
	logrus.SetLevel(level)
	logrus.SetFormatter(formatter)

	// Stellar SDK log wrapper: drives log.Ctx(ctx) calls in services.
	// SetFormatter is not exposed by support/log.Entry; we can only
	// install the level. Operators wanting the JSON formatter on these
	// lines should pipe stdout into a separate JSON-emitting structlog
	// processor, or switch the project to use logrus directly. Tracked
	// for a follow-up sprint.
	stellarLogger := log.New()
	stellarLogger.SetLevel(level)
	log.DefaultLogger = stellarLogger
}

// chooseFormatter resolves cfg.Logging.Format to a concrete formatter.
// Defaults to JSON for any value other than "json"/"text"/"auto"; the
// config Validate() layer rejects anything outside those, so a default
// here is purely defensive.
func chooseFormatter(format string) logrus.Formatter {
	switch format {
	case "json":
		return &logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z07:00"}
	case "text":
		return &logrus.TextFormatter{FullTimestamp: true}
	default: // "auto" and anything else
		if isTTY(os.Stdout) {
			return &logrus.TextFormatter{FullTimestamp: true}
		}
		return &logrus.JSONFormatter{TimestampFormat: "2006-01-02T15:04:05.000Z07:00"}
	}
}

// isTTY reports whether the given file is attached to a terminal.
// Used to drive the auto-detection of LOG_FORMAT. Returns false on
// systems that don't expose os.File.Stat() reliably; the conservative
// default (false → JSON) is correct for containers.
func isTTY(f *os.File) bool {
	if f == nil {
		return false
	}
	info, err := f.Stat()
	if err != nil {
		return false
	}
	return (info.Mode() & os.ModeCharDevice) != 0
}
