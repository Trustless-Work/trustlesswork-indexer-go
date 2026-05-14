// Package config is the single entry point for the Indexer's runtime
// configuration. All env reads happen here; other packages receive the
// parsed and validated *Config struct from main and treat it as
// read-only. This keeps the "where do I configure X?" question to one
// place.
//
// Loading is done via github.com/caarlos0/env/v11, which drives parsing
// from struct tags (`env:"..."`, `envDefault:"..."`). The Config struct
// below is the contract; adding a new tunable is a matter of adding a
// tagged field.
//
// The String() method dumps the effective config at boot, redacting
// secrets and URL credentials. Operators see exactly what the binary is
// running with, without secret leakage in logs.
//
// Future loaders (LoadFromFile, LoadFromVault) can produce the same
// *Config without changing callers — the struct is the contract, the
// loader is the variable part.
package config

import "time"

// Config is the full runtime configuration tree. Nested by domain so
// callers can pass sub-configs down without overexposing settings (e.g.
// the sink package receives SinkConfig + RabbitMQConfig, not the whole
// thing).
type Config struct {
	Network   NetworkConfig   `envPrefix:"NETWORK_"`
	RPC       RPCConfig       `envPrefix:"RPC_"`
	Indexer   IndexerConfig   `envPrefix:"INDEXER_"`
	Sink      SinkConfig      `envPrefix:"SINK_"`
	RabbitMQ  RabbitMQConfig  `envPrefix:"RABBITMQ_"`
	State     StateConfig     `envPrefix:"STATE_"`
	Watchlist WatchlistConfig `envPrefix:"WATCHLIST_"`
	Health    HealthConfig    `envPrefix:"HEALTH_"`
	Logging   LoggingConfig   `envPrefix:"LOG_"`

	// StrictMode controls whether errs.IsSkippable errors halt the
	// pipeline (true) or are logged and skipped (false). Default true
	// in production: prefer halt-and-alert over silent data loss.
	StrictMode bool `env:"STRICT_MODE" envDefault:"true"`
}

// NetworkConfig identifies which Stellar network this Indexer instance
// targets. Name is the short label used in routing keys and log fields;
// Passphrase is the cryptographic identifier and is part of every signed
// envelope at the Stellar layer.
type NetworkConfig struct {
	Name       string `env:"NAME" envDefault:"testnet"`
	Passphrase string `env:"PASSPHRASE" envDefault:"Test SDF Network ; September 2015"`
}

// RPCConfig points the Indexer at a Soroban-capable RPC endpoint and
// bounds the per-request timeout. URL is required and has no default
// because pointing at the wrong RPC is one of the easier ways to
// silently index the wrong network.
type RPCConfig struct {
	URL            string        `env:"URL,required"`
	RequestTimeout time.Duration `env:"REQUEST_TIMEOUT" envDefault:"30s"`
}

// IndexerConfig tunes the ledger ingestion loop.
type IndexerConfig struct {
	// StartLedger is the first ledger to process when no state file
	// exists. Zero means "start from the RPC tip", which is the
	// safe default for live mode but loses backfill.
	StartLedger uint32 `env:"START_LEDGER"`

	// EndLedger bounds processing for backfill. Zero means unbounded
	// (live mode). Validation requires StartLedger <= EndLedger when
	// EndLedger > 0.
	EndLedger uint32 `env:"END_LEDGER"`

	// LedgerBackendType selects between live RPC ("rpc") and archived
	// ledger storage ("datastore"). Only "rpc" is implemented today.
	LedgerBackendType string `env:"LEDGER_BACKEND_TYPE" envDefault:"rpc"`

	// GetLedgersLimit bounds how many ledgers a single backend
	// PrepareRange covers. The Stellar Go SDK documents this as an
	// internal buffer size; 100 has worked well empirically.
	GetLedgersLimit int `env:"GET_LEDGERS_LIMIT" envDefault:"100"`

	// Workers caps the per-ledger parallel-tx worker pool. Zero means
	// "auto" (runtime.NumCPU * 2 at construction time). Setting an
	// explicit value is useful for resource-constrained environments.
	Workers int `env:"WORKERS"`

	// SkipTxMeta and SkipTxEnvelope strip the corresponding XDR fields
	// from captured transactions to save bandwidth / storage. Default
	// false (keep everything).
	SkipTxMeta     bool `env:"SKIP_TX_META"`
	SkipTxEnvelope bool `env:"SKIP_TX_ENVELOPE"`
}

// SinkConfig selects which transport receives envelopes. Concrete sink
// configuration lives in dedicated structs (RabbitMQConfig, etc.).
type SinkConfig struct {
	// Type is one of "noop" or "rabbitmq". The "noop" sink discards
	// everything and is the default for dev to avoid requiring a
	// broker.
	Type string `env:"TYPE" envDefault:"noop"`
}

// RabbitMQConfig configures the RabbitMQ sink. Only consumed when
// SinkConfig.Type == "rabbitmq". URL is validated as required by the
// cross-field check in Validate, not by the env tag, because we don't
// want noop deployments to be forced to set RABBITMQ_URL.
type RabbitMQConfig struct {
	URL               string `env:"URL"`
	Exchange          string `env:"EXCHANGE" envDefault:"stellar.events"`
	PublisherConfirms bool   `env:"PUBLISHER_CONFIRMS" envDefault:"true"`
}

// StateConfig governs the on-disk state file (cursor + watchlist).
type StateConfig struct {
	Path  string `env:"PATH" envDefault:"./indexer.state.json"`
	Reset bool   `env:"RESET"`
}

// WatchlistConfig holds optional seed source for the escrow watchlist
// at first boot.
type WatchlistConfig struct {
	SeedPath string `env:"SEED_PATH"`
}

// HealthConfig governs the HTTP health/metrics server.
type HealthConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"true"`
	Port    int  `env:"PORT" envDefault:"8080"`
}

// LoggingConfig governs the logger.
//   - Level is a logrus level string (panic, fatal, error, warn, info,
//     debug, trace).
//   - Format is one of "auto" (JSON when stdout is not a TTY), "json", or
//     "text".
type LoggingConfig struct {
	Level  string `env:"LEVEL" envDefault:"info"`
	Format string `env:"FORMAT" envDefault:"auto"`
}
