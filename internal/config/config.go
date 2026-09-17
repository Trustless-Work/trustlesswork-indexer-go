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
	Network  NetworkConfig  `envPrefix:"NETWORK_"`
	RPC      RPCConfig      `envPrefix:"RPC_"`
	Indexer  IndexerConfig  `envPrefix:"INDEXER_"`
	Escrow   EscrowConfig   `envPrefix:"ESCROW_"`
	Sink     SinkConfig     `envPrefix:"SINK_"`
	RabbitMQ RabbitMQConfig `envPrefix:"RABBITMQ_"`
	State    StateConfig    `envPrefix:"STATE_"`
	Health   HealthConfig   `envPrefix:"HEALTH_"`
	Logging  LoggingConfig  `envPrefix:"LOG_"`

	// StrictMode controls whether errs.IsSkippable errors halt the
	// pipeline (true) or are logged and skipped (false). Default true
	// in production: prefer halt-and-alert over silent data loss.
	StrictMode bool `env:"STRICT_MODE" envDefault:"true"`

	// AdminToken guards the /admin/* control surface on the health
	// server (track/remove/refresh escrows, pause/resume, reconcile).
	// Empty (the default) disables the surface entirely. The health port
	// must never be public — admin auth is defence in depth behind the
	// private network, not a substitute for it.
	AdminToken string `env:"ADMIN_TOKEN" secret:"true"`

	// EnvelopeHMACKey is the key shared with the core API for signing
	// published envelopes (HMAC-SHA256 over the exact body bytes, sent
	// as the `x-tw-sig` AMQP header). The name and the raw-string key
	// semantics are the consumer's contract — the core reads the SAME
	// env var and feeds the string to its HMAC as-is, so no hex/base64
	// decoding may ever be applied here. Empty disables signing (the
	// consumer's rollout modes tolerate a missing header until it
	// switches to enforce).
	EnvelopeHMACKey string `env:"ENVELOPE_HMAC_KEY" secret:"true"`

	// Replay marks a one-shot `indexer replay` run (set by the CLI, not
	// by env): bounded range, nothing persisted (the live indexer keeps
	// its cursor and flock), watchlist read point-in-time, no command
	// consumption (it would compete with the live consumer on the same
	// queue), no heartbeat (a replay pinging the dead-man's switch would
	// mask a dead live indexer).
	Replay bool `env:"-"`
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

	// FallbackURLs is the ordered failover list tried when the endpoint
	// in use fails or its tip stalls: URL first, then these, wrapping
	// around until one works (the process exits only when ALL failed).
	// Every endpoint must serve the same network — a passphrase mismatch
	// disqualifies the endpoint at connect time. Order matters twice:
	// earlier entries are preferred, and when the cursor fell out of the
	// failing endpoint's retention, a DEEPER-retention (archive) fallback
	// turns what would be a recorded gap into actually served ledgers —
	// so list archive-grade providers here. Comma-separated in
	// RPC_FALLBACK_URLS; empty means no failover (single-endpoint
	// behaviour, as before).
	FallbackURLs []string `env:"FALLBACK_URLS" envSeparator:","`

	// LedgerFetchTimeout bounds a single getLedgers/getHealth request made
	// by the ledger backend. It is separate from RequestTimeout because a
	// ledger batch can weigh tens of MB on mainnet (~2.65MB per ledger,
	// limit=10 ≈ 27MB) while getLedgerEntries/getLatestLedger responses
	// are tiny. Without an explicit client here the SDK backend has NO
	// timeout at all and one hung connection stalls the loop forever.
	LedgerFetchTimeout time.Duration `env:"LEDGER_FETCH_TIMEOUT" envDefault:"120s"`
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

	// SweepEnabled toggles the reconciliation sweep (one budgeted
	// getLedgerEntries batch per ledger at the tip, rotating over the
	// whole watchlist). On by default; the kill switch exists for
	// operational emergencies (e.g. an RPC provider rate-limiting the
	// extra request) without needing a build.
	SweepEnabled bool `env:"SWEEP_ENABLED" envDefault:"true"`
}

// EscrowConfig declares which contracts count as TW escrows. A contract
// is recognised as an escrow when its WASM code hash is in
// ApprovedWasmHashes — one hash per published contract version. Adding a
// new contract version is a config change here, not a code change.
type EscrowConfig struct {
	// ApprovedWasmHashes is the set of approved escrow code hashes, each
	// a 32-byte hex string, comma-separated in the env var
	// (ESCROW_APPROVED_WASM_HASHES). May be empty in dev; the registry
	// then recognises escrows only via seed.
	ApprovedWasmHashes []string `env:"APPROVED_WASM_HASHES" envSeparator:","`

	// SeedPath is an optional file of escrow contract IDs (one per line)
	// loaded into the registry at first boot — bootstrap for escrows
	// created before the indexed range. The API that deploys escrows can
	// export it. Env: ESCROW_SEED_PATH.
	SeedPath string `env:"SEED_PATH"`
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

// StateConfig governs the on-disk state files (cursor + watchlist; the
// watchlist file lives next to Path with a .watchlist.json suffix).
type StateConfig struct {
	Path string `env:"PATH" envDefault:"./indexer.state.json"`
}

// HealthConfig governs the HTTP health/metrics server and the outbound
// heartbeat.
type HealthConfig struct {
	Enabled bool `env:"ENABLED" envDefault:"true"`
	Port    int  `env:"PORT" envDefault:"8080"`

	// HeartbeatURL, when set, receives a throttled GET after processed
	// ledgers (dead-man's switch: the external monitor alerts on
	// silence). Provider-agnostic — any service that accepts a ping URL
	// works (Better Stack, Healthchecks.io, UptimeRobot). Treat the URL
	// as a secret: whoever holds it can silence the alarm.
	// Env: HEALTH_HEARTBEAT_URL.
	HeartbeatURL string `env:"HEARTBEAT_URL"`
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
