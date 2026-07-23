package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// Supported enum values, centralized so Validate stays declarative.
var (
	validLedgerBackendTypes = map[string]struct{}{
		"rpc":       {},
		"datastore": {},
	}
	validSinkTypes = map[string]struct{}{
		"noop":     {},
		"rabbitmq": {},
	}
	validLogFormats = map[string]struct{}{
		"auto": {},
		"json": {},
		"text": {},
	}
)

// Validate enforces invariants that env-tag parsing alone cannot express:
//
//   - Cross-field consistency (e.g. SINK_TYPE=rabbitmq requires
//     RABBITMQ_URL).
//   - Numeric ranges (port within 1..65535, GetLedgersLimit positive,
//     Workers non-negative, ledger range ordered).
//   - Enum membership (LedgerBackendType, SinkType, LogFormat).
//   - Network sanity hints (warn-style heuristic for mainnet/testnet URL
//     mismatch — surfaced as a hard error here to force the operator to
//     confirm, since misconfiguration produces silent data contamination).
//
// All violations are collected and joined with errors.Join so the
// operator sees every problem at once rather than fixing one and
// triggering the next.
func (c *Config) Validate() error {
	var errs []error

	// --- Network ---
	if c.Network.Name == "" {
		errs = append(errs, fmt.Errorf("NETWORK_NAME must not be empty"))
	}
	if c.Network.Passphrase == "" {
		errs = append(errs, fmt.Errorf("NETWORK_PASSPHRASE must not be empty"))
	}

	// Catch the classic mismatch where someone points a mainnet binary
	// at a testnet RPC (or vice-versa). The URL hostname is the cheapest
	// disambiguator we have.
	rpcURLLower := strings.ToLower(c.RPC.URL)
	if c.Network.Name == "mainnet" && strings.Contains(rpcURLLower, "testnet") {
		errs = append(errs, fmt.Errorf("NETWORK_NAME=mainnet but RPC_URL appears to point to testnet (%q)", c.RPC.URL))
	}
	if c.Network.Name == "testnet" && strings.Contains(rpcURLLower, "mainnet") {
		errs = append(errs, fmt.Errorf("NETWORK_NAME=testnet but RPC_URL appears to point to mainnet (%q)", c.RPC.URL))
	}

	// --- RPC timeouts ---
	if c.RPC.RequestTimeout <= 0 {
		errs = append(errs, fmt.Errorf("RPC_REQUEST_TIMEOUT must be positive (got %s)", c.RPC.RequestTimeout))
	}
	if c.RPC.LedgerFetchTimeout <= 0 {
		errs = append(errs, fmt.Errorf("RPC_LEDGER_FETCH_TIMEOUT must be positive (got %s)", c.RPC.LedgerFetchTimeout))
	}

	// --- Indexer ranges ---
	// 200 is the hard server-side cap of getLedgers on stellar-rpc; a
	// larger value fails at runtime with a provider error instead of at
	// boot. On mainnet keep this LOW (~10): each ledger weighs ~2.65MB,
	// so limit=100 would request ~265MB in a single response.
	if c.Indexer.GetLedgersLimit <= 0 || c.Indexer.GetLedgersLimit > 200 {
		errs = append(errs, fmt.Errorf("INDEXER_GET_LEDGERS_LIMIT must be in [1, 200] (stellar-rpc getLedgers cap; got %d)", c.Indexer.GetLedgersLimit))
	}
	if c.Indexer.Workers < 0 {
		errs = append(errs, fmt.Errorf("INDEXER_WORKERS must be >= 0 (0 means auto; got %d)", c.Indexer.Workers))
	}
	if c.Indexer.EndLedger != 0 && c.Indexer.StartLedger > c.Indexer.EndLedger {
		errs = append(errs, fmt.Errorf("INDEXER_START_LEDGER (%d) must be <= INDEXER_END_LEDGER (%d)", c.Indexer.StartLedger, c.Indexer.EndLedger))
	}
	if _, ok := validLedgerBackendTypes[c.Indexer.LedgerBackendType]; !ok {
		errs = append(errs, fmt.Errorf("INDEXER_LEDGER_BACKEND_TYPE must be one of [rpc datastore]; got %q", c.Indexer.LedgerBackendType))
	}

	// --- Escrow identity ---
	for _, h := range c.Escrow.ApprovedWasmHashes {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		if b, err := hex.DecodeString(h); err != nil || len(b) != 32 {
			errs = append(errs, fmt.Errorf("ESCROW_APPROVED_WASM_HASHES entries must be 32-byte hex strings; got %q", h))
		}
	}

	// --- Sink + cross-deps ---
	if _, ok := validSinkTypes[c.Sink.Type]; !ok {
		errs = append(errs, fmt.Errorf("SINK_TYPE must be one of [noop rabbitmq]; got %q", c.Sink.Type))
	}
	if c.Sink.Type == "rabbitmq" && strings.TrimSpace(c.RabbitMQ.URL) == "" {
		errs = append(errs, fmt.Errorf("RABBITMQ_URL is required when SINK_TYPE=rabbitmq"))
	}

	// --- State ---
	if strings.TrimSpace(c.State.Path) == "" {
		errs = append(errs, fmt.Errorf("STATE_PATH must not be empty"))
	}

	// --- Health ---
	if c.Health.Enabled && (c.Health.Port < 1 || c.Health.Port > 65535) {
		errs = append(errs, fmt.Errorf("HEALTH_PORT must be in [1, 65535] when HEALTH_ENABLED=true; got %d", c.Health.Port))
	}

	// --- Logging ---
	if _, ok := validLogFormats[c.Logging.Format]; !ok {
		errs = append(errs, fmt.Errorf("LOG_FORMAT must be one of [auto json text]; got %q", c.Logging.Format))
	}

	if len(errs) > 0 {
		return errors.Join(errs...)
	}
	return nil
}
