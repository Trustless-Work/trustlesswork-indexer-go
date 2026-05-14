package ingest

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Environment variables read by LoadConfigFromEnv. All have defaults so the
// indexer can be started with no configuration in development.
const (
	EnvRPCURL            = "RPC_URL"
	EnvNetworkPassphrase = "NETWORK_PASSPHRASE"
	EnvStartLedger       = "START_LEDGER"
	EnvEndLedger         = "END_LEDGER"
	EnvGetLedgersLimit   = "GET_LEDGERS_LIMIT"
	EnvLedgerBackendType = "LEDGER_BACKEND_TYPE"
)

// Defaults preserve the previously hardcoded values so existing deployments
// keep working when no environment variables are set.
const (
	defaultRPCURL            = "https://soroban-testnet.stellar.org"
	defaultNetworkPassphrase = "Test SDF Network ; September 2015"
	defaultGetLedgersLimit   = 100
)

// LoadConfigFromEnv builds a Config from environment variables, falling back to
// previously hardcoded values when a variable is unset. A StartLedger value of 0
// is interpreted by the caller as "start from the latest ledger reported by
// the RPC health endpoint". EndLedger of 0 means unbounded (live mode).
func LoadConfigFromEnv() (Config, error) {
	cfg := Config{
		RPCURL:            getenvDefault(EnvRPCURL, defaultRPCURL),
		NetworkPassphrase: getenvDefault(EnvNetworkPassphrase, defaultNetworkPassphrase),
		LedgerBackendType: LedgerBackendTypeRPC,
		GetLedgersLimit:   defaultGetLedgersLimit,
	}

	if v := strings.TrimSpace(os.Getenv(EnvLedgerBackendType)); v != "" {
		cfg.LedgerBackendType = LedgerBackendType(strings.ToLower(v))
	}

	if v := strings.TrimSpace(os.Getenv(EnvGetLedgersLimit)); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			return Config{}, fmt.Errorf("parsing %s=%q as int: %w", EnvGetLedgersLimit, v, err)
		}
		if parsed <= 0 {
			return Config{}, fmt.Errorf("%s must be positive, got %d", EnvGetLedgersLimit, parsed)
		}
		cfg.GetLedgersLimit = parsed
	}

	if v := strings.TrimSpace(os.Getenv(EnvStartLedger)); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return Config{}, fmt.Errorf("parsing %s=%q as uint32: %w", EnvStartLedger, v, err)
		}
		cfg.StartLedger = uint32(parsed)
	}

	if v := strings.TrimSpace(os.Getenv(EnvEndLedger)); v != "" {
		parsed, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return Config{}, fmt.Errorf("parsing %s=%q as uint32: %w", EnvEndLedger, v, err)
		}
		cfg.EndLedger = uint32(parsed)
	}

	if cfg.EndLedger != 0 && cfg.StartLedger > cfg.EndLedger {
		return Config{}, fmt.Errorf("%s (%d) must be <= %s (%d)", EnvStartLedger, cfg.StartLedger, EnvEndLedger, cfg.EndLedger)
	}

	return cfg, nil
}

// getenvDefault returns the trimmed value of the environment variable name, or
// def when the variable is unset or empty.
func getenvDefault(name, def string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return def
}
