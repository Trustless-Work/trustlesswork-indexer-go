package config

import (
	"strings"
	"testing"
)

// withEnv sets env vars for the duration of the test and restores them.
// It uses t.Setenv which already handles restoration; this helper just
// provides a compact bulk form.
func withEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

// minimalEnv returns the smallest set of env vars that produces a valid
// Config. Most tests start from this and override one field at a time.
func minimalEnv() map[string]string {
	return map[string]string{
		"RPC_URL": "https://soroban-testnet.stellar.org",
	}
}

func TestLoad_MinimalEnv_SucceedsWithDefaults(t *testing.T) {
	withEnv(t, minimalEnv())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	// Spot-check the defaults are present.
	if cfg.Network.Name != "testnet" {
		t.Errorf("expected Network.Name=testnet; got %q", cfg.Network.Name)
	}
	if cfg.Indexer.GetLedgersLimit != 100 {
		t.Errorf("expected GetLedgersLimit=100; got %d", cfg.Indexer.GetLedgersLimit)
	}
	if cfg.Sink.Type != "noop" {
		t.Errorf("expected Sink.Type=noop; got %q", cfg.Sink.Type)
	}
	if cfg.State.Path != "./indexer.state.json" {
		t.Errorf("expected State.Path=./indexer.state.json; got %q", cfg.State.Path)
	}
	if !cfg.Health.Enabled {
		t.Error("expected Health.Enabled=true by default")
	}
	if cfg.Health.Port != 8080 {
		t.Errorf("expected Health.Port=8080; got %d", cfg.Health.Port)
	}
	if !cfg.StrictMode {
		t.Error("expected StrictMode=true by default")
	}
}

func TestLoad_FailsWithoutRPCURL(t *testing.T) {
	// caarlos0/env returns its own error for missing required; we
	// surface it through Load's wrapper.
	_, err := Load()
	if err == nil {
		t.Fatal("expected error when RPC_URL is missing")
	}
	if !strings.Contains(err.Error(), "RPC_URL") && !strings.Contains(err.Error(), "URL") {
		t.Fatalf("error should mention the missing RPC_URL; got %v", err)
	}
}

func TestLoad_RabbitMQ_RequiresURL(t *testing.T) {
	env := minimalEnv()
	env["SINK_TYPE"] = "rabbitmq"
	// RABBITMQ_URL deliberately omitted
	withEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when SINK_TYPE=rabbitmq but RABBITMQ_URL is empty")
	}
	if !strings.Contains(err.Error(), "RABBITMQ_URL") {
		t.Fatalf("error should mention RABBITMQ_URL; got %v", err)
	}
}

func TestLoad_RabbitMQ_HappyPath(t *testing.T) {
	env := minimalEnv()
	env["SINK_TYPE"] = "rabbitmq"
	env["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"
	env["RABBITMQ_PUBLISHER_CONFIRMS"] = "false"
	withEnv(t, env)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RabbitMQ.URL != "amqp://guest:guest@localhost:5672/" {
		t.Errorf("RabbitMQ.URL parsed incorrectly: %q", cfg.RabbitMQ.URL)
	}
	if cfg.RabbitMQ.PublisherConfirms {
		t.Error("PublisherConfirms must respect explicit env override (false)")
	}
}

func TestLoad_RejectsInvalidLedgerRange(t *testing.T) {
	env := minimalEnv()
	env["INDEXER_START_LEDGER"] = "100"
	env["INDEXER_END_LEDGER"] = "50"
	withEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatal("expected error when START_LEDGER > END_LEDGER")
	}
	if !strings.Contains(err.Error(), "START_LEDGER") {
		t.Fatalf("error should mention START_LEDGER; got %v", err)
	}
}

func TestLoad_RejectsBadEnums(t *testing.T) {
	cases := map[string]map[string]string{
		"sink type": {"SINK_TYPE": "kafka"},
		"backend":   {"INDEXER_LEDGER_BACKEND_TYPE": "nonsense"},
		"format":    {"LOG_FORMAT": "yaml"},
	}
	for name, overrides := range cases {
		t.Run(name, func(t *testing.T) {
			env := minimalEnv()
			for k, v := range overrides {
				env[k] = v
			}
			withEnv(t, env)
			_, err := Load()
			if err == nil {
				t.Fatalf("expected validation error for %s", name)
			}
		})
	}
}

func TestLoad_DetectsNetworkURLMismatch_MainnetSaysTestnet(t *testing.T) {
	env := minimalEnv()
	env["NETWORK_NAME"] = "mainnet"
	env["NETWORK_PASSPHRASE"] = "Public Global Stellar Network ; September 2015"
	env["RPC_URL"] = "https://soroban-testnet.stellar.org" // wrong
	withEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatal("expected error for mainnet config pointing at testnet URL")
	}
	if !strings.Contains(err.Error(), "mainnet") || !strings.Contains(err.Error(), "testnet") {
		t.Fatalf("error should call out the mainnet/testnet mismatch; got %v", err)
	}
}

func TestLoad_RejectsZeroGetLedgersLimit(t *testing.T) {
	env := minimalEnv()
	env["INDEXER_GET_LEDGERS_LIMIT"] = "0"
	withEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatal("expected validation error for zero GET_LEDGERS_LIMIT")
	}
}

func TestLoad_AggregatesMultipleErrors(t *testing.T) {
	env := minimalEnv()
	env["INDEXER_GET_LEDGERS_LIMIT"] = "0"
	env["SINK_TYPE"] = "kafka"
	env["LOG_FORMAT"] = "yaml"
	withEnv(t, env)

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	mentions := 0
	for _, needle := range []string{"GET_LEDGERS_LIMIT", "SINK_TYPE", "LOG_FORMAT"} {
		if strings.Contains(msg, needle) {
			mentions++
		}
	}
	if mentions < 2 {
		t.Fatalf("expected aggregated errors to mention at least two of the three violations; got %v", err)
	}
}
