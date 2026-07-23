package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoad_LedgerFetchTimeout_DefaultsTo120s(t *testing.T) {
	withEnv(t, minimalEnv())
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.RPC.LedgerFetchTimeout != 120*time.Second {
		t.Errorf("expected RPC.LedgerFetchTimeout=120s; got %s", cfg.RPC.LedgerFetchTimeout)
	}
}

func TestLoad_LedgerFetchTimeout_NonPositive_Fails(t *testing.T) {
	withEnv(t, minimalEnv())
	t.Setenv("RPC_LEDGER_FETCH_TIMEOUT", "0s")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RPC_LEDGER_FETCH_TIMEOUT") {
		t.Fatalf("expected RPC_LEDGER_FETCH_TIMEOUT validation error; got %v", err)
	}
}

func TestLoad_RequestTimeout_NonPositive_Fails(t *testing.T) {
	withEnv(t, minimalEnv())
	t.Setenv("RPC_REQUEST_TIMEOUT", "-1s")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "RPC_REQUEST_TIMEOUT") {
		t.Fatalf("expected RPC_REQUEST_TIMEOUT validation error; got %v", err)
	}
}

func TestLoad_GetLedgersLimit_OverServerCap_Fails(t *testing.T) {
	withEnv(t, minimalEnv())
	// 200 is the hard server-side cap of getLedgers; 201 used to pass boot
	// validation and then fail deterministically at runtime against the RPC.
	t.Setenv("INDEXER_GET_LEDGERS_LIMIT", "201")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "INDEXER_GET_LEDGERS_LIMIT") {
		t.Fatalf("expected INDEXER_GET_LEDGERS_LIMIT validation error; got %v", err)
	}
}

func TestLoad_GetLedgersLimit_AtServerCap_Succeeds(t *testing.T) {
	withEnv(t, minimalEnv())
	t.Setenv("INDEXER_GET_LEDGERS_LIMIT", "200")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Indexer.GetLedgersLimit != 200 {
		t.Errorf("expected GetLedgersLimit=200; got %d", cfg.Indexer.GetLedgersLimit)
	}
}
