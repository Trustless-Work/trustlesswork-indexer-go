package ingest

import (
	"context"
	"fmt"
	"net/http"

	"github.com/Trustless-Work/Indexer/internal/config"
	"github.com/stellar/go-stellar-sdk/ingest/ledgerbackend"
	"github.com/stellar/go-stellar-sdk/support/log"
)

// LedgerBackendType represents the type of ledger backend to use.
type LedgerBackendType string

const (
	// LedgerBackendTypeRPC uses RPC to fetch ledgers (the only
	// implementation today).
	LedgerBackendTypeRPC LedgerBackendType = "rpc"
	// LedgerBackendTypeDatastore reads archived ledgers from cloud
	// object storage (S3/GCS). Stubbed; not implemented.
	LedgerBackendTypeDatastore LedgerBackendType = "datastore"
)

// NewLedgerBackend constructs a LedgerBackend matching cfg.Indexer.
// The choice is driven by INDEXER_LEDGER_BACKEND_TYPE; "rpc" is the
// only supported value today. "datastore" is reserved for a future
// S3/GCS-backed implementation.
func NewLedgerBackend(ctx context.Context, cfg *config.Config) (ledgerbackend.LedgerBackend, error) {
	switch LedgerBackendType(cfg.Indexer.LedgerBackendType) {
	case LedgerBackendTypeDatastore:
		return nil, fmt.Errorf("datastore backend not implemented yet")
	case LedgerBackendTypeRPC:
		return newRPCLedgerBackend(cfg), nil
	default:
		return nil, fmt.Errorf("unsupported ledger backend type %q", cfg.Indexer.LedgerBackendType)
	}
}

// newRPCLedgerBackend builds an RPC-backed LedgerBackend using
// cfg.RPC.URL and cfg.Indexer.GetLedgersLimit as the internal buffer
// hint.
func newRPCLedgerBackend(cfg *config.Config) ledgerbackend.LedgerBackend {
	backend := ledgerbackend.NewRPCLedgerBackend(ledgerbackend.RPCLedgerBackendOptions{
		RPCServerURL: cfg.RPC.URL,
		BufferSize:   uint32(cfg.Indexer.GetLedgersLimit),
		// Without an explicit client the SDK dials with no timeout and a
		// hung getLedgers/getHealth blocks GetLedger forever — the process
		// looks alive while serving nothing.
		HttpClient: &http.Client{Timeout: cfg.RPC.LedgerFetchTimeout},
	})
	log.Infof("Using RPCLedgerBackend (buffer=%d, fetch_timeout=%s) against %s",
		cfg.Indexer.GetLedgersLimit, cfg.RPC.LedgerFetchTimeout, cfg.RPC.URL)
	return backend
}
