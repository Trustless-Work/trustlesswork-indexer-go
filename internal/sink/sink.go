package sink

import (
	"context"

	"github.com/Trustless-Work/Indexer/internal/entities"
	"github.com/Trustless-Work/Indexer/internal/indexer/types"
)

// LedgerBuffer is the read-only view of a processed ledger that sinks receive.
// It exposes only the getter methods — sinks never need to push or merge data.
// IndexerBufferInterface satisfies this interface automatically via Go structural typing.
type LedgerBuffer interface {
	GetTransactions() []types.Transaction
	GetOperations() []types.Operation
	GetStateChanges() []types.StateChange
	GetTrustlineChanges() []types.TrustlineChange
	GetContractChanges() []types.ContractChange
	GetEscrows() []entities.Escrow
	GetAllParticipants() []string
}

// Sink is the output abstraction. Every implementation must satisfy this interface.
// Backend-specific semantics (RabbitMQ publisher confirms, Postgres transactions,
// Mongo write concern) are implementation details — the caller only sees error/nil.
type Sink interface {
	// Write receives the fully processed buffer for one ledger.
	Write(ctx context.Context, buffer LedgerBuffer, ledgerSeq uint32) error

	// Close releases resources (connections, channels, goroutine pools, etc.)
	// Write must not be called after Close.
	Close() error
}

// HealthChecker is optional. Implement if the sink supports health/readiness probes.
type HealthChecker interface {
	Ping(ctx context.Context) error
}

// Flusher is optional. For sinks that accumulate an internal buffer and need an
// explicit flush (e.g. a time-windowed batch publisher).
type Flusher interface {
	Flush(ctx context.Context) error
}