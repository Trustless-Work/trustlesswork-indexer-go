// Package state persists the Indexer's durable progress: the cursor (last
// processed ledger) and the escrow set. It lets the Indexer resume after a
// restart instead of starting over from the network tip.
//
// The store is a single JSON record written atomically. A file backend is
// the right fit for this thin, single-node forwarder (no database of its
// own); the contract is the Store interface, so a DB-backed store could be
// dropped in later without touching callers.
package state

import (
	"context"
	"errors"
	"time"
)

// CurrentVersion is the schema version of the on-disk State. Bump on a
// backwards-incompatible change. Additive fields (like Gaps) do NOT bump
// it: older files simply load with the zero value.
const CurrentVersion = 1

// State is the persisted runtime state.
type State struct {
	// Version is the on-disk schema version (stamped on Save).
	Version int `json:"version"`
	// Network is the passphrase the state was built against. Compared at
	// boot against the configured network; a mismatch is fatal.
	Network string `json:"network"`
	// LastLedgerSeq is the most recent ledger whose facts were all
	// published successfully. Resume happens at LastLedgerSeq + 1.
	LastLedgerSeq uint32 `json:"last_ledger_seq"`
	// EscrowContracts is the persisted escrow set (the registry snapshot).
	// Populated and consumed once registry persistence is wired.
	EscrowContracts []string `json:"escrow_contracts"`
	// Gaps records every ledger range this instance knowingly skipped
	// (e.g. the cursor fell below the RPC retention window and was
	// clamped forward). It is the durable evidence a later backfill needs
	// to know WHAT to replay — without it, a clamp silently rewrites
	// history as "nothing happened here". Append-only; entries are
	// removed only by an operator after the range has been replayed.
	Gaps []Gap `json:"gaps,omitempty"`
}

// Gap is one contiguous ledger range that was skipped instead of
// processed. FromLedger/ToLedger are inclusive bounds.
type Gap struct {
	FromLedger uint32 `json:"from_ledger"`
	ToLedger   uint32 `json:"to_ledger"`
	// Reason is a short machine-readable cause, e.g. "rpc_retention".
	Reason     string    `json:"reason"`
	DetectedAt time.Time `json:"detected_at"`
}

// Store persists and restores State.
type Store interface {
	// Load returns the persisted State, or ErrStateNotFound if none exists.
	Load(ctx context.Context) (State, error)
	// Save writes st atomically.
	Save(ctx context.Context, st State) error
	// Close releases held resources (e.g. the single-writer lock).
	Close() error
}

var (
	// ErrStateNotFound is returned by Load when no state has been persisted.
	ErrStateNotFound = errors.New("state not found")
	// ErrStateLockHeld is returned by NewFileStore when another process
	// already holds the single-writer lock.
	ErrStateLockHeld = errors.New("state lock held by another process")
	// ErrStateCorrupted is returned by Load when the file cannot be parsed.
	ErrStateCorrupted = errors.New("state file corrupted")
)
