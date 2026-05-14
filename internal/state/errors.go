// Package state persists the Indexer's runtime state across restarts. It
// holds two pieces in one atomic unit: the ledger cursor (last successfully
// processed ledger) and the watchlist (set of escrow contract addresses
// discovered through tw_init events).
//
// Atomic co-persistence is the central invariant: after Save returns nil,
// the on-disk state guarantees "cursor advanced to ledger N ⇒ watchlist
// contains every escrow discovered up to and including N". This is what
// makes the filter-and-forward pipeline crash-safe: there is no window in
// which the cursor has moved but the watchlist update is missing.
//
// The current implementation is file-backed (FileStore). The Store interface
// is deliberately small so Redis or Postgres backends can be added later
// without touching callers.
package state

import "errors"

// Sentinel errors emitted by the state package. The main loop uses
// errors.Is on these to decide whether to fall back to bootstrap or fail
// fast. None of these are transient.
var (
	// ErrStateNotFound indicates the state file does not exist yet. The
	// expected first-boot signal. Callers handle this by constructing a
	// fresh State (optionally seeded from WATCHLIST_SEED_PATH) and
	// arranging start_ledger explicitly.
	ErrStateNotFound = errors.New("state file not found")

	// ErrStateCorrupted indicates the state file exists but cannot be
	// parsed as JSON or its top-level structure is invalid. Operator
	// must intervene — delete the file and provide an explicit
	// start_ledger if a re-bootstrap is acceptable.
	ErrStateCorrupted = errors.New("state file corrupted")

	// ErrStateVersionMismatch indicates the on-disk Version is newer than
	// CurrentVersion. The binary is older than the cursor it was handed.
	// Refuse to continue — opening it would risk silent data loss.
	ErrStateVersionMismatch = errors.New("state file schema version unsupported")

	// ErrStateNetworkMismatch indicates the persisted Network does not
	// match the configured network. Typically: operator pointed the
	// binary at the wrong RPC, or accidentally reused a testnet cursor
	// for mainnet. Refuse to continue.
	ErrStateNetworkMismatch = errors.New("state file network mismatch")

	// ErrStateLockHeld indicates another process already holds the
	// advisory lock on the state file. Two Indexer instances sharing the
	// same state would corrupt it silently; fail-fast is the only safe
	// choice. Operator must kill the other instance.
	ErrStateLockHeld = errors.New("state file lock held by another process")
)
