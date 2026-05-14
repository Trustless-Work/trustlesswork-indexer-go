package state

import "context"

// Store persists Indexer State across restarts.
//
// Contract:
//   - Load is called once at boot. It returns ErrStateNotFound on a clean
//     filesystem and the parsed State on a warm one. Other errors are
//     unrecoverable from the loop's perspective (corruption, version
//     mismatch, lock contention).
//   - Save is called after every successful sink.Write to advance the
//     cursor and persist any new watchlist entries discovered in that
//     ledger. Save MUST be atomic: either the on-disk state reflects the
//     full new State, or it reflects the unchanged previous one. A
//     partial state file is never acceptable.
//   - Close releases any held resources (file locks, file handles).
//     Calling Close more than once is safe and a no-op.
//
// Implementations may add side effects (e.g. fsync, advisory locking) but
// must not change the semantic contract above. Callers should not assume
// implementation details — in particular, Save may be slow (10ms+ if
// fsync hits a spinning disk) and must always be called with the
// understanding that it is a synchronization point.
type Store interface {
	Load(ctx context.Context) (State, error)
	Save(ctx context.Context, s State) error
	Close() error
}
