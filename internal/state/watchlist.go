package state

import (
	"sort"
	"sync"
)

// Watchlist is a thread-safe in-memory set of escrow contract addresses
// known to the Indexer. It is the runtime form of State.EscrowContracts:
// loaded once at boot from disk or seed, mutated as new escrows are
// discovered, and snapshot back to disk via Snapshot during state saves.
//
// Concurrency model: the Indexer's per-ledger processing has two phases —
// a sequential pass that may call Add when a tw_init event is detected,
// and a parallel pass that calls IsTracked many times to filter SAC
// transfer events. Both paths must be safe. An RWMutex over a map is the
// cheapest correct choice; lookups are O(1) and contention is negligible
// because Adds are rare (once per new escrow) while reads are hot.
type Watchlist struct {
	mu  sync.RWMutex
	set map[string]struct{}
}

// NewWatchlist constructs a Watchlist pre-populated with the given seed
// contract addresses. Duplicates in the seed are coalesced. Passing nil or
// an empty slice yields an empty watchlist.
func NewWatchlist(seed []string) *Watchlist {
	w := &Watchlist{set: make(map[string]struct{}, len(seed))}
	for _, id := range seed {
		if id != "" {
			w.set[id] = struct{}{}
		}
	}
	return w
}

// IsTracked reports whether contractID is in the watchlist. Safe for
// concurrent calls. Returns false for the empty string by construction
// (empty contract IDs are never stored).
func (w *Watchlist) IsTracked(contractID string) bool {
	if contractID == "" {
		return false
	}
	w.mu.RLock()
	defer w.mu.RUnlock()
	_, ok := w.set[contractID]
	return ok
}

// Add records contractID in the watchlist. Returns true if the entry was
// newly added, false if it was already present. The empty string is
// silently ignored (returns false).
//
// The boolean return lets callers cheaply detect "fresh discovery" without
// a separate IsTracked call, e.g. for incrementing a "new escrows" metric.
func (w *Watchlist) Add(contractID string) bool {
	if contractID == "" {
		return false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if _, ok := w.set[contractID]; ok {
		return false
	}
	w.set[contractID] = struct{}{}
	return true
}

// Snapshot returns a sorted copy of the watchlist for persistence. Sorting
// is part of the on-disk contract: it produces stable diffs across saves,
// which is useful for ops review and reduces noise if state files are
// committed to a backup.
func (w *Watchlist) Snapshot() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	out := make([]string, 0, len(w.set))
	for id := range w.set {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

// Size returns the number of distinct escrow contracts in the watchlist.
// Constant-time; intended for the indexer_watchlist_size metric.
func (w *Watchlist) Size() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.set)
}
