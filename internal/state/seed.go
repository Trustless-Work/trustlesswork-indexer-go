package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// LoadSeed reads an optional JSON file containing a list of escrow contract
// addresses and returns them as a string slice. It is used at first boot
// (when no state exists yet) to bootstrap the watchlist with already-known
// escrow addresses — typically exported from the Core's database to capture
// historical contracts that the Indexer would otherwise be blind to.
//
// Expected file format is a plain JSON array of strings:
//
//	[
//	  "CAQA...",
//	  "CBQB..."
//	]
//
// Semantics:
//   - path == "": no seed configured; returns (nil, nil).
//   - path set but file does not exist: not an error; returns (nil, nil).
//     Operators set WATCHLIST_SEED_PATH unconditionally in their config and
//     drop the file only when they want to seed; we don't punish that.
//   - path set, file exists, JSON parse fails: returns a wrapped error.
//   - path set, file exists, JSON parses: returns the slice (may be empty).
//
// The returned slice is not deduplicated; callers should deduplicate at
// load (NewWatchlist does this naturally).
func LoadSeed(path string) ([]string, error) {
	if path == "" {
		return nil, nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading watchlist seed %q: %w", path, err)
	}
	var ids []string
	if err := json.Unmarshal(data, &ids); err != nil {
		return nil, fmt.Errorf("parsing watchlist seed %q: %w", path, err)
	}
	return ids, nil
}
