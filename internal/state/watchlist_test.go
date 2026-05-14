package state

import (
	"sort"
	"sync"
	"testing"
)

func TestWatchlist_NewEmpty(t *testing.T) {
	w := NewWatchlist(nil)
	if w.Size() != 0 {
		t.Fatalf("expected size 0; got %d", w.Size())
	}
	if w.IsTracked("CAQA") {
		t.Fatal("empty watchlist must not track anything")
	}
}

func TestWatchlist_NewWithSeed(t *testing.T) {
	seed := []string{"CAQA", "CBQB", "CCQC"}
	w := NewWatchlist(seed)
	if w.Size() != 3 {
		t.Fatalf("expected size 3; got %d", w.Size())
	}
	for _, id := range seed {
		if !w.IsTracked(id) {
			t.Errorf("seed %q must be tracked", id)
		}
	}
}

func TestWatchlist_SeedDeduplicates(t *testing.T) {
	w := NewWatchlist([]string{"CAQA", "CAQA", "CAQA"})
	if w.Size() != 1 {
		t.Fatalf("duplicates must be coalesced; size=%d", w.Size())
	}
}

func TestWatchlist_IgnoresEmptyContractID(t *testing.T) {
	// Empty contract IDs would corrupt downstream lookups; they must
	// never enter the set.
	w := NewWatchlist([]string{"", "CAQA", ""})
	if w.Size() != 1 {
		t.Fatalf("empty IDs must not be stored; size=%d", w.Size())
	}
	if w.IsTracked("") {
		t.Fatal("empty string must never be reported as tracked")
	}
	if w.Add("") != false {
		t.Fatal("Add(\"\") must return false")
	}
	if w.Size() != 1 {
		t.Fatalf("size must not change after Add(\"\"); got %d", w.Size())
	}
}

func TestWatchlist_Add_NewVsExisting(t *testing.T) {
	w := NewWatchlist(nil)
	if !w.Add("CAQA") {
		t.Fatal("Add of new entry must return true")
	}
	if w.Add("CAQA") {
		t.Fatal("Add of existing entry must return false")
	}
	if w.Size() != 1 {
		t.Fatalf("size must be 1 after duplicate Add; got %d", w.Size())
	}
}

func TestWatchlist_SnapshotIsSorted(t *testing.T) {
	w := NewWatchlist([]string{"CCQC", "CAQA", "CBQB"})
	snap := w.Snapshot()
	if !sort.StringsAreSorted(snap) {
		t.Fatalf("snapshot must be sorted; got %v", snap)
	}
}

func TestWatchlist_SnapshotIsCopy(t *testing.T) {
	// Mutating the snapshot must not affect the underlying set, otherwise
	// callers could accidentally corrupt watchlist state.
	w := NewWatchlist([]string{"CAQA"})
	snap := w.Snapshot()
	snap[0] = "MUTATED"
	if !w.IsTracked("CAQA") {
		t.Fatal("snapshot mutation must not affect the watchlist")
	}
}

func TestWatchlist_ConcurrentReadsAndWrites(t *testing.T) {
	// Run a stress test: many goroutines doing IsTracked while one does
	// Add. Failure mode is a data race, which the race detector catches
	// when tests are run with -race.
	w := NewWatchlist(nil)
	var wg sync.WaitGroup
	const readers = 8
	const adds = 100

	wg.Add(readers)
	stop := make(chan struct{})
	for i := 0; i < readers; i++ {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = w.IsTracked("CAQA")
					_ = w.Size()
				}
			}
		}()
	}

	for i := 0; i < adds; i++ {
		w.Add("CAQA")
	}
	close(stop)
	wg.Wait()

	if w.Size() != 1 {
		t.Fatalf("expected size 1 after many duplicate Adds; got %d", w.Size())
	}
}
