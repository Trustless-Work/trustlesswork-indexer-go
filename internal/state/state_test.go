package state

import (
	"testing"
	"time"
)

func TestNewState_HasCurrentVersion(t *testing.T) {
	s := NewState("testnet", "v1")
	if s.Version != CurrentVersion {
		t.Fatalf("expected Version=%d; got %d", CurrentVersion, s.Version)
	}
	if s.Network != "testnet" {
		t.Fatalf("expected Network testnet; got %q", s.Network)
	}
	if s.SchemaVersion != "v1" {
		t.Fatalf("expected SchemaVersion v1; got %q", s.SchemaVersion)
	}
	if len(s.EscrowContracts) != 0 {
		t.Fatalf("expected empty escrow list; got %v", s.EscrowContracts)
	}
}

func TestWithCursor_DoesNotMutateReceiver(t *testing.T) {
	original := NewState("testnet", "v1")
	now := time.Unix(1700000000, 0)
	updated := original.WithCursor(42, "abc-0", now)

	if original.LastLedgerSeq != 0 {
		t.Fatalf("WithCursor must not mutate receiver; original.LastLedgerSeq=%d", original.LastLedgerSeq)
	}
	if updated.LastLedgerSeq != 42 {
		t.Fatalf("expected updated.LastLedgerSeq=42; got %d", updated.LastLedgerSeq)
	}
	if updated.LastMessageID != "abc-0" {
		t.Fatalf("expected updated.LastMessageID=abc-0; got %q", updated.LastMessageID)
	}
	if !updated.LastPublishedAt.Equal(now) {
		t.Fatalf("expected updated.LastPublishedAt=%v; got %v", now, updated.LastPublishedAt)
	}
}

func TestWithCursor_PreservesWatchlist(t *testing.T) {
	original := NewState("testnet", "v1").WithWatchlist([]string{"CAQA", "CBQB"})
	updated := original.WithCursor(42, "abc-0", time.Unix(1, 0))
	if len(updated.EscrowContracts) != 2 {
		t.Fatalf("WithCursor must preserve watchlist; got %v", updated.EscrowContracts)
	}
}

func TestWithWatchlist_DefensiveCopy(t *testing.T) {
	// Mutating the input slice after WithWatchlist must not affect the
	// stored state, otherwise callers can accidentally corrupt persisted
	// data.
	wl := []string{"CAQA", "CBQB"}
	s := NewState("testnet", "v1").WithWatchlist(wl)
	wl[0] = "MUTATED"
	if s.EscrowContracts[0] == "MUTATED" {
		t.Fatal("WithWatchlist must defensive-copy its input")
	}
}
