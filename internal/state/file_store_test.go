package state

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
)

func TestFileStore_SaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	if _, err := s.Load(context.Background()); !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("expected ErrStateNotFound on first load, got %v", err)
	}

	want := State{Network: "Test SDF Network", LastLedgerSeq: 42, EscrowContracts: []string{"CESCROW"}}
	if err := s.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}

	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.Version != CurrentVersion {
		t.Errorf("version = %d, want %d", got.Version, CurrentVersion)
	}
	if got.LastLedgerSeq != 42 || got.Network != "Test SDF Network" || len(got.EscrowContracts) != 1 {
		t.Fatalf("round-trip mismatch: %+v", got)
	}
}

func TestFileStore_LockHeld(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s1, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s1.Close()

	if _, err := NewFileStore(path); !errors.Is(err, ErrStateLockHeld) {
		t.Fatalf("expected ErrStateLockHeld for a second opener, got %v", err)
	}
}
