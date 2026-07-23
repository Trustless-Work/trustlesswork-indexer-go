package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
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

func TestFileStore_GapsRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	detected := time.Date(2026, 7, 22, 15, 4, 5, 0, time.UTC)
	want := State{
		Network:       "Test SDF Network",
		LastLedgerSeq: 100,
		Gaps: []Gap{
			{FromLedger: 10, ToLedger: 49, Reason: "rpc_retention", DetectedAt: detected},
		},
	}
	if err := s.Save(context.Background(), want); err != nil {
		t.Fatal(err)
	}

	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Gaps) != 1 {
		t.Fatalf("gaps = %+v, want exactly 1", got.Gaps)
	}
	g := got.Gaps[0]
	if g.FromLedger != 10 || g.ToLedger != 49 || g.Reason != "rpc_retention" || !g.DetectedAt.Equal(detected) {
		t.Fatalf("gap round-trip mismatch: %+v", g)
	}
}

// A state file written before the Gaps field existed must load cleanly —
// the field is additive and versionless by design.
func TestFileStore_PreGapsFileLoads(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	legacy := []byte(`{"version":1,"network":"Test SDF Network","last_ledger_seq":7,"escrow_contracts":["CESCROW"]}`)
	if err := os.WriteFile(path, legacy, 0o644); err != nil {
		t.Fatal(err)
	}

	s, err := NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	got, err := s.Load(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.LastLedgerSeq != 7 || got.Gaps != nil {
		t.Fatalf("legacy load mismatch: %+v", got)
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
