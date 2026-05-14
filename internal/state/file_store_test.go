package state

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func newTempStore(t *testing.T) (*FileStore, string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "indexer.state.json")
	fs, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("NewFileStore: %v", err)
	}
	t.Cleanup(func() { _ = fs.Close() })
	return fs, path
}

func TestFileStore_LoadOnFreshFilesystem_ReturnsNotFound(t *testing.T) {
	fs, _ := newTempStore(t)
	_, err := fs.Load(context.Background())
	if !errors.Is(err, ErrStateNotFound) {
		t.Fatalf("expected ErrStateNotFound; got %v", err)
	}
}

func TestFileStore_RoundTrip(t *testing.T) {
	fs, _ := newTempStore(t)

	original := NewState("testnet", "v1").
		WithCursor(12345, "abc-0", time.Unix(1700000000, 0).UTC()).
		WithWatchlist([]string{"CAQA", "CBQB"})

	if err := fs.Save(context.Background(), original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	loaded, err := fs.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if loaded.LastLedgerSeq != original.LastLedgerSeq {
		t.Errorf("LastLedgerSeq: got %d, want %d", loaded.LastLedgerSeq, original.LastLedgerSeq)
	}
	if loaded.Network != original.Network {
		t.Errorf("Network: got %q, want %q", loaded.Network, original.Network)
	}
	if len(loaded.EscrowContracts) != len(original.EscrowContracts) {
		t.Fatalf("EscrowContracts length: got %d, want %d", len(loaded.EscrowContracts), len(original.EscrowContracts))
	}
	for i, id := range original.EscrowContracts {
		if loaded.EscrowContracts[i] != id {
			t.Errorf("EscrowContracts[%d]: got %q, want %q", i, loaded.EscrowContracts[i], id)
		}
	}
	if !loaded.LastPublishedAt.Equal(original.LastPublishedAt) {
		t.Errorf("LastPublishedAt: got %v, want %v", loaded.LastPublishedAt, original.LastPublishedAt)
	}
}

func TestFileStore_Save_StampsVersion(t *testing.T) {
	fs, path := newTempStore(t)

	// Construct a State with explicit version 0 to confirm Save rewrites it.
	st := NewState("testnet", "v1")
	st.Version = 0
	if err := fs.Save(context.Background(), st); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// Open a sibling store to load (closes/releases the lock automatically
	// at test cleanup).
	fs.Close()
	fs2, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("re-open: %v", err)
	}
	defer fs2.Close()
	loaded, err := fs2.Load(context.Background())
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.Version != CurrentVersion {
		t.Fatalf("expected Save to stamp Version=%d; got %d", CurrentVersion, loaded.Version)
	}
}

func TestFileStore_Load_DetectsCorruption(t *testing.T) {
	fs, path := newTempStore(t)

	if err := os.WriteFile(path, []byte("not json"), 0o644); err != nil {
		t.Fatalf("priming corrupt file: %v", err)
	}

	_, err := fs.Load(context.Background())
	if !errors.Is(err, ErrStateCorrupted) {
		t.Fatalf("expected ErrStateCorrupted; got %v", err)
	}
}

func TestFileStore_Load_DetectsFutureVersion(t *testing.T) {
	fs, path := newTempStore(t)

	// Write a state whose Version is higher than what this binary supports.
	future := `{"version": 9999, "network": "testnet", "schema_version": "v1", "last_ledger_seq": 0, "last_message_id": "", "last_published_at": "0001-01-01T00:00:00Z", "escrow_contracts": []}`
	if err := os.WriteFile(path, []byte(future), 0o644); err != nil {
		t.Fatalf("priming future-version file: %v", err)
	}

	_, err := fs.Load(context.Background())
	if !errors.Is(err, ErrStateVersionMismatch) {
		t.Fatalf("expected ErrStateVersionMismatch; got %v", err)
	}
}

func TestFileStore_LockHeld_RejectsSecondOpener(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "indexer.state.json")

	first, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("first NewFileStore: %v", err)
	}
	defer first.Close()

	_, err = NewFileStore(path)
	if !errors.Is(err, ErrStateLockHeld) {
		t.Fatalf("expected ErrStateLockHeld for second opener; got %v", err)
	}
}

func TestFileStore_Close_ReleasesLock(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "indexer.state.json")

	first, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("first NewFileStore: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}

	// After Close, the lock is released; a second instance must succeed.
	second, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("second NewFileStore after Close: %v", err)
	}
	if err := second.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

func TestFileStore_Close_Idempotent(t *testing.T) {
	fs, _ := newTempStore(t)
	if err := fs.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("second Close must be no-op; got %v", err)
	}
}

func TestFileStore_Save_AtomicWithRespectToPartialWrites(t *testing.T) {
	// We can't actually crash the process mid-write, but we can verify
	// that Save's algorithm does not leave a corrupt main file even when
	// the temp file already exists with garbage (simulating leftover
	// from a previous failed Save).
	fs, path := newTempStore(t)
	original := NewState("testnet", "v1").WithCursor(1, "x", time.Unix(1, 0))
	if err := fs.Save(context.Background(), original); err != nil {
		t.Fatalf("initial Save: %v", err)
	}

	// Drop garbage into the temp file path.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte("leftover garbage"), 0o644); err != nil {
		t.Fatalf("priming leftover temp: %v", err)
	}

	updated := original.WithCursor(2, "y", time.Unix(2, 0))
	if err := fs.Save(context.Background(), updated); err != nil {
		t.Fatalf("Save with leftover temp: %v", err)
	}

	loaded, err := fs.Load(context.Background())
	if err != nil {
		t.Fatalf("Load after Save: %v", err)
	}
	if loaded.LastLedgerSeq != 2 {
		t.Fatalf("main file did not reflect new state; got LastLedgerSeq=%d", loaded.LastLedgerSeq)
	}

	// The temp file must not exist after a successful Save.
	if _, err := os.Stat(tmp); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temp file must be cleaned up after Save; stat err=%v", err)
	}
}
