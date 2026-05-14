package state

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// FileStore persists State as a JSON file on the local filesystem.
//
// Atomicity is achieved via the canonical POSIX temp-and-rename pattern:
//
//  1. Marshal the State to JSON.
//  2. Write the JSON to <path>.tmp.
//  3. fsync the temp file.
//  4. Close the temp file.
//  5. os.Rename(<path>.tmp, <path>). On POSIX this is atomic on the same
//     filesystem: an observer always sees either the old file or the new
//     file, never a half-written intermediate.
//  6. fsync the parent directory so the rename itself survives a crash.
//
// Steps 3 and 6 are the difference between "atomic under normal operation"
// and "atomic under power loss". Both are cheap (single fsync each).
//
// Single-writer guarantee is enforced via an advisory POSIX flock on a
// sidecar lock file (<path>.lock). The lock is acquired non-blocking at
// NewFileStore time; if another process holds it, construction fails with
// ErrStateLockHeld. The lock is released on Close.
//
// FileStore is NOT safe for concurrent Save calls from multiple goroutines
// in the same process. The Indexer's main loop calls Save sequentially per
// ledger, so this is fine; if a future use case needs concurrent Save, add
// a sync.Mutex around the body of Save.
type FileStore struct {
	path     string
	lockFile *os.File
}

// NewFileStore opens (or initialises) the state at path. It does not read
// the existing State — that happens on Load. NewFileStore only ensures the
// containing directory exists and acquires the single-writer lock.
//
// path is typically read from STATE_PATH and defaults to
// "./indexer.state.json" in dev. The directory is created with mode 0o755
// if missing.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("state path must not be empty")
	}

	// Ensure the directory exists. We do this up-front because a missing
	// directory at Save time is a particularly nasty failure mode (cursor
	// "advanced" but never persisted).
	dir := filepath.Dir(path)
	if dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating state directory %q: %w", dir, err)
		}
	}

	lockPath := path + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening state lock %q: %w", lockPath, err)
	}
	// Non-blocking exclusive lock. If another process is up, we fail
	// immediately rather than block forever.
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrStateLockHeld, lockPath)
		}
		return nil, fmt.Errorf("acquiring state lock %q: %w", lockPath, err)
	}

	return &FileStore{path: path, lockFile: lock}, nil
}

// Load reads and parses the on-disk State. It does NOT validate the State
// against runtime configuration (e.g. matching Network or being inside the
// RPC retention window) — those checks are the caller's responsibility,
// because they require config that lives outside this package.
//
// Returns:
//   - (state, nil) on success.
//   - (zero, ErrStateNotFound) when the file does not exist.
//   - (zero, ErrStateCorrupted) when the file exists but cannot be parsed.
//   - (zero, ErrStateVersionMismatch) when Version > CurrentVersion.
//   - (zero, wrapped-error) for any other IO failure.
func (s *FileStore) Load(_ context.Context) (State, error) {
	data, err := os.ReadFile(s.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return State{}, ErrStateNotFound
		}
		return State{}, fmt.Errorf("reading state file %q: %w", s.path, err)
	}

	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, fmt.Errorf("%w: %v", ErrStateCorrupted, err)
	}

	if st.Version > CurrentVersion {
		return State{}, fmt.Errorf("%w: file version %d, binary supports up to %d",
			ErrStateVersionMismatch, st.Version, CurrentVersion)
	}

	return st, nil
}

// Save serializes st and replaces the on-disk state file atomically.
// See the FileStore type comment for the exact algorithm.
func (s *FileStore) Save(_ context.Context, st State) error {
	// Always stamp the current version on the way out.
	st.Version = CurrentVersion

	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		// Best-effort cleanup of partial temp file. If this fails we
		// don't surface it — the next Save will overwrite the temp
		// anyway, and the original file is untouched.
		_ = os.Remove(tmp)
		return fmt.Errorf("writing temp state file %q: %w", tmp, err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming temp state file: %w", err)
	}

	// fsync the parent directory so the rename survives an OS crash.
	// Failure here is logged at the caller's discretion; the rename has
	// already succeeded so the state is at least durable to a clean
	// shutdown.
	if dir, err := os.Open(filepath.Dir(s.path)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}

	return nil
}

// Close releases the advisory lock and closes the lock file. Idempotent.
func (s *FileStore) Close() error {
	if s.lockFile == nil {
		return nil
	}
	// Errors from Flock/Close on shutdown are not actionable; we always
	// nil the handle so a second Close is a no-op.
	_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	err := s.lockFile.Close()
	s.lockFile = nil
	return err
}

// writeFileSync writes data to path, fsyncs the file, and closes it. It
// does NOT rename — callers must do that. Encapsulated here so the open /
// write / sync / close sequence can be tested in isolation and so that
// every Save path uses identical durability semantics.
func writeFileSync(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}
