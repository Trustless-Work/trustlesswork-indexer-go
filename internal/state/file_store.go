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

// FileStore persists State as a JSON file, written atomically via the
// canonical temp + fsync + rename + parent-dir fsync sequence so an
// observer always sees the old or new file, never a half-written one —
// even across a power loss. A non-blocking advisory flock on a sidecar
// lock file enforces a single writer.
type FileStore struct {
	path     string
	lockFile *os.File
}

// NewFileStore opens (and locks) the state at path. It creates the parent
// directory if missing and acquires the single-writer lock; if another
// process holds it, construction fails with ErrStateLockHeld.
func NewFileStore(path string) (*FileStore, error) {
	if path == "" {
		return nil, fmt.Errorf("state path must not be empty")
	}

	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("creating state directory %q: %w", dir, err)
		}
	}

	lockPath := path + ".lock"
	lock, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("opening state lock %q: %w", lockPath, err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("%w: %s", ErrStateLockHeld, lockPath)
		}
		return nil, fmt.Errorf("acquiring state lock %q: %w", lockPath, err)
	}

	return &FileStore{path: path, lockFile: lock}, nil
}

// Load reads and parses the on-disk State. Returns ErrStateNotFound when
// the file does not exist and ErrStateCorrupted when it cannot be parsed.
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
	return st, nil
}

// Save serializes st and atomically replaces the state file.
func (s *FileStore) Save(_ context.Context, st State) error {
	st.Version = CurrentVersion

	data, err := json.MarshalIndent(&st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state: %w", err)
	}

	tmp := s.path + ".tmp"
	if err := writeFileSync(tmp, data); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("writing temp state file %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("renaming temp state file: %w", err)
	}

	// fsync the parent directory so the rename survives an OS crash.
	if dir, derr := os.Open(filepath.Dir(s.path)); derr == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

// Close releases the advisory lock. Idempotent.
func (s *FileStore) Close() error {
	if s.lockFile == nil {
		return nil
	}
	_ = syscall.Flock(int(s.lockFile.Fd()), syscall.LOCK_UN)
	err := s.lockFile.Close()
	s.lockFile = nil
	return err
}

// writeFileSync writes data to path, fsyncs and closes it (no rename).
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
