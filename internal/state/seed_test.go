package state

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadSeed_EmptyPath_ReturnsNothing(t *testing.T) {
	ids, err := LoadSeed("")
	if err != nil {
		t.Fatalf("empty path must not error; got %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil; got %v", ids)
	}
}

func TestLoadSeed_MissingFile_ReturnsNothing(t *testing.T) {
	// path set but file does not exist: operationally common (env var
	// always present in config templates, file optional). Must not error.
	ids, err := LoadSeed(filepath.Join(t.TempDir(), "does-not-exist.json"))
	if err != nil {
		t.Fatalf("missing file must not error; got %v", err)
	}
	if ids != nil {
		t.Fatalf("expected nil; got %v", ids)
	}
}

func TestLoadSeed_HappyPath(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(path, []byte(`["CAQA","CBQB","CCQC"]`), 0o644); err != nil {
		t.Fatalf("priming seed file: %v", err)
	}

	ids, err := LoadSeed(path)
	if err != nil {
		t.Fatalf("LoadSeed: %v", err)
	}
	if len(ids) != 3 {
		t.Fatalf("expected 3 entries; got %d", len(ids))
	}
}

func TestLoadSeed_RejectsMalformedJSON(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(path, []byte(`not-json`), 0o644); err != nil {
		t.Fatalf("priming seed file: %v", err)
	}

	_, err := LoadSeed(path)
	if err == nil {
		t.Fatal("expected error for malformed JSON")
	}
	if !strings.Contains(err.Error(), "parsing") {
		t.Fatalf("error should mention parsing; got %q", err.Error())
	}
}

func TestLoadSeed_EmptyArray_ReturnsEmptySlice(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "seed.json")
	if err := os.WriteFile(path, []byte(`[]`), 0o644); err != nil {
		t.Fatalf("priming seed file: %v", err)
	}

	ids, err := LoadSeed(path)
	if err != nil {
		t.Fatalf("LoadSeed: %v", err)
	}
	if ids == nil {
		t.Fatal("expected non-nil empty slice for `[]`; got nil")
	}
	if len(ids) != 0 {
		t.Fatalf("expected length 0; got %d", len(ids))
	}
}
