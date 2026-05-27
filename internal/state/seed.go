package state

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

// LoadSeed reads escrow contract IDs from a seed file: one ID per line,
// with blank lines and lines starting with '#' ignored. It returns
// (nil, nil) when path is empty (seeding is optional).
//
// The seed bootstraps escrows created before the indexed range — the API
// that deploys escrows knows all their addresses and can export this file.
func LoadSeed(path string) ([]string, error) {
	if strings.TrimSpace(path) == "" {
		return nil, nil
	}

	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("escrow seed file %q not found: %w", path, err)
		}
		return nil, fmt.Errorf("opening escrow seed %q: %w", path, err)
	}
	defer f.Close()

	var ids []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		ids = append(ids, line)
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("reading escrow seed %q: %w", path, err)
	}
	return ids, nil
}
