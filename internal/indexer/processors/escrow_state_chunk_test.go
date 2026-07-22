package processors

import "testing"

// The poison-ledger case: a ledger touching 51 escrows builds 51*4 = 204
// candidate keys, which exceeds the 200-key getLedgerEntries cap in a single
// request. Chunking must keep every request within the cap and lose no keys.
func TestChunkStrings_PoisonLedger(t *testing.T) {
	keys := make([]string, 51*4)
	for i := range keys {
		keys[i] = "k"
	}

	chunks := chunkStrings(keys, maxLedgerEntryKeysPerRequest)

	if len(chunks) != 2 {
		t.Fatalf("expected 2 chunks for 204 keys, got %d", len(chunks))
	}
	total := 0
	for _, c := range chunks {
		if len(c) > maxLedgerEntryKeysPerRequest {
			t.Fatalf("chunk size %d exceeds cap %d", len(c), maxLedgerEntryKeysPerRequest)
		}
		total += len(c)
	}
	if total != len(keys) {
		t.Fatalf("chunks lost keys: total %d, want %d", total, len(keys))
	}
}

func TestChunkStrings_EdgeCases(t *testing.T) {
	mk := func(n int) []string { return make([]string, n) }

	cases := []struct {
		name       string
		n, size    int
		wantChunks int
	}{
		{"exact multiple", 200, 200, 1},
		{"single element", 1, 200, 1},
		{"size zero -> one chunk", 10, 0, 1},
		{"just over -> two chunks", 201, 200, 2},
		{"401 -> three chunks", 401, 200, 3},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := chunkStrings(mk(tc.n), tc.size)
			if len(got) != tc.wantChunks {
				t.Fatalf("chunkStrings(%d, %d): got %d chunks, want %d",
					tc.n, tc.size, len(got), tc.wantChunks)
			}
		})
	}
}
