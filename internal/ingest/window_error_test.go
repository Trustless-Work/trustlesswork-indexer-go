package ingest

import (
	"errors"
	"testing"
)

// The message fixtures mirror real stellar-rpc responses observed live:
// the beyond-tip case was captured verbatim from mainnet.sorobanrpc.com
// on 2026-07-23; the below-retention case matches the 2026-07-22 testnet
// incident (start ledger below the oldest retained).
func TestClassifyWindowError(t *testing.T) {
	tests := []struct {
		name       string
		err        error
		requested  uint32
		wantClass  windowErrorClass
		wantOldest uint32
		wantLatest uint32
	}{
		{
			name: "beyond tip (observed live on mainnet provider)",
			err: errors.New(
				"[-32600] start ledger (63613791) must be between the oldest ledger: 2 and the latest ledger: 63613790 for this rpc instance"),
			requested:  63613791,
			wantClass:  windowBeyondTip,
			wantOldest: 2,
			wantLatest: 63613790,
		},
		{
			name: "below retention (2026-07-22 incident shape)",
			err: errors.New(
				"[-32600] start ledger must be between the oldest ledger: 3627887 and the latest ledger: 3748846"),
			requested:  3217500,
			wantClass:  windowBelowRetention,
			wantOldest: 3627887,
			wantLatest: 3748846,
		},
		{
			name: "inside the window is not a window error",
			err: errors.New(
				"start ledger (100) must be between the oldest ledger: 50 and the latest ledger: 200"),
			requested: 100,
			wantClass: windowNone,
		},
		{
			name:      "unrelated transport error",
			err:       errors.New("Post \"https://rpc.example\": connection reset by peer"),
			requested: 42,
			wantClass: windowNone,
		},
		{
			name: "window numbers too large for uint32 fall back to transient",
			err: errors.New(
				"start ledger must be between the oldest ledger: 99999999999 and the latest ledger: 999999999999"),
			requested: 42,
			wantClass: windowNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			class, oldest, latest := classifyWindowError(tt.err, tt.requested)
			if class != tt.wantClass {
				t.Fatalf("class = %d, want %d", class, tt.wantClass)
			}
			if tt.wantClass == windowNone {
				return
			}
			if oldest != tt.wantOldest || latest != tt.wantLatest {
				t.Fatalf("window = [%d, %d], want [%d, %d]", oldest, latest, tt.wantOldest, tt.wantLatest)
			}
		})
	}
}
