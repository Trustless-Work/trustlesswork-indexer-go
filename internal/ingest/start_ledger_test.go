package ingest

import (
	"testing"
	"time"
)

func TestClampStartLedger(t *testing.T) {
	now := time.Date(2026, 7, 23, 12, 0, 0, 0, time.UTC)
	const oldest, latest = 1000, 2000

	tests := []struct {
		name      string
		start     uint32
		wantStart uint32
		wantGap   bool
		wantErr   bool
	}{
		{name: "inside the window is untouched", start: 1500, wantStart: 1500},
		{name: "exactly at oldest is untouched", start: oldest, wantStart: oldest},
		{name: "normal resume at tip+1 is untouched", start: latest + 1, wantStart: latest + 1},
		{name: "slightly ahead of a stale tip is tolerated", start: latest + resetDetectionSlack, wantStart: latest + resetDetectionSlack},
		{name: "below retention clamps and records the gap", start: 400, wantStart: oldest, wantGap: true},
		{name: "far beyond the tip means reset or wrong chain", start: latest + resetDetectionSlack + 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, gap, err := clampStartLedger(tt.start, oldest, latest, now)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got start=%d gap=%v", got, gap)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.wantStart {
				t.Fatalf("start = %d, want %d", got, tt.wantStart)
			}
			if (gap != nil) != tt.wantGap {
				t.Fatalf("gap = %v, wantGap = %v", gap, tt.wantGap)
			}
			if gap != nil {
				if gap.FromLedger != tt.start || gap.ToLedger != oldest-1 {
					t.Fatalf("gap range = [%d, %d], want [%d, %d]", gap.FromLedger, gap.ToLedger, tt.start, oldest-1)
				}
				if gap.Reason != gapReasonRPCRetention {
					t.Fatalf("gap reason = %q, want %q", gap.Reason, gapReasonRPCRetention)
				}
				if !gap.DetectedAt.Equal(now) {
					t.Fatalf("gap detected_at = %v, want %v", gap.DetectedAt, now)
				}
			}
		})
	}
}

func TestClampStartLedger_InvertedWindowIsRejected(t *testing.T) {
	if _, _, err := clampStartLedger(100, 2000, 1000, time.Now()); err == nil {
		t.Fatal("expected an error for an inverted [oldest, latest] window")
	}
}
