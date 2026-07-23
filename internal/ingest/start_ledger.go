package ingest

import (
	"fmt"
	"time"

	"github.com/Trustless-Work/Indexer/internal/state"
)

// gapReasonRPCRetention marks a gap caused by the resume cursor falling
// below the RPC's retention window. The value is part of the state file
// and of the (future) gap control envelope — treat it as a wire constant.
const gapReasonRPCRetention = "rpc_retention"

// resetDetectionSlack is how many ledgers ABOVE the RPC tip the start
// ledger may sit before we call it impossible. A normal resume lands at
// tip+1 (and a slightly stale getHealth can make that look a few ledgers
// ahead); anything further means the chain restarted below our cursor —
// a testnet reset — or the RPC serves a different, shorter chain. Both
// need an operator, not a wait: with the same passphrase the network
// check cannot catch this, and the backend would poll for a ledger that
// is months away.
const resetDetectionSlack = 60

// clampStartLedger validates start against the RPC's served window
// [oldest, latest] and returns the ledger to actually start from.
//
// Three outcomes:
//   - start inside (or immediately after) the window: returned unchanged.
//   - start below oldest: clamped to oldest, plus a Gap recording the
//     skipped range [start, oldest-1]. The caller must persist that gap
//     BEFORE processing anything — it is the only durable evidence a
//     later backfill can use to know what to replay. This replaces the
//     pre-Sprint-5 behaviour (a deterministic PrepareRange failure and a
//     crash-loop: the 2026-07-22 incident).
//   - start absurdly beyond latest: an error naming the likely cause.
//
// now is injected so tests produce deterministic Gap timestamps.
func clampStartLedger(start, oldest, latest uint32, now time.Time) (uint32, *state.Gap, error) {
	if oldest > latest {
		return 0, nil, fmt.Errorf("RPC reports an inverted ledger window [%d, %d] — refusing to start against it", oldest, latest)
	}

	if start > latest+resetDetectionSlack {
		return 0, nil, fmt.Errorf(
			"start ledger %d is far beyond the RPC tip %d: the chain appears to have restarted below our cursor (testnet reset?) or RPC_URL serves the wrong chain — archive the state file and start fresh, or fix RPC_URL",
			start, latest)
	}

	if start >= oldest {
		return start, nil, nil
	}

	gap := &state.Gap{
		FromLedger: start,
		ToLedger:   oldest - 1,
		Reason:     gapReasonRPCRetention,
		DetectedAt: now.UTC(),
	}
	return oldest, gap, nil
}
