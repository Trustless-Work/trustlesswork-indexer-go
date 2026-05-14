package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// These tests exercise the recorder functions for basic correctness and
// label cardinality. They use prometheus/client_golang/testutil to
// inspect counter values without parsing the /metrics output.

func TestRecordLedgerProcessed_IncrementsCounter(t *testing.T) {
	before := testutil.ToFloat64(ledgerProcessedTotal.WithLabelValues("testnet", StatusOK))
	RecordLedgerProcessed("testnet", 0.1, StatusOK)
	after := testutil.ToFloat64(ledgerProcessedTotal.WithLabelValues("testnet", StatusOK))
	if after-before != 1 {
		t.Fatalf("expected counter to increment by 1; got delta=%v", after-before)
	}
}

func TestSetCurrentLedger_StoresValue(t *testing.T) {
	SetCurrentLedger("testnet", 12345)
	got := testutil.ToFloat64(currentLedgerSeq.WithLabelValues("testnet"))
	if got != 12345 {
		t.Fatalf("expected 12345; got %v", got)
	}
	// Setting a smaller value must overwrite, not max — the metric is a
	// gauge, not a high-water mark.
	SetCurrentLedger("testnet", 12000)
	got = testutil.ToFloat64(currentLedgerSeq.WithLabelValues("testnet"))
	if got != 12000 {
		t.Fatalf("expected gauge to be overwritten to 12000; got %v", got)
	}
}

func TestSetLag_HandlesUnknownTip(t *testing.T) {
	// Initialize to a known value, then call SetLag with tipLedger=0 —
	// the gauge must stay where it was, not reset.
	lagLedgers.WithLabelValues("testnet").Set(7)
	SetLag("testnet", 0, 100) // tip unknown
	got := testutil.ToFloat64(lagLedgers.WithLabelValues("testnet"))
	if got != 7 {
		t.Fatalf("expected gauge unchanged; got %v", got)
	}
}

func TestSetLag_HandlesTipBehindCurrent(t *testing.T) {
	// Defensive: if the RPC reports a tip lower than our current
	// ledger (shouldn't happen but could during failover), we must
	// not record a negative lag.
	lagLedgers.WithLabelValues("testnet").Set(3)
	SetLag("testnet", 50, 100)
	got := testutil.ToFloat64(lagLedgers.WithLabelValues("testnet"))
	if got != 3 {
		t.Fatalf("expected gauge unchanged when tip<current; got %v", got)
	}
}

func TestSetSinkUp_Binary(t *testing.T) {
	SetSinkUp("rabbitmq", true)
	if got := testutil.ToFloat64(sinkUp.WithLabelValues("rabbitmq")); got != 1 {
		t.Fatalf("expected 1; got %v", got)
	}
	SetSinkUp("rabbitmq", false)
	if got := testutil.ToFloat64(sinkUp.WithLabelValues("rabbitmq")); got != 0 {
		t.Fatalf("expected 0; got %v", got)
	}
}

func TestRecordEventDetected_PerKind(t *testing.T) {
	before := testutil.ToFloat64(eventsDetectedTotal.WithLabelValues("testnet", "tw_init"))
	RecordEventDetected("testnet", "tw_init")
	RecordEventDetected("testnet", "tw_init")
	after := testutil.ToFloat64(eventsDetectedTotal.WithLabelValues("testnet", "tw_init"))
	if after-before != 2 {
		t.Fatalf("expected +2; got delta=%v", after-before)
	}
}

func TestRecordError_PerCategory(t *testing.T) {
	before := testutil.ToFloat64(errorsTotal.WithLabelValues(CategoryTransient))
	RecordError(CategoryTransient)
	after := testutil.ToFloat64(errorsTotal.WithLabelValues(CategoryTransient))
	if after-before != 1 {
		t.Fatalf("expected +1; got delta=%v", after-before)
	}
}
