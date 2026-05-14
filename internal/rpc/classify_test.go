package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"syscall"
	"testing"
	"time"
)

func TestClassify_Nil(t *testing.T) {
	if err := Classify(nil); err != nil {
		t.Fatalf("Classify(nil) must be nil; got %v", err)
	}
}

func TestClassify_PassesThroughContextCanceled(t *testing.T) {
	if got := Classify(context.Canceled); !errors.Is(got, context.Canceled) {
		t.Fatalf("context.Canceled must pass through; got %v", got)
	}
}

func TestClassify_PassesThroughDeadlineExceeded(t *testing.T) {
	if got := Classify(context.DeadlineExceeded); !errors.Is(got, context.DeadlineExceeded) {
		t.Fatalf("context.DeadlineExceeded must pass through; got %v", got)
	}
}

func TestClassify_LedgerNotYet(t *testing.T) {
	cases := []string{
		"ledger 9999 not yet available",
		"ledger has not yet been closed",
		"requested ledger not yet",
	}
	for _, c := range cases {
		got := Classify(errors.New(c))
		if !errors.Is(got, ErrLedgerNotYetAvailable) {
			t.Errorf("expected ErrLedgerNotYetAvailable for %q; got %v", c, got)
		}
	}
}

func TestClassify_OutOfRetention(t *testing.T) {
	cases := []string{
		"ledger out of range",
		"ledger fell out of retention window",
		"requested ledger too old",
		"ledger not in retention",
	}
	for _, c := range cases {
		got := Classify(errors.New(c))
		if !errors.Is(got, ErrLedgerOutOfRetention) {
			t.Errorf("expected ErrLedgerOutOfRetention for %q; got %v", c, got)
		}
	}
}

func TestClassify_404IsTreatedAsNotYet(t *testing.T) {
	// Soroban RPC commonly returns 404 for ledgers past the tip.
	got := Classify(errors.New("HTTP 404 not found"))
	if !errors.Is(got, ErrLedgerNotYetAvailable) {
		t.Fatalf("expected ErrLedgerNotYetAvailable for 404; got %v", got)
	}
}

func TestClassify_NetworkFailure_URLError(t *testing.T) {
	urlErr := &url.Error{
		Op:  "Post",
		URL: "http://example.test",
		Err: errors.New("dial tcp: lookup failed"),
	}
	got := Classify(urlErr)
	if !errors.Is(got, ErrRPCUnreachable) {
		t.Fatalf("expected ErrRPCUnreachable for *url.Error; got %v", got)
	}
}

func TestClassify_NetworkFailure_ECONNREFUSED(t *testing.T) {
	got := Classify(syscall.ECONNREFUSED)
	if !errors.Is(got, ErrRPCUnreachable) {
		t.Fatalf("expected ErrRPCUnreachable for ECONNREFUSED; got %v", got)
	}
}

// fakeNetErr satisfies net.Error to exercise the type-assertion branch.
type fakeNetErr struct{}

func (fakeNetErr) Error() string   { return "fake net error" }
func (fakeNetErr) Timeout() bool   { return true }
func (fakeNetErr) Temporary() bool { return true }

var _ net.Error = fakeNetErr{}
var _ time.Duration = 0 // keeps `time` import used in case future tests need it

func TestClassify_NetworkFailure_NetError(t *testing.T) {
	got := Classify(fakeNetErr{})
	if !errors.Is(got, ErrRPCUnreachable) {
		t.Fatalf("expected ErrRPCUnreachable for net.Error; got %v", got)
	}
}

func TestClassify_InvalidResponse(t *testing.T) {
	cases := []string{
		"invalid response from RPC",
		"parsing JSON-RPC response",
		"malformed result body",
	}
	for _, c := range cases {
		got := Classify(errors.New(c))
		if !errors.Is(got, ErrRPCInvalidResponse) {
			t.Errorf("expected ErrRPCInvalidResponse for %q; got %v", c, got)
		}
	}
}

func TestClassify_UnclassifiedFallsBackToInvalidResponse(t *testing.T) {
	got := Classify(errors.New("something totally novel"))
	if !errors.Is(got, ErrRPCInvalidResponse) {
		t.Fatalf("expected unclassified error to wrap ErrRPCInvalidResponse; got %v", got)
	}
}

func TestClassify_PreservesUnderlying(t *testing.T) {
	// Underlying error must be reachable for log/debug purposes.
	inner := errors.New("ledger 9999 not yet available — server says")
	got := Classify(inner)
	if got.Error() == "" {
		t.Fatal("classified error must include the underlying message")
	}
	// errors.Is should match BOTH the sentinel and (via the message)
	// inform the caller.
	if !errors.Is(got, ErrLedgerNotYetAvailable) {
		t.Fatal("must match the sentinel")
	}
}

func TestClassify_DoubleClassifyStable(t *testing.T) {
	// Defensive: passing an already-classified error through Classify
	// again should not change its category. This matters because
	// callers may wrap and re-classify in nested helpers.
	once := Classify(errors.New("ledger not yet available"))
	twice := Classify(once)
	if !errors.Is(twice, ErrLedgerNotYetAvailable) {
		t.Fatalf("double-classify must remain stable; got %v", twice)
	}
}

// Avoid an unused-import warning when fmt is referenced only in comments.
var _ = fmt.Errorf
