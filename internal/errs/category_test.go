package errs

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/state"
)

func TestIsFatal_MatchesStateSentinels(t *testing.T) {
	cases := []error{
		state.ErrStateCorrupted,
		state.ErrStateVersionMismatch,
		state.ErrStateNetworkMismatch,
		state.ErrStateLockHeld,
	}
	for _, e := range cases {
		t.Run(e.Error(), func(t *testing.T) {
			if !IsFatal(e) {
				t.Errorf("expected fatal: %v", e)
			}
		})
	}
}

func TestIsFatal_MatchesEnvelopeInvalid(t *testing.T) {
	if !IsFatal(events.ErrEnvelopeInvalid) {
		t.Fatal("ErrEnvelopeInvalid must be fatal — it's our bug, not data")
	}
}

func TestIsFatal_WalksWrappedChain(t *testing.T) {
	wrapped := fmt.Errorf("loading state: %w", state.ErrStateCorrupted)
	if !IsFatal(wrapped) {
		t.Fatal("IsFatal must traverse %w wrapping")
	}
}

func TestIsFatal_RejectsUnrelated(t *testing.T) {
	if IsFatal(errors.New("random error")) {
		t.Fatal("IsFatal must not match unclassified errors")
	}
	if IsFatal(nil) {
		t.Fatal("IsFatal(nil) must be false")
	}
}

func TestIsSkippable_MatchesXDRDecoding(t *testing.T) {
	if !IsSkippable(events.ErrXDRDecodingFail) {
		t.Fatal("ErrXDRDecodingFail must be skippable")
	}
}

func TestIsSkippable_WalksWrappedChain(t *testing.T) {
	wrapped := fmt.Errorf("decoding event 3: %w", events.ErrXDRDecodingFail)
	if !IsSkippable(wrapped) {
		t.Fatal("IsSkippable must traverse %w wrapping")
	}
}

func TestIsSkippable_RejectsFatal(t *testing.T) {
	// Fatal and Skippable must be disjoint categories.
	if IsSkippable(state.ErrStateCorrupted) {
		t.Fatal("fatal errors must not be classified as skippable")
	}
}

func TestIsTransient_Phase1IsEmpty(t *testing.T) {
	// Phase 1 invariant: nothing is transient until Phase 2 adds RPC/sink
	// sentinels. This test pins the invariant so a future change is
	// forced to update the contract intentionally.
	cases := []error{
		nil,
		errors.New("random"),
		events.ErrUnknownTopic,
		events.ErrXDRDecodingFail,
		events.ErrEnvelopeInvalid,
		state.ErrStateNotFound,
		state.ErrStateCorrupted,
	}
	for _, e := range cases {
		if IsTransient(e) {
			t.Errorf("Phase 1: nothing must be transient yet; got true for %v", e)
		}
	}
}
