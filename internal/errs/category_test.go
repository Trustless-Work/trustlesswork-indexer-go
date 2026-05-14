package errs

import (
	"errors"
	"fmt"
	"testing"

	"github.com/Trustless-Work/Indexer/internal/events"
	"github.com/Trustless-Work/Indexer/internal/rpc"
	"github.com/Trustless-Work/Indexer/internal/sink"
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

func TestIsTransient_MatchesRPCAndSink(t *testing.T) {
	cases := []error{
		rpc.ErrLedgerNotYetAvailable,
		rpc.ErrRPCUnreachable,
		rpc.ErrRPCInvalidResponse,
		sink.ErrSinkUnavailable,
		sink.ErrSinkPublishRejected,
	}
	for _, e := range cases {
		t.Run(e.Error(), func(t *testing.T) {
			if !IsTransient(e) {
				t.Errorf("expected transient: %v", e)
			}
		})
	}
}

func TestIsTransient_RejectsNonTransient(t *testing.T) {
	// Errors that belong to fatal/skippable/clean-shutdown categories
	// must NOT be reported as transient.
	cases := []error{
		nil,
		errors.New("random"),
		events.ErrUnknownTopic,
		events.ErrXDRDecodingFail,    // skippable
		events.ErrEnvelopeInvalid,    // fatal
		state.ErrStateNotFound,        // recovered via bootstrap, not retry
		state.ErrStateCorrupted,       // fatal
		rpc.ErrLedgerOutOfRetention,   // fatal
	}
	for _, e := range cases {
		if IsTransient(e) {
			t.Errorf("must not be transient: %v", e)
		}
	}
}

func TestIsFatal_MatchesLedgerOutOfRetention(t *testing.T) {
	if !IsFatal(rpc.ErrLedgerOutOfRetention) {
		t.Fatal("ErrLedgerOutOfRetention must be fatal")
	}
}

func TestCategories_AreDisjoint(t *testing.T) {
	// Spot-check: no sentinel should match more than one of the three
	// predicates. (Context errors are handled separately and should
	// match none.)
	all := []error{
		state.ErrStateCorrupted,
		state.ErrStateVersionMismatch,
		state.ErrStateNetworkMismatch,
		state.ErrStateLockHeld,
		rpc.ErrLedgerOutOfRetention,
		rpc.ErrLedgerNotYetAvailable,
		rpc.ErrRPCUnreachable,
		rpc.ErrRPCInvalidResponse,
		sink.ErrSinkUnavailable,
		sink.ErrSinkPublishRejected,
		events.ErrEnvelopeInvalid,
		events.ErrXDRDecodingFail,
	}
	for _, e := range all {
		count := 0
		if IsTransient(e) {
			count++
		}
		if IsFatal(e) {
			count++
		}
		if IsSkippable(e) {
			count++
		}
		if count != 1 {
			t.Errorf("sentinel %v classified by %d predicates; want exactly 1", e, count)
		}
	}
}
