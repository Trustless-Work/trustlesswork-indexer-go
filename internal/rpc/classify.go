package rpc

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
	"syscall"
)

// Classify translates an opaque error from a Stellar RPC / SDK call into
// one wrapped with a stable sentinel from this package. The returned
// error always wraps the original (with %w) so callers can still see the
// underlying details via errors.Unwrap.
//
// Classification rules, in priority order:
//
//  1. nil → returned as nil. Convenient for hot-path callers that do
//     `return Classify(err)`.
//  2. Context errors (Canceled, DeadlineExceeded) → returned unchanged.
//     The category package treats these as a separate "clean shutdown"
//     class; we must not reclassify them as transient or fatal.
//  3. Substring match on the error message for known RPC outcomes:
//      - "not found" / "ledger not yet" / "not yet available" / "404"
//          → ErrLedgerNotYetAvailable
//      - "out of range" / "retention" / "too old"
//          → ErrLedgerOutOfRetention
//      - "invalid response" / "parsing" / "malformed"
//          → ErrRPCInvalidResponse
//  4. Network failure signature (*url.Error wrapping a transport error,
//     net.Error, syscall.ECONNREFUSED/EHOSTUNREACH/etc.) →
//     ErrRPCUnreachable.
//  5. Otherwise → returned wrapped with ErrRPCInvalidResponse with a
//     hint that it was unclassified. This is the conservative default:
//     the loop treats it as transient-with-caution rather than fail-fast.
//
// Substring matching is intentionally pragmatic — the Stellar Go SDK does
// not export typed errors for these conditions, so callers either match
// on strings or wrap. We keep the substrings narrow and the order
// deterministic. If the SDK introduces typed errors later, switch to
// errors.As; the public surface of Classify does not change.
func Classify(err error) error {
	if err == nil {
		return nil
	}

	// Context errors flow through. Do not wrap.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}

	msg := strings.ToLower(err.Error())

	switch {
	case containsAny(msg, "not yet available", "ledger not yet", "not yet been closed"):
		return fmt.Errorf("%w: %v", ErrLedgerNotYetAvailable, err)
	case containsAny(msg, "out of range", "out of retention", "too old", "not in retention"):
		return fmt.Errorf("%w: %v", ErrLedgerOutOfRetention, err)
	case isNetworkFailure(err):
		return fmt.Errorf("%w: %v", ErrRPCUnreachable, err)
	case strings.Contains(msg, "404") || strings.Contains(msg, "not found"):
		// Soroban RPC commonly reports "ledger not yet" as a 404;
		// after the more-specific matches above, treat a plain 404
		// as "not yet available" (the cursor-out-of-retention case
		// is already matched above by "out of range"/"retention").
		return fmt.Errorf("%w: %v", ErrLedgerNotYetAvailable, err)
	case containsAny(msg, "invalid response", "parsing", "malformed", "unexpected"):
		return fmt.Errorf("%w: %v", ErrRPCInvalidResponse, err)
	}

	// Unrecognized failure. Treat as invalid-response so the main loop
	// applies transient retry semantics rather than fail-fast — we
	// prefer to keep going on novel errors and gather more data than
	// to halt the indexer on a surprise.
	return fmt.Errorf("%w: unclassified RPC error: %v", ErrRPCInvalidResponse, err)
}

// containsAny reports whether s contains any of needles. Lowercase
// expected from the caller.
func containsAny(s string, needles ...string) bool {
	for _, n := range needles {
		if strings.Contains(s, n) {
			return true
		}
	}
	return false
}

// isNetworkFailure inspects err's chain for typed network failures.
// Returns true for any of:
//   - *url.Error whose underlying err is a transport problem
//   - net.Error with Timeout() true OR a known syscall like ECONNREFUSED
//   - direct syscall errors (ECONNREFUSED, EHOSTUNREACH, ENETUNREACH)
func isNetworkFailure(err error) bool {
	// syscall errno checks
	var sysErr syscall.Errno
	if errors.As(err, &sysErr) {
		switch sysErr {
		case syscall.ECONNREFUSED, syscall.EHOSTUNREACH, syscall.ENETUNREACH, syscall.ETIMEDOUT:
			return true
		}
	}

	// *url.Error wrapping a transport failure (DNS, dial, TLS, ...).
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		return true
	}

	// net.Error (covers most timeouts).
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}

	return false
}
