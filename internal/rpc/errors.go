// Package rpc owns the Indexer's view of the Stellar RPC error space.
// The Stellar Go SDK and Soroban RPC return errors as opaque strings,
// JRPC error objects, or wrapped *url.Error/http.Response status codes.
// The main ingestion loop should not have to know about those — it
// should be able to ask "is this a wait-and-retry?" or "is this fatal?".
//
// Two pieces fulfill that goal:
//
//   - Sentinel errors in this file. Every RPC failure the loop cares
//     about gets its own sentinel. Callers compare via errors.Is.
//   - Classify (classify.go). A pure helper that takes any error from a
//     RPC call site and returns it wrapped with the appropriate
//     sentinel. Bounded surface: the classifier knows what kinds of
//     errors the SDK / our http client can produce, and never silently
//     reclassifies things it doesn't recognize.
//
// Adding a new RPC failure mode: add the sentinel here, extend Classify
// with the recognition logic, and (if it changes category) update the
// predicates in internal/errs.
package rpc

import "errors"

// Sentinel errors emitted by Classify.
//
// Category mapping (see internal/errs):
//
//   - ErrLedgerNotYetAvailable    → transient (wait + retry)
//   - ErrRPCUnreachable           → transient
//   - ErrRPCInvalidResponse       → transient with caution (often
//                                   indicates a server-side incident
//                                   that recovers within seconds)
//   - ErrLedgerOutOfRetention     → fatal (cursor is too old)
//
var (
	// ErrLedgerNotYetAvailable indicates the requested ledger has not
	// closed yet. Normal at-tip behavior; the loop should wait and
	// retry. The Soroban RPC commonly responds with HTTP 404 / a
	// specific error code for this case.
	ErrLedgerNotYetAvailable = errors.New("ledger not yet closed by network")

	// ErrLedgerOutOfRetention indicates the requested ledger is older
	// than what the RPC retains. The cursor has fallen behind the RPC's
	// retention window. Fatal: there is no way for this Indexer to
	// catch up via this RPC without intervention (use a different RPC,
	// a datastore backend, or reset the cursor).
	ErrLedgerOutOfRetention = errors.New("ledger out of RPC retention window")

	// ErrRPCUnreachable indicates a transport-level failure when
	// contacting the RPC (DNS, dial, TLS, refused connection, network
	// timeout, etc.). Transient.
	ErrRPCUnreachable = errors.New("RPC unreachable")

	// ErrRPCInvalidResponse indicates the RPC responded but with a body
	// the Indexer cannot parse (HTTP 5xx with non-JSON body, JRPC error
	// object the SDK could not deserialize). Transient with caution —
	// often indicates server-side incident.
	ErrRPCInvalidResponse = errors.New("RPC returned invalid response")
)
