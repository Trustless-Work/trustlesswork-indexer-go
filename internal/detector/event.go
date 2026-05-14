// Package detector walks the events in a Soroban ledger and identifies
// the subset the Indexer publishes — events emitted by TW escrow
// contracts (the 12 topic Symbols) plus SAC/SEP-41 transfer events
// whose destination is a known escrow contract.
//
// The detector is the heart of the filter-and-forward model. It does
// not parse contract-specific data structures; it identifies events of
// interest by their topic Symbol (for TW events) or by their topics +
// watchlist membership (for token transfers), captures minimal
// identity, and serializes the original XDR payload as base64. The
// downstream consumer decodes the XDR with the Stellar SDK.
//
// See project_indexer_philosophy.md for the rationale.
package detector

import "time"

// DetectedEvent is the detector's output: a minimal record identifying
// one Soroban contract event of interest, plus its raw XDR for the
// consumer to decode.
//
// The "EscrowID" field is intentionally NOT the Soroban event's emitter
// contract. For TW events (tw_init, tw_fund, ...) the emitter IS the
// escrow, so EscrowID equals the event's contract_id. For
// Indexer-synthesized token_transfer events, the Soroban emitter is the
// SAC/SEP-41 token contract; EscrowID is the watchlist-matched recipient
// (the "to" address of the transfer). This gives the consumer one
// uniform field to key on for "all events for escrow X". The original
// emitter is still available in RawXDR for consumers that need it.
type DetectedEvent struct {
	// EscrowID is the TW escrow contract address this event concerns.
	// Always a "C..." Stellar contract address. Used as
	// Envelope.ContractID downstream.
	EscrowID string

	// TxHash is the hex-encoded hash of the transaction containing
	// the event.
	TxHash string

	// EventIndex is the event's position in the transaction's full
	// contract-event list (zero-indexed across all operations).
	EventIndex uint32

	// EventKind labels the semantic category. For TW-emitted events
	// it is the pass-through Symbol (e.g. "tw_init"). For
	// Indexer-synthesized events it is a namespaced constant
	// (e.g. "token_transfer"). See internal/events for the canonical
	// set.
	EventKind string

	// RawXDR is the base64-encoded marshal of the full
	// xdr.ContractEvent (header + body) at the time of detection.
	// Consumers decode with the Stellar SDK.
	RawXDR string

	// LedgerSeq is the ledger sequence containing this event.
	LedgerSeq uint32

	// LedgerClosedAt is the close time of the ledger (from the chain).
	LedgerClosedAt time.Time
}
