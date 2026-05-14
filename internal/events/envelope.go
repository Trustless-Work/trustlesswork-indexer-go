package events

import (
	"fmt"
	"time"
)

// CurrentSchemaVersion is the active envelope schema. The Indexer stamps
// every published envelope with this value, and downstream consumers branch
// on it when handling schema evolution.
//
// Versioning policy:
//   - Additive changes (new optional fields, new EventKind values) DO NOT
//     bump the version. Consumers must ignore unknown fields/kinds.
//   - Breaking changes (renaming a field, changing a type, removing a field)
//     bump to "v2". During migration, the Indexer publishes in parallel on
//     v1 and v2 routing keys until consumers are upgraded.
//
// See docs/event-schema.md for the full schema and evolution rules.
const CurrentSchemaVersion = "v1"

// Envelope is the wire contract between the Indexer and downstream consumers.
// One Envelope == one detected blockchain event (atomic, idempotent unit).
//
// The envelope carries minimal identity for routing and deduplication, plus
// the raw Soroban event payload as base64-encoded XDR in RawXDR. Consumers
// decode RawXDR with the Stellar SDK on their side; the Indexer does not
// parse contract-specific structures (see project_indexer_philosophy.md).
//
// JSON serialization uses snake_case by convention. Consumers in
// class-based languages should map fields explicitly (e.g. NestJS with
// class-transformer's @Expose).
type Envelope struct {
	// SchemaVersion is the value of CurrentSchemaVersion at publication
	// time. Consumers use it to dispatch to version-specific handlers.
	SchemaVersion string `json:"schema_version"`

	// MessageID uniquely and deterministically identifies this event in
	// the global history. Same physical event ⇒ same MessageID. Suitable
	// as an idempotency key. Format: "<tx_hash>-<event_index>".
	MessageID string `json:"message_id"`

	// Network identifies which Stellar network produced this event. Used
	// both as a routing label and as a guard against cross-network mixups.
	Network string `json:"network"`

	// EventKind labels the semantic category of this event. For events
	// emitted by TW contracts, the value is the pass-through Symbol
	// (e.g. "tw_init"). For Indexer-synthesized events (e.g. token
	// transfers to escrows), it is a namespaced constant.
	EventKind string `json:"event_kind"`

	// ContractID is the Soroban contract address that emitted the event.
	// Always present. Downstream consumers use it as the primary key for
	// per-escrow state.
	ContractID string `json:"contract_id"`

	// TxHash is the hex-encoded transaction hash that contained the
	// event. Always present.
	TxHash string `json:"tx_hash"`

	// LedgerSeq is the sequence number of the ledger that closed the
	// transaction. Used for ordering and replay. A value of 0 means
	// "unset" and is rejected by Validate.
	LedgerSeq uint32 `json:"ledger_seq"`

	// EventIndex is the position of this event within its transaction's
	// event list. Zero-indexed. Required component of MessageID.
	EventIndex uint32 `json:"event_index"`

	// LedgerClosedAt is the close time of the ledger (from the chain).
	// Deterministic, replayable. Use for ordering and idempotency.
	LedgerClosedAt time.Time `json:"ledger_closed_at"`

	// PublishedAt is the wall-clock time at which the Indexer assembled
	// this envelope. Used for observability ("how delayed is the
	// Indexer?") and should not influence consumer logic.
	PublishedAt time.Time `json:"published_at"`

	// RawXDR is the base64-encoded marshal of the original event payload
	// (typically a ContractEventBody for Soroban events). Consumers
	// decode with the Stellar SDK.
	RawXDR string `json:"raw_xdr"`
}

// Validate verifies that every required field of the envelope is populated
// before publication. Sinks call Validate immediately before serialization
// so that caller bugs surface as ErrEnvelopeInvalid rather than as opaque
// transport errors. Validation is conservative: every field is required
// because consumers depend on each for either routing or persistence.
func (e *Envelope) Validate() error {
	if e.SchemaVersion == "" {
		return fmt.Errorf("%w: schema_version is empty", ErrEnvelopeInvalid)
	}
	if e.MessageID == "" {
		return fmt.Errorf("%w: message_id is empty", ErrEnvelopeInvalid)
	}
	if e.Network == "" {
		return fmt.Errorf("%w: network is empty", ErrEnvelopeInvalid)
	}
	if e.EventKind == "" {
		return fmt.Errorf("%w: event_kind is empty", ErrEnvelopeInvalid)
	}
	if e.ContractID == "" {
		return fmt.Errorf("%w: contract_id is empty", ErrEnvelopeInvalid)
	}
	if e.TxHash == "" {
		return fmt.Errorf("%w: tx_hash is empty", ErrEnvelopeInvalid)
	}
	if e.LedgerSeq == 0 {
		return fmt.Errorf("%w: ledger_seq is zero", ErrEnvelopeInvalid)
	}
	if e.LedgerClosedAt.IsZero() {
		return fmt.Errorf("%w: ledger_closed_at is zero", ErrEnvelopeInvalid)
	}
	if e.PublishedAt.IsZero() {
		return fmt.Errorf("%w: published_at is zero", ErrEnvelopeInvalid)
	}
	if e.RawXDR == "" {
		return fmt.Errorf("%w: raw_xdr is empty", ErrEnvelopeInvalid)
	}
	return nil
}
