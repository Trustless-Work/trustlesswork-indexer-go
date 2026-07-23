// Package events defines the wire contract the Indexer publishes: the
// Envelope and how it is built from a detected EscrowEvent. See
// docs/event-schema.md for the full specification.
package events

import (
	"errors"
	"fmt"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer/processors"
)

// CurrentSchemaVersion is the wire-contract version stamped on every
// envelope. Bump per the versioning policy in docs/event-schema.md.
//
// 1.1 (additive): state envelopes may carry state_change_type "removed"
// with an EMPTY raw_xdr (the entry no longer exists — the signal is the
// payload), and a new type "control" reports pipeline-level facts, today
// only control_kind "gap_detected" (a skipped ledger range). The consumer
// accepts any 1.x; deploy the consumer BEFORE an indexer that emits these.
const CurrentSchemaVersion = "1.1"

// ControlKindGapDetected marks a control envelope reporting a ledger
// range the indexer knowingly skipped (e.g. clamped past RPC retention).
const ControlKindGapDetected = "gap_detected"

// StateChangeRemoved is the state_change_type of a state envelope whose
// on-chain entry no longer exists.
const StateChangeRemoved = "removed"

// ErrEnvelopeInvalid is returned by Validate when a required field is
// missing. A sink should treat it as fatal (a caller bug), not transient.
var ErrEnvelopeInvalid = errors.New("envelope invalid")

// Envelope is one message published to the broker. The JSON field names
// are the wire contract (snake_case) and must match docs/event-schema.md.
type Envelope struct {
	SchemaVersion string `json:"schema_version"`
	Type          string `json:"type"`
	Network       string `json:"network"`
	// ContractID is empty for type "control": a gap concerns a ledger
	// range, not one escrow.
	ContractID string `json:"contract_id,omitempty"`
	LedgerSeq  uint32 `json:"ledger_seq"`
	// omitzero: control envelopes carry no chain clock; data envelopes
	// always set it, so their wire shape is unchanged.
	LedgerClosedAt time.Time `json:"ledger_closed_at,omitzero"`
	TxHash         string    `json:"tx_hash,omitempty"`
	TxIndex        uint32    `json:"tx_index,omitempty"`
	EventKind      string    `json:"event_kind,omitempty"`
	EventIndex     uint32    `json:"event_index,omitempty"`
	// StateChangeType is set only for type "state" (created|updated|removed).
	StateChangeType string    `json:"state_change_type,omitempty"`
	MessageID       string    `json:"message_id"`
	PublishedAt     time.Time `json:"published_at"`
	// RawXDR is empty for "control" and for state "removed" (no entry to
	// carry — the signal is the payload).
	RawXDR string `json:"raw_xdr"`

	// ── Control fields (type "control" only, schema 1.1) ──
	ControlKind string `json:"control_kind,omitempty"`
	// Inclusive skipped range for control_kind "gap_detected".
	GapFromLedger uint32 `json:"gap_from_ledger,omitempty"`
	GapToLedger   uint32 `json:"gap_to_ledger,omitempty"`
	// Reason is the machine-readable cause, e.g. "rpc_retention".
	Reason     string    `json:"reason,omitempty"`
	DetectedAt time.Time `json:"detected_at,omitzero"`
}

// NewMessageID builds the deterministic idempotency key for an
// event/deposit envelope: "{tx_hash}:{event_index}". Consumers dedupe on
// it (INSERT ... ON CONFLICT DO NOTHING).
func NewMessageID(txHash string, eventIndex uint32) string {
	return fmt.Sprintf("%s:%d", txHash, eventIndex)
}

// FromEscrowEvent maps a detected fact onto a wire envelope, stamping the
// schema version, message id and publish time.
func FromEscrowEvent(network string, ev processors.EscrowEvent) Envelope {
	return Envelope{
		SchemaVersion:  CurrentSchemaVersion,
		Type:           string(ev.Type),
		Network:        network,
		ContractID:     ev.EscrowID,
		LedgerSeq:      ev.LedgerSeq,
		LedgerClosedAt: ev.LedgerClosedAt,
		TxHash:         ev.TxHash,
		TxIndex:        ev.TxIndex,
		EventKind:      ev.EventKind,
		EventIndex:     ev.EventIndex,
		MessageID:      NewMessageID(ev.TxHash, ev.EventIndex),
		PublishedAt:    time.Now().UTC(),
		RawXDR:         ev.RawXDR,
	}
}

// NewStateMessageID builds the idempotency key for a state envelope:
// "{contract_id}:{ledger_seq}". Latest-wins: the consumer upserts the row
// with the highest ledger_seq.
func NewStateMessageID(contractID string, ledgerSeq uint32) string {
	return fmt.Sprintf("%s:%d", contractID, ledgerSeq)
}

// FromStateChange maps a detected ContractData state change onto a state
// envelope.
func FromStateChange(network string, sc processors.EscrowStateChange) Envelope {
	return Envelope{
		SchemaVersion:   CurrentSchemaVersion,
		Type:            "state",
		Network:         network,
		ContractID:      sc.EscrowID,
		LedgerSeq:       sc.LedgerSeq,
		LedgerClosedAt:  sc.LedgerClosedAt,
		StateChangeType: sc.StateChangeType,
		MessageID:       NewStateMessageID(sc.EscrowID, sc.LedgerSeq),
		PublishedAt:     time.Now().UTC(),
		RawXDR:          sc.RawXDR,
	}
}

// NewGapMessageID builds the idempotency key for a gap control envelope:
// "gap:{network}:{from}:{to}". Deterministic on purpose — the indexer
// republishes gap evidence on every boot (at-least-once), and the
// consumer's unique key makes every arrival after the first a no-op.
func NewGapMessageID(network string, fromLedger, toLedger uint32) string {
	return fmt.Sprintf("gap:%s:%d:%d", network, fromLedger, toLedger)
}

// FromGap builds the control envelope reporting a skipped ledger range.
// LedgerSeq is stamped with the ledger AFTER the gap (where processing
// resumed): it is the only chain anchor a gap has, and consumers bound
// it with the same plausibility window as data envelopes.
func FromGap(network string, fromLedger, toLedger uint32, reason string, detectedAt time.Time) Envelope {
	return Envelope{
		SchemaVersion: CurrentSchemaVersion,
		Type:          "control",
		ControlKind:   ControlKindGapDetected,
		Network:       network,
		LedgerSeq:     toLedger + 1,
		GapFromLedger: fromLedger,
		GapToLedger:   toLedger,
		Reason:        reason,
		DetectedAt:    detectedAt.UTC(),
		MessageID:     NewGapMessageID(network, fromLedger, toLedger),
		PublishedAt:   time.Now().UTC(),
	}
}

// RoutingKey is the topic-exchange key for this envelope:
// "stellar.<network>.escrow.<type>.<kind>", where kind is the event kind
// for event/deposit, the state_change_type for state, and the control
// kind for control. Every segment is a single token so AMQP
// single-segment wildcards work.
func (e Envelope) RoutingKey() string {
	kind := e.EventKind
	switch e.Type {
	case "state":
		kind = e.StateChangeType
	case "control":
		kind = e.ControlKind
	}
	return fmt.Sprintf("stellar.%s.escrow.%s.%s", e.Network, e.Type, kind)
}

// Validate checks that the required fields are present, returning a
// wrapped ErrEnvelopeInvalid otherwise. Requirements are per-type since
// 1.1: control envelopes have no contract/XDR, and a removed state has
// no XDR (there is no entry left to carry).
func (e Envelope) Validate() error {
	// Common fields, required for every type.
	switch {
	case e.SchemaVersion == "":
		return fmt.Errorf("%w: missing schema_version", ErrEnvelopeInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: missing type", ErrEnvelopeInvalid)
	case e.Network == "":
		return fmt.Errorf("%w: missing network", ErrEnvelopeInvalid)
	case e.MessageID == "":
		return fmt.Errorf("%w: missing message_id", ErrEnvelopeInvalid)
	case e.LedgerSeq == 0:
		return fmt.Errorf("%w: ledger_seq must be > 0", ErrEnvelopeInvalid)
	}

	// Type-specific fields.
	switch e.Type {
	case "event", "deposit":
		if e.ContractID == "" {
			return fmt.Errorf("%w: missing contract_id", ErrEnvelopeInvalid)
		}
		if e.RawXDR == "" {
			return fmt.Errorf("%w: missing raw_xdr", ErrEnvelopeInvalid)
		}
		if e.EventKind == "" {
			return fmt.Errorf("%w: missing event_kind for %s", ErrEnvelopeInvalid, e.Type)
		}
		if e.TxHash == "" {
			return fmt.Errorf("%w: missing tx_hash for %s", ErrEnvelopeInvalid, e.Type)
		}
	case "state":
		if e.ContractID == "" {
			return fmt.Errorf("%w: missing contract_id", ErrEnvelopeInvalid)
		}
		if e.StateChangeType == "" {
			return fmt.Errorf("%w: missing state_change_type for state", ErrEnvelopeInvalid)
		}
		// A removed entry HAS no XDR; every other state change must carry it.
		if e.StateChangeType != StateChangeRemoved && e.RawXDR == "" {
			return fmt.Errorf("%w: missing raw_xdr", ErrEnvelopeInvalid)
		}
	case "control":
		if e.ControlKind == "" {
			return fmt.Errorf("%w: missing control_kind for control", ErrEnvelopeInvalid)
		}
		if e.GapFromLedger == 0 || e.GapToLedger == 0 || e.GapFromLedger > e.GapToLedger {
			return fmt.Errorf("%w: control gap range [%d, %d] is not a valid ledger range",
				ErrEnvelopeInvalid, e.GapFromLedger, e.GapToLedger)
		}
	default:
		return fmt.Errorf("%w: unknown type %q", ErrEnvelopeInvalid, e.Type)
	}
	return nil
}
