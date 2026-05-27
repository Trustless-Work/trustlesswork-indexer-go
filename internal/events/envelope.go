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
const CurrentSchemaVersion = "1.0"

// ErrEnvelopeInvalid is returned by Validate when a required field is
// missing. A sink should treat it as fatal (a caller bug), not transient.
var ErrEnvelopeInvalid = errors.New("envelope invalid")

// Envelope is one message published to the broker. The JSON field names
// are the wire contract (snake_case) and must match docs/event-schema.md.
type Envelope struct {
	SchemaVersion  string    `json:"schema_version"`
	Type           string    `json:"type"`
	Network        string    `json:"network"`
	ContractID     string    `json:"contract_id"`
	LedgerSeq      uint32    `json:"ledger_seq"`
	LedgerClosedAt time.Time `json:"ledger_closed_at"`
	TxHash         string    `json:"tx_hash"`
	TxIndex        uint32    `json:"tx_index"`
	EventKind      string    `json:"event_kind"`
	EventIndex     uint32    `json:"event_index"`
	MessageID      string    `json:"message_id"`
	PublishedAt    time.Time `json:"published_at"`
	RawXDR         string    `json:"raw_xdr"`
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

// RoutingKey is the topic-exchange key for this envelope:
// "stellar.<network>.escrow.<type>.<event_kind>". Every segment is a
// single token so AMQP single-segment wildcards work.
func (e Envelope) RoutingKey() string {
	return fmt.Sprintf("stellar.%s.escrow.%s.%s", e.Network, e.Type, e.EventKind)
}

// Validate checks that the required fields are present, returning a
// wrapped ErrEnvelopeInvalid otherwise.
func (e Envelope) Validate() error {
	switch {
	case e.SchemaVersion == "":
		return fmt.Errorf("%w: missing schema_version", ErrEnvelopeInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: missing type", ErrEnvelopeInvalid)
	case e.Network == "":
		return fmt.Errorf("%w: missing network", ErrEnvelopeInvalid)
	case e.ContractID == "":
		return fmt.Errorf("%w: missing contract_id", ErrEnvelopeInvalid)
	case e.TxHash == "":
		return fmt.Errorf("%w: missing tx_hash", ErrEnvelopeInvalid)
	case e.EventKind == "":
		return fmt.Errorf("%w: missing event_kind", ErrEnvelopeInvalid)
	case e.MessageID == "":
		return fmt.Errorf("%w: missing message_id", ErrEnvelopeInvalid)
	case e.RawXDR == "":
		return fmt.Errorf("%w: missing raw_xdr", ErrEnvelopeInvalid)
	case e.LedgerSeq == 0:
		return fmt.Errorf("%w: ledger_seq must be > 0", ErrEnvelopeInvalid)
	}
	return nil
}
