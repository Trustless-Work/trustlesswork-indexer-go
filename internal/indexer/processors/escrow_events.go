package processors

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
	"github.com/stellar/go-stellar-sdk/ingest"
	"github.com/stellar/go-stellar-sdk/strkey"
	"github.com/stellar/go-stellar-sdk/xdr"
)

// transferTopic is the first-topic Symbol of a SAC/SEP-41 transfer event.
const transferTopic = "transfer"

// EscrowEventType discriminates the two fact kinds this detector emits.
type EscrowEventType string

const (
	// EscrowEventTypeEvent is a contract event emitted by an escrow
	// itself (the emitter IS the escrow).
	EscrowEventTypeEvent EscrowEventType = "event"
	// EscrowEventTypeDeposit is a SAC/SEP-41 transfer whose recipient is
	// a known escrow (the emitter is the token contract).
	EscrowEventTypeDeposit EscrowEventType = "deposit"
)

// EscrowEvent is a detected fact about one Soroban contract event of
// interest. It carries minimal identity plus the raw event XDR; the
// downstream consumer decodes the payload. Fields map directly onto the
// output envelope (see docs/event-schema.md).
type EscrowEvent struct {
	Type           EscrowEventType
	EscrowID       string // the escrow this fact concerns (C... strkey)
	EventKind      string // topic[0] for events; "token_transfer" for deposits
	EventIndex     uint32 // position within the transaction's event list
	TxHash         string
	TxIndex        uint32 // application order within the ledger
	LedgerSeq      uint32
	LedgerClosedAt time.Time
	RawXDR         string // base64 of the full xdr.ContractEvent
}

// EscrowEventDetector walks a transaction's contract events and returns
// the subset concerning known escrows, identified via the registry:
//
//   - any event emitted by a registered escrow (forwarded regardless of
//     topic — new event kinds need no code change);
//   - any SAC transfer whose `to` address is a registered escrow.
//
// It does NOT enumerate TW topics and does NOT decode contract-specific
// payloads; it filters by identity and forwards raw XDR.
type EscrowEventDetector struct {
	registry *registry.Registry
}

// NewEscrowEventDetector builds a detector over the given registry.
func NewEscrowEventDetector(reg *registry.Registry) *EscrowEventDetector {
	return &EscrowEventDetector{registry: reg}
}

// Name identifies the processor in logs/metrics.
func (d *EscrowEventDetector) Name() string { return "escrow_events" }

// DetectFromTransaction returns the escrow-related events found in tx, in
// the order encountered. EventIndex is a per-transaction running counter
// across all operations' events, matching how Soroban consumers number
// events within a transaction.
func (d *EscrowEventDetector) DetectFromTransaction(tx ingest.LedgerTransaction) ([]EscrowEvent, error) {
	ledgerSeq := tx.Ledger.LedgerSequence()
	closedAt := time.Unix(tx.Ledger.LedgerCloseTime(), 0).UTC()
	txHash := tx.Hash.HexString()
	txIndex := tx.Index

	var out []EscrowEvent
	var eventIdx uint32

	opCount := uint32(len(tx.Envelope.Operations()))
	for opIdx := uint32(0); opIdx < opCount; opIdx++ {
		events, err := tx.GetContractEventsForOperation(opIdx)
		if err != nil {
			return out, fmt.Errorf("getting contract events for op=%d: %w", opIdx, err)
		}
		for _, ev := range events {
			fact, ok := d.classify(ev)
			if ok {
				raw, err := encodeEventXDR(ev)
				if err != nil {
					return out, fmt.Errorf("encoding event idx=%d: %w", eventIdx, err)
				}
				fact.EventIndex = eventIdx
				fact.TxHash = txHash
				fact.TxIndex = txIndex
				fact.LedgerSeq = ledgerSeq
				fact.LedgerClosedAt = closedAt
				fact.RawXDR = raw
				out = append(out, fact)
			}
			eventIdx++
		}
	}
	return out, nil
}

// classify decides whether ev concerns a known escrow and, if so, how.
// It never errors: a malformed or irrelevant event is simply not of
// interest.
func (d *EscrowEventDetector) classify(ev xdr.ContractEvent) (EscrowEvent, bool) {
	// 1) Event emitted by an escrow itself → forward it whatever the kind.
	if emitter, ok := contractIDFromEvent(ev); ok && d.registry.IsEscrow(emitter) {
		kind, _ := firstTopicSymbol(ev) // may be empty; still forwarded
		return EscrowEvent{Type: EscrowEventTypeEvent, EscrowID: emitter, EventKind: kind}, true
	}

	// 2) SAC transfer whose recipient is a known escrow → a deposit.
	if sym, ok := firstTopicSymbol(ev); ok && sym == transferTopic {
		if to, ok := transferToAddress(ev); ok && d.registry.IsEscrow(to) {
			return EscrowEvent{Type: EscrowEventTypeDeposit, EscrowID: to, EventKind: "token_transfer"}, true
		}
	}

	return EscrowEvent{}, false
}

// contractIDFromEvent returns the emitter contract as a "C..." strkey,
// or ok=false for system events with no contract id.
func contractIDFromEvent(ev xdr.ContractEvent) (string, bool) {
	if ev.ContractId == nil {
		return "", false
	}
	enc, err := strkey.Encode(strkey.VersionByteContract, ev.ContractId[:])
	if err != nil {
		return "", false
	}
	return enc, true
}

// firstTopicSymbol returns the Symbol of the event's first topic.
func firstTopicSymbol(ev xdr.ContractEvent) (string, bool) {
	if ev.Body.V != 0 || ev.Body.V0 == nil {
		return "", false
	}
	topics := ev.Body.V0.Topics
	if len(topics) == 0 {
		return "", false
	}
	sym, ok := topics[0].GetSym()
	if !ok {
		return "", false
	}
	return string(sym), true
}

// transferToAddress extracts the `to` address (topics[2]) of a SAC/SEP-41
// transfer event: topics = [Symbol("transfer"), from, to].
func transferToAddress(ev xdr.ContractEvent) (string, bool) {
	if ev.Body.V != 0 || ev.Body.V0 == nil {
		return "", false
	}
	topics := ev.Body.V0.Topics
	if len(topics) < 3 {
		return "", false
	}
	return addressFromScVal(topics[2])
}

// addressFromScVal decodes an ScVal carrying an ScAddress into its strkey
// ("G..." for an account, "C..." for a contract).
func addressFromScVal(v xdr.ScVal) (string, bool) {
	addr, ok := v.GetAddress()
	if !ok {
		return "", false
	}
	s, err := addr.String()
	if err != nil || s == "" {
		return "", false
	}
	return s, true
}

// encodeEventXDR returns the base64 marshal of the full ContractEvent.
func encodeEventXDR(ev xdr.ContractEvent) (string, error) {
	b, err := ev.MarshalBinary()
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}
