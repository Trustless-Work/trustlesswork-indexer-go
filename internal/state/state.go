package state

import "time"

// CurrentVersion is the schema version of the on-disk State struct.
// Increment when a Load/Save change is not backwards-compatible.
//
// Version 1: initial — cursor + watchlist co-located.
const CurrentVersion = 1

// State is the persisted runtime state of the Indexer. It is the SINGLE
// source of durable progress information: cursor advancement, last
// published message, and the dynamic watchlist of escrow contracts
// discovered through tw_init events.
//
// State is serialized to disk as JSON. Field tags use snake_case to align
// with the wire envelope contract. The struct is intentionally a flat
// record (no nested pointers) so it can be marshaled and copied cheaply.
//
// Invariant maintained by the Store: after a successful Save, the
// LastLedgerSeq accurately represents the most recent ledger for which
// sink.Write returned without error, AND EscrowContracts contains every
// contract_id discovered from tw_init events in ledgers ≤ LastLedgerSeq.
// Code paths that update one must also update the other in the same Save.
type State struct {
	// Version is the schema version of this State record. Always equal
	// to CurrentVersion at write time; may be ≤ CurrentVersion at read
	// time, in which case future migration logic kicks in (not needed
	// today since we only have v1).
	Version int `json:"version"`

	// Network identifies which Stellar network this state was built
	// against. Compared against the live configured network at boot;
	// a mismatch fails fast with ErrStateNetworkMismatch.
	Network string `json:"network"`

	// SchemaVersion is the envelope schema version used by the Indexer
	// that wrote this state. Currently informational; will be used when
	// envelope evolution requires coordinated handling.
	SchemaVersion string `json:"schema_version"`

	// LastLedgerSeq is the sequence number of the most recent ledger
	// whose events were successfully published to the sink. Zero means
	// "nothing processed yet" — boot semantics defer to START_LEDGER.
	LastLedgerSeq uint32 `json:"last_ledger_seq"`

	// LastMessageID is the MessageID of the last envelope published.
	// Useful for cross-checking with downstream consumers ("did you
	// receive this?"). May be empty if the last processed ledger
	// contained no events.
	LastMessageID string `json:"last_message_id"`

	// LastPublishedAt is the wall-clock time at which the last envelope
	// was published. Observability only — never used for logic.
	LastPublishedAt time.Time `json:"last_published_at"`

	// EscrowContracts is the persisted watchlist: contract addresses of
	// every TW escrow discovered so far. Stored sorted ascending for
	// stable diffs across restarts. In-memory operations go through the
	// Watchlist type (see watchlist.go); this slice is the snapshot used
	// for serialization only.
	EscrowContracts []string `json:"escrow_contracts"`
}

// NewState constructs an empty State for a network. Used at first boot
// when no state file exists.
func NewState(network, schemaVersion string) State {
	return State{
		Version:         CurrentVersion,
		Network:         network,
		SchemaVersion:   schemaVersion,
		EscrowContracts: []string{},
	}
}

// WithCursor returns a copy of s with the cursor fields advanced. The
// receiver is unchanged. Watchlist fields are preserved.
//
// This is the canonical way to advance the cursor: the main loop builds a
// new State value with WithCursor + (optional) WithWatchlist, then hands it
// to Store.Save. Using value semantics avoids accidental partial updates.
func (s State) WithCursor(ledgerSeq uint32, lastMessageID string, publishedAt time.Time) State {
	out := s
	out.LastLedgerSeq = ledgerSeq
	out.LastMessageID = lastMessageID
	out.LastPublishedAt = publishedAt
	return out
}

// WithWatchlist returns a copy of s with the EscrowContracts slice replaced.
// The slice is expected to be sorted (the Watchlist type produces sorted
// snapshots); the Store does not re-sort.
func (s State) WithWatchlist(escrowContracts []string) State {
	out := s
	// Defensive copy to avoid aliasing — callers may keep modifying the
	// original slice after passing it in.
	out.EscrowContracts = append([]string(nil), escrowContracts...)
	return out
}
