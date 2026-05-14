package events

// EventKind identifies the semantic category of a detected event. For Soroban
// contract events emitted by TW contracts, the value is the pass-through
// Symbol name (e.g. "tw_init", "tw_fund"). For events detected by the Indexer
// from external sources (e.g. token transfers to escrows), it is a
// namespace-qualified constant (e.g. "token.transfer").
//
// The set is intentionally open: a future TW contract topic can be added
// without changing the type, and downstream consumers are expected to ignore
// unknown kinds gracefully.
type EventKind string

// Soroban contract event Symbols emitted directly by TW escrow contracts.
// These are the values that appear as the first topic of a ContractEvent
// emitted by an escrow contract. Pass-through naming aligns with the
// "filter and forward" principle: the chain-side symbol is the only source
// of truth.
const (
	EventKindTWInit        EventKind = "tw_init"
	EventKindTWFund        EventKind = "tw_fund"
	EventKindTWRelease     EventKind = "tw_release"
	EventKindTWUpdate      EventKind = "tw_update"
	EventKindTWMSChange    EventKind = "tw_ms_change"
	EventKindTWMSApprove   EventKind = "tw_ms_approve"
	EventKindTWMSManage    EventKind = "tw_ms_manage"
	EventKindTWDispResolve EventKind = "tw_disp_resolve"
	EventKindTWDispute     EventKind = "tw_dispute"
	EventKindTWMSDispute   EventKind = "tw_ms_dispute"
	EventKindTWTTLExtend   EventKind = "tw_ttl_extend"
	EventKindTWWithdraw    EventKind = "tw_withdraw"
)

// Indexer-synthesized event kinds. These are NOT emitted by TW contracts;
// the Indexer detects them via heuristics (e.g. SAC transfer events whose
// destination is in the escrow watchlist) and labels them with these
// namespaced names so consumers can route them distinctly.
const (
	// EventKindTokenTransfer is emitted for a SAC or SEP-41 `transfer`
	// event where `to` or `from` is a known escrow contract.
	EventKindTokenTransfer EventKind = "token.transfer"
)

// AllTWTopics returns the canonical set of TW-emitted topic Symbols. This is
// the snapshot of contracts as of 2026-05-13 across the single-release-develop,
// multi-release-develop, feat/single-release-v2 and feat/multi-release-v2
// branches. Not every topic appears in every branch — the union is taken so
// the Indexer is tolerant of running against either version.
//
// Adding a new TW topic requires editing this list and (typically) bumping
// the envelope schema version if the new topic carries fields downstream
// consumers should be aware of.
func AllTWTopics() []EventKind {
	return []EventKind{
		EventKindTWInit,
		EventKindTWFund,
		EventKindTWRelease,
		EventKindTWUpdate,
		EventKindTWMSChange,
		EventKindTWMSApprove,
		EventKindTWMSManage,
		EventKindTWDispResolve,
		EventKindTWDispute,
		EventKindTWMSDispute,
		EventKindTWTTLExtend,
		EventKindTWWithdraw,
	}
}
