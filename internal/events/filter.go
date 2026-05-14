package events

import "github.com/stellar/go-stellar-sdk/xdr"

// TopicFilter decides whether a Soroban contract event is of interest by
// looking at its first topic Symbol. The filter is intentionally simple: it
// holds a set of allowed Symbol names and reports membership. State is
// immutable after construction; concurrent reads are safe.
//
// Two-stage filtering is applied at the call site: events that match the
// topic filter are emitted as their own EventKind; the Indexer's transfer
// detection path applies an additional destination-address filter (using
// the watchlist) before emitting a synthesized token.transfer event.
type TopicFilter struct {
	allowed map[string]struct{}
}

// NewTopicFilter builds a TopicFilter from a list of EventKind values
// representing topic Symbol names.
func NewTopicFilter(topics []EventKind) *TopicFilter {
	m := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		m[string(t)] = struct{}{}
	}
	return &TopicFilter{allowed: m}
}

// DefaultTWTopicFilter returns a TopicFilter pre-configured with the canonical
// set of TW topics returned by AllTWTopics. This is the default the Indexer
// uses unless the operator overrides via configuration.
func DefaultTWTopicFilter() *TopicFilter {
	return NewTopicFilter(AllTWTopics())
}

// Matches reports whether the event's first topic Symbol is in the filter's
// allowed set. Returns false (no error) for events with no topics or whose
// first topic is not a Symbol — these are not "of interest" by definition.
func (f *TopicFilter) Matches(ev xdr.ContractEvent) bool {
	sym, ok := FirstTopicSymbol(ev)
	if !ok {
		return false
	}
	_, ok = f.allowed[sym]
	return ok
}

// Size reports how many distinct Symbols are in the filter. Useful for
// metrics and boot-time logging.
func (f *TopicFilter) Size() int {
	return len(f.allowed)
}

// FirstTopicSymbol extracts the first topic of a Soroban contract event as a
// Symbol string. Returns (symbol, true) on success; ("", false) when the
// event has no topics, when its body version is unsupported, or when the
// first topic is not a Symbol.
//
// Exposed for callers that need the Symbol for routing/labeling beyond a
// boolean match (e.g. to set Envelope.EventKind from a matched event).
func FirstTopicSymbol(ev xdr.ContractEvent) (string, bool) {
	// Soroban contract events use Body.V == 0 today. If the protocol ever
	// adds a new body version, we treat it as "not interesting" until the
	// Indexer is updated explicitly.
	if ev.Body.V != 0 {
		return "", false
	}
	body := ev.Body.V0
	if len(body.Topics) == 0 {
		return "", false
	}
	sym, ok := body.Topics[0].GetSym()
	if !ok {
		return "", false
	}
	return string(sym), true
}
