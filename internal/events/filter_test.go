package events

import (
	"testing"

	"github.com/stellar/go-stellar-sdk/xdr"
)

// makeEvent builds a Soroban ContractEvent whose first topic is a Symbol
// with the given name. Helper for filter tests.
func makeEvent(topicSymbol string) xdr.ContractEvent {
	sym := xdr.ScSymbol(topicSymbol)
	topic := xdr.ScVal{
		Type: xdr.ScValTypeScvSymbol,
		Sym:  &sym,
	}
	return xdr.ContractEvent{
		Type: xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec{topic},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
}

// makeEventNoTopics builds an event whose body has zero topics.
func makeEventNoTopics() xdr.ContractEvent {
	return xdr.ContractEvent{
		Type: xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec{},
				Data:   xdr.ScVal{Type: xdr.ScValTypeScvVoid},
			},
		},
	}
}

// makeEventNonSymbolTopic builds an event whose first topic is not a Symbol.
func makeEventNonSymbolTopic() xdr.ContractEvent {
	i := xdr.Uint32(42)
	topic := xdr.ScVal{
		Type: xdr.ScValTypeScvU32,
		U32:  &i,
	}
	return xdr.ContractEvent{
		Type: xdr.ContractEventTypeContract,
		Body: xdr.ContractEventBody{
			V: 0,
			V0: &xdr.ContractEventV0{
				Topics: xdr.ScVec{topic},
			},
		},
	}
}

func TestTopicFilter_MatchesKnownTopic(t *testing.T) {
	f := DefaultTWTopicFilter()
	for _, topic := range AllTWTopics() {
		ev := makeEvent(string(topic))
		if !f.Matches(ev) {
			t.Errorf("DefaultTWTopicFilter must match %q", topic)
		}
	}
}

func TestTopicFilter_RejectsUnknownTopic(t *testing.T) {
	f := DefaultTWTopicFilter()
	cases := []string{"", "transfer", "mint", "approve", "unrelated_topic"}
	for _, c := range cases {
		ev := makeEvent(c)
		if f.Matches(ev) {
			t.Errorf("DefaultTWTopicFilter must NOT match %q", c)
		}
	}
}

func TestTopicFilter_RejectsEventWithNoTopics(t *testing.T) {
	f := DefaultTWTopicFilter()
	if f.Matches(makeEventNoTopics()) {
		t.Fatal("filter must reject event with zero topics")
	}
}

func TestTopicFilter_RejectsNonSymbolFirstTopic(t *testing.T) {
	f := DefaultTWTopicFilter()
	if f.Matches(makeEventNonSymbolTopic()) {
		t.Fatal("filter must reject event whose first topic is not a Symbol")
	}
}

func TestTopicFilter_EmptyAllowedNeverMatches(t *testing.T) {
	f := NewTopicFilter(nil)
	for _, topic := range AllTWTopics() {
		if f.Matches(makeEvent(string(topic))) {
			t.Fatalf("empty filter must never match; matched %q", topic)
		}
	}
}

func TestTopicFilter_Size(t *testing.T) {
	if got := DefaultTWTopicFilter().Size(); got != 12 {
		t.Fatalf("expected default filter size 12 (canonical TW topics); got %d", got)
	}
}

func TestFirstTopicSymbol_HappyPath(t *testing.T) {
	ev := makeEvent("tw_init")
	sym, ok := FirstTopicSymbol(ev)
	if !ok {
		t.Fatal("expected ok=true for a well-formed event")
	}
	if sym != "tw_init" {
		t.Fatalf("expected 'tw_init'; got %q", sym)
	}
}

func TestFirstTopicSymbol_RejectsFutureBodyVersion(t *testing.T) {
	// If Soroban adds a body version we don't support, FirstTopicSymbol
	// must return ok=false so events are skipped (not panic, not silently
	// match the wrong topic).
	ev := makeEvent("tw_init")
	ev.Body.V = 99
	if _, ok := FirstTopicSymbol(ev); ok {
		t.Fatal("expected ok=false for unsupported body version")
	}
}
