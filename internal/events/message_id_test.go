package events

import "testing"

func TestNewMessageID_Deterministic(t *testing.T) {
	a := NewMessageID("deadbeef", 3)
	b := NewMessageID("deadbeef", 3)
	if a != b {
		t.Fatalf("MessageID must be deterministic; got %q vs %q", a, b)
	}
}

func TestNewMessageID_DistinguishesIndex(t *testing.T) {
	a := NewMessageID("deadbeef", 3)
	b := NewMessageID("deadbeef", 4)
	if a == b {
		t.Fatal("MessageID must differ for different event indices in the same tx")
	}
}

func TestNewMessageID_DistinguishesTx(t *testing.T) {
	a := NewMessageID("deadbeef", 0)
	b := NewMessageID("cafebabe", 0)
	if a == b {
		t.Fatal("MessageID must differ for different transactions")
	}
}

func TestNewMessageID_HumanReadable(t *testing.T) {
	// The plaintext format is part of the contract — keeps logs readable.
	got := NewMessageID("abc", 7)
	want := "abc-7"
	if got != want {
		t.Fatalf("expected %q; got %q", want, got)
	}
}

func TestNewMessageID_HandlesEmptyHash(t *testing.T) {
	// Empty hash is invalid in practice but the function must not panic;
	// validation belongs at the Envelope level, not in the ID builder.
	got := NewMessageID("", 0)
	if got != "-0" {
		t.Fatalf("expected %q; got %q", "-0", got)
	}
}
