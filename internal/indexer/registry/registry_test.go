package registry

import (
	"strings"
	"testing"
)

var (
	approvedHex = strings.Repeat("ab", 32) // a valid 32-byte hex hash
	otherHex    = strings.Repeat("cd", 32)
)

func TestNew_rejectsMalformedHash(t *testing.T) {
	if _, err := New([]string{"not-hex"}); err == nil {
		t.Fatal("expected error for malformed hash")
	}
	if _, err := New([]string{strings.Repeat("ab", 16)}); err == nil {
		t.Fatal("expected error for wrong-length hash")
	}
	if _, err := New([]string{approvedHex, "", "  "}); err != nil {
		t.Fatalf("blank entries should be skipped, got %v", err)
	}
}

func TestRegister_onlyApprovedHashes(t *testing.T) {
	r, err := New([]string{approvedHex})
	if err != nil {
		t.Fatal(err)
	}

	approved, _ := ParseHash(approvedHex)
	other, _ := ParseHash(otherHex)

	if !r.Register("CESCROW", approved) {
		t.Fatal("expected approved-hash contract to be registered")
	}
	if r.Register("CESCROW", approved) {
		t.Fatal("re-registering the same contract should return false")
	}
	if r.Register("CSTRANGER", other) {
		t.Fatal("non-approved hash must not be registered")
	}

	if !r.IsEscrow("CESCROW") {
		t.Fatal("CESCROW should be a known escrow")
	}
	if r.IsEscrow("CSTRANGER") {
		t.Fatal("CSTRANGER should not be a known escrow")
	}
	if r.IsEscrow("") {
		t.Fatal("empty contract id is never an escrow")
	}
	if r.Size() != 1 {
		t.Fatalf("expected size 1, got %d", r.Size())
	}
}

func TestSeed_bypassesHashCheck(t *testing.T) {
	r, err := New(nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Seed([]string{"CSEEDED", "", "CSEEDED2"})
	if !r.IsEscrow("CSEEDED") || !r.IsEscrow("CSEEDED2") {
		t.Fatal("seeded contracts should be known escrows")
	}
	if r.Size() != 2 {
		t.Fatalf("expected size 2, got %d", r.Size())
	}
}
