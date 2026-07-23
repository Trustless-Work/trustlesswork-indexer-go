package processors

import (
	"testing"

	"github.com/Trustless-Work/Indexer/internal/indexer/registry"
)

// seededRegistry builds a registry tracking the given ids (via Seed, so
// no wasm-hash approval is involved).
func seededRegistry(t *testing.T, ids ...string) *registry.Registry {
	t.Helper()
	reg, err := registry.New(nil)
	if err != nil {
		t.Fatal(err)
	}
	reg.Seed(ids)
	return reg
}

func TestSweeper_RotatesInBudgetedBatches(t *testing.T) {
	reg := seededRegistry(t, "CA", "CB", "CC", "CD", "CE")
	s := NewSweeper(reg)
	flatCost := func(string) int { return 4 } // nothing learned yet

	// Budget of 8 keys = 2 escrows per batch at cost 4.
	var passes [][]string
	for i := range 3 {
		batch, done := s.NextBatch(100, flatCost, 8)
		passes = append(passes, batch)
		if i < 2 && done {
			t.Fatalf("pass reported complete too early at batch %d", i)
		}
		if i == 2 && !done {
			t.Fatal("third batch should complete the pass over 5 escrows")
		}
	}
	if len(passes[0]) != 2 || len(passes[1]) != 2 || len(passes[2]) != 1 {
		t.Fatalf("batch sizes = %d/%d/%d, want 2/2/1", len(passes[0]), len(passes[1]), len(passes[2]))
	}
	// Every escrow appears exactly once across the pass.
	seen := map[string]int{}
	for _, p := range passes {
		for _, id := range p {
			seen[id]++
		}
	}
	if len(seen) != 5 {
		t.Fatalf("coverage = %v, want all 5 exactly once", seen)
	}
}

func TestSweeper_OversizedSingleEscrowStillProgresses(t *testing.T) {
	reg := seededRegistry(t, "CA", "CB")
	s := NewSweeper(reg)
	huge := func(string) int { return 1000 } // cost alone exceeds any budget

	batch, _ := s.NextBatch(1, huge, 200)
	if len(batch) != 1 {
		t.Fatalf("batch = %v, want exactly one id despite the oversized cost", batch)
	}
}

func TestSweeper_ModifiedSinceLagsOnePass(t *testing.T) {
	reg := seededRegistry(t, "CA")
	s := NewSweeper(reg)
	one := func(string) int { return 1 }

	// First pass: no previous pass, so everything must be reported.
	if _, done := s.NextBatch(100, one, 200); !done {
		t.Fatal("single-escrow pass should complete in one batch")
	}
	if s.ModifiedSince() != 0 {
		t.Fatalf("first pass ModifiedSince = %d, want 0 (full reconciliation)", s.ModifiedSince())
	}

	// Second pass: filter anchors to where the FIRST pass started.
	if _, done := s.NextBatch(150, one, 200); !done {
		t.Fatal("second pass should complete in one batch")
	}
	if s.ModifiedSince() != 100 {
		t.Fatalf("second pass ModifiedSince = %d, want 100", s.ModifiedSince())
	}
}

func TestFilterUnchanged(t *testing.T) {
	changes := []EscrowStateChange{
		{EscrowID: "CA", StateChangeType: "updated", LastModifiedLedger: 90},
		{EscrowID: "CB", StateChangeType: "updated", LastModifiedLedger: 110},
		{EscrowID: "CC", StateChangeType: "removed"}, // no modification ledger
	}

	out := filterUnchanged(changes, 100)
	if len(out) != 2 {
		t.Fatalf("filtered = %+v, want CB (fresh) and CC (removed)", out)
	}
	if out[0].EscrowID != "CB" || out[1].EscrowID != "CC" {
		t.Fatalf("filtered = %+v, want [CB, CC]", out)
	}

	// Zero disables the filter entirely (activity path + first pass).
	if got := filterUnchanged(changes, 0); len(got) != 3 {
		t.Fatalf("modifiedSince=0 must pass everything; got %d", len(got))
	}
}

func TestKeyCost_DropsAfterLearning(t *testing.T) {
	reg := seededRegistry(t, "CA")
	d := NewEscrowStateDetector(nil, reg)

	if got := d.KeyCost("CA"); got != candidateKeysPerEscrow {
		t.Fatalf("unlearned KeyCost = %d, want %d", got, candidateKeysPerEscrow)
	}
	d.learned["CA"] = "BASE64KEY"
	if got := d.KeyCost("CA"); got != 1 {
		t.Fatalf("learned KeyCost = %d, want 1", got)
	}

	// A miss forgets the shortcut so the next query uses full candidates.
	relearn := d.forgetLearnedAmong([]string{"CA", "CB"})
	if len(relearn) != 1 || relearn[0] != "CA" {
		t.Fatalf("forgetLearnedAmong = %v, want [CA]", relearn)
	}
	if got := d.KeyCost("CA"); got != candidateKeysPerEscrow {
		t.Fatalf("KeyCost after forget = %d, want %d", got, candidateKeysPerEscrow)
	}
}

func TestMissingFrom(t *testing.T) {
	present := []EscrowStateChange{{EscrowID: "CA"}, {EscrowID: "CB"}}
	missing := missingFrom([]string{"CA", "CB", "CC"}, present)
	if len(missing) != 1 || missing[0] != "CC" {
		t.Fatalf("missing = %v, want [CC]", missing)
	}
}
