package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/state"
)

func TestV14AOverlayCommitmentMatchesFullSnapshot(t *testing.T) {
	base := map[string]string{
		"s0::counter": "10",
		"s0::plain":   "old",
		"s0::cas":     "before",
		"s0::noop":    "keep",
	}
	baseCommitment := state.NewCommitment(base)
	baseRoot := baseCommitment.Root()

	updates := []state.StateKV{
		{Key: "counter", UpdateSemantics: "commutative_delta", Delta: 3},
		{Key: "plain", Value: "new"},
		{Key: "cas", Value: "after", BaseValueDigest: stateValueDigest("before")},
		{Key: "noop", Value: "ignored", OrderingNoop: true},
	}
	got, commitment, err := applyStateDeltaToSnapshotWithCommitment(base, baseCommitment, updates, "s0", 7)
	if err != nil {
		t.Fatal(err)
	}
	if commitment == nil {
		t.Fatal("expected cloned overlay commitment")
	}
	if got["s0::counter"] != "13" || got["s0::plain"] != "new" || got["s0::cas"] != "after" || got["s0::noop"] != "keep" {
		t.Fatalf("unexpected overlay snapshot: %#v", got)
	}
	if want := state.RootOfSnapshot(got); commitment.Root() != want {
		t.Fatalf("overlay commitment mismatch: got %s want %s", commitment.Root(), want)
	}
	if baseCommitment.Root() != baseRoot {
		t.Fatalf("base commitment mutated: got %s want %s", baseCommitment.Root(), baseRoot)
	}
}

func TestV14AOverlayCommitmentHandlesInitialCommutativeValue(t *testing.T) {
	base := map[string]string{"s0::stable": "x"}
	baseCommitment := state.NewCommitment(base)
	updates := []state.StateKV{
		{Key: "fresh", UpdateSemantics: "commutative_delta", Delta: 4, HasInitialValue: true, InitialValue: 11},
	}
	got, commitment, err := applyStateDeltaToSnapshotWithCommitment(base, baseCommitment, updates, "s0", 8)
	if err != nil {
		t.Fatal(err)
	}
	if got["s0::fresh"] != "15" {
		t.Fatalf("initial commutative value mismatch: %#v", got)
	}
	if want := state.RootOfSnapshot(got); commitment.Root() != want {
		t.Fatalf("initial-value commitment mismatch: got %s want %s", commitment.Root(), want)
	}
}

func TestV14AOverlayCommitmentCASFailureLeavesBaseUnchanged(t *testing.T) {
	base := map[string]string{"s0::cas": "before"}
	baseCommitment := state.NewCommitment(base)
	baseRoot := baseCommitment.Root()
	updates := []state.StateKV{
		{Key: "cas", Value: "after", BaseValueDigest: stateValueDigest("wrong")},
	}
	got, commitment, err := applyStateDeltaToSnapshotWithCommitment(base, baseCommitment, updates, "s0", 9)
	if err == nil {
		t.Fatal("expected CAS mismatch")
	}
	if got != nil || commitment != nil {
		t.Fatalf("failed overlay must not return partial materialization: got=%#v commitment=%v", got, commitment)
	}
	if baseCommitment.Root() != baseRoot {
		t.Fatalf("base commitment mutated on failed overlay: got %s want %s", baseCommitment.Root(), baseRoot)
	}
}

func TestV14AOverlayCommitmentNilFallbackPreservesLegacyStoragePath(t *testing.T) {
	base := map[string]string{"s0::plain": "old"}
	updates := []state.StateKV{{Key: "plain", Value: "new"}}
	got, commitment, err := applyStateDeltaToSnapshotWithCommitment(base, nil, updates, "s0", 10)
	if err != nil {
		t.Fatal(err)
	}
	if commitment != nil {
		t.Fatal("storage without authenticated snapshotter must preserve nil commitment fallback")
	}
	if got["s0::plain"] != "new" {
		t.Fatalf("legacy fallback snapshot mismatch: %#v", got)
	}
}

func TestV14ANoOverlayClonesMatchingCommitment(t *testing.T) {
	base := map[string]string{"s0::a": "1"}
	baseCommitment := state.NewCommitment(base)
	got, commitment, err := applyStateDeltaToSnapshotWithCommitment(base, baseCommitment, nil, "s0", 11)
	if err != nil {
		t.Fatal(err)
	}
	if commitment == nil || commitment.Root() != baseCommitment.Root() {
		t.Fatal("no-overlay path lost matching authenticated commitment")
	}
	if got["s0::a"] != "1" {
		t.Fatalf("no-overlay snapshot changed: %#v", got)
	}
	commitment.Set("s0::a", "2")
	if baseCommitment.Root() == commitment.Root() {
		t.Fatal("commitment clone is not path-copy isolated")
	}
}
