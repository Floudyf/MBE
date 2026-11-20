package v5

import (
	"os"
	"strings"
	"testing"
)

func TestMTCompactFixV2ParentDurableAndBusyCases(t *testing.T) {
	if !mtFrontierGateNeedsDeferralV1(28, 26, false) {
		t.Fatal("future h28 must be deferred before h27 commits")
	}
	if !mtFrontierGateNeedsDeferralV1(28, 27, true) {
		t.Fatal("cache publication under commit lock must defer")
	}
	if mtFrontierGateNeedsDeferralV1(28, 27, false) {
		t.Fatal("ready h28 must enter the original validator")
	}
	if mtFrontierGateNeedsDeferralV1(27, 27, true) {
		t.Fatal("committed height is not future")
	}
}

// Guard actual integration location, not just the correctness of the predicate.
func TestMTCompactFixV2CompactHydrationBeforeGateAndFullValidationAfter(t *testing.T) {
	b, err := os.ReadFile("runtime_pbft.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	a := strings.Index(s, "pre = hydratedPre")
	bidx := strings.Index(s, "r.mtGuardHydratedFrontierPrePrepareV2(msg, pre)")
	c := strings.Index(s, "r.validateConsensusBlockBody(pre.Block)")
	d := strings.Index(s, "state.OnPrePrepare(pre)")
	if a < 0 || bidx < a || c < bidx || d < c {
		t.Fatalf("invalid PBFT hydrate/retain/body/vote order: %d %d %d %d", a, bidx, c, d)
	}
}

func TestMTCompactFixV2ReplayRunsAfterCommitLockRelease(t *testing.T) {
	b, err := os.ReadFile("runtime.go")
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	start := strings.Index(s, "func (r *NodeRuntime) commitWithOrigin(")
	end := strings.Index(s, "func (r *NodeRuntime) drainPendingCommits(")
	if start < 0 || end < start {
		t.Fatal("commit function missing")
	}
	part := s[start:end]
	unlock := strings.Index(part, "r.commitMu.Unlock()")
	replay := strings.Index(part, "if replayReady {\n\t\t\tr.replayDeferredPrePrepare(ctx)")
	if unlock < 0 || replay < unlock {
		t.Fatalf("replay must follow commit lock release: unlock=%d replay=%d", unlock, replay)
	}
}
