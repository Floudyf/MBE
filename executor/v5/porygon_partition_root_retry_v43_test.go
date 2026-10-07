package v5

import (
	"errors"
	"testing"

	statepkg "metaverse-chainlab/executor/realism/state"
)

func TestPorygonV43PartitionRootReadinessDistinguishesLagFromMismatch(t *testing.T) {
	r := porygonV38TruthRuntime("n0", "s0")
	defer porygonV38CleanupRuntime(r)

	tState := map[string]string{"s0/a": "t2"}
	tRoot := statepkg.RootOfSnapshot(tState)
	roots2 := map[string]string{"s0": tRoot, "s1": "remote-t2-root"}
	state := &porygonPaperRuntimeState{
		baselineHeight: 0,
		txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
		certifiedSnapshots:      map[uint64]map[string]string{2: copyRegistryStringMap(tState)},
		certifiedPartitionRoots: map[uint64]map[string]string{2: copyRegistryStringMap(roots2)},
		certifiedRoots:          map[uint64]string{2: stableJSONDigest(roots2)}, latestCertifiedHeight: 2,
	}
	porygonV38InstallPaperState(r, state, &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 2, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}})

	request := PorygonPaperPartitionRootRequest{
		RequestID: "v43-request", RequesterNode: "n5", BlockHash: "b4", Height: 4,
		TStateHeight: 2, PartitionID: "s0", TPartitionRoot: tRoot,
		CanonicalBaseHeight: 3, CanonicalBaseRoot: "expected-h3-root",
		Updates: nil, UpdateDigest: porygonUpdateDigest(nil),
	}
	if _, err := r.buildPorygonPaperPartitionRootAck(request); !errors.Is(err, errPorygonPaperPartitionRootNotReady) {
		t.Fatalf("missing h3 canonical base must be retryable not-ready, got %v", err)
	}

	h3 := map[string]string{"s0/a": "h3"}
	h3Root := statepkg.RootOfSnapshot(h3)
	state.mu.Lock()
	state.certifiedSnapshots[3] = copyRegistryStringMap(h3)
	state.certifiedPartitionRoots[3] = map[string]string{"s0": h3Root, "s1": "remote-h3-root"}
	state.certifiedRoots[3] = stableJSONDigest(state.certifiedPartitionRoots[3])
	state.latestCertifiedHeight = 3
	state.mu.Unlock()

	// Once h3 exists locally, a request bound to a different base is a real
	// deterministic contradiction and must not be classified as retryable lag.
	request.CanonicalBaseRoot = "wrong-h3-root"
	if _, err := r.buildPorygonPaperPartitionRootAck(request); err == nil || errors.Is(err, errPorygonPaperPartitionRootNotReady) {
		t.Fatalf("present-but-conflicting h3 base must fail closed, got %v", err)
	}

	if _, ready, err := r.porygonPaperPartitionSnapshotReadiness(3, "s0", h3Root); err != nil || !ready {
		t.Fatalf("certified h3 snapshot should be ready: ready=%t err=%v", ready, err)
	}
	if _, ready, err := r.porygonPaperPartitionSnapshotReadiness(3, "s0", "wrong-root"); err == nil || !ready {
		t.Fatalf("present h3 snapshot with contradictory root must be ready+fatal, ready=%t err=%v", ready, err)
	}
}
