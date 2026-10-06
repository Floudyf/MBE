package v5

import (
	"fmt"
	"testing"

	statepkg "metaverse-chainlab/executor/realism/state"
)

func porygonV38TruthRuntime(nodeID, storagePartition string) *NodeRuntime {
	return &NodeRuntime{
		node:    NodePlan{NodeID: nodeID, ShardID: "porygon-global", ExecutionShardID: storagePartition},
		plugins: RuntimePlugins{StateStorage: porygonPartitionStateStore{}},
		pluginSnapshot: map[string]PluginConfig{
			"block_executor": {PluginID: porygonBlockExecutorID, Config: map[string]any{"execution_shard_count": 2}},
		},
		runtimeMetricCounts: map[string]int64{},
	}
}

func porygonV38InstallPaperState(r *NodeRuntime, state *porygonPaperRuntimeState, pipeline *porygonPipelineRuntime) {
	porygonPaperRuntimeStates.Store(r, state)
	porygonPipelineRuntimes.Store(r, pipeline)
}

func porygonV38CleanupRuntime(r *NodeRuntime) {
	porygonPaperRuntimeStates.Delete(r)
	porygonPipelineRuntimes.Delete(r)
}

func TestPorygonV38LateDurableCannotSplitHistoricalProposalTByStorageRole(t *testing.T) {
	s0h3 := map[string]string{"s0/a": "v3"}
	s1h3 := map[string]string{"s1/b": "v3"}
	s0h4 := map[string]string{"s0/a": "v4"}
	s1h4 := map[string]string{"s1/b": "v4"}
	roots3 := map[string]string{"s0": statepkg.RootOfSnapshot(s0h3), "s1": statepkg.RootOfSnapshot(s1h3)}
	roots4 := map[string]string{"s0": statepkg.RootOfSnapshot(s0h4), "s1": statepkg.RootOfSnapshot(s1h4)}
	wantT3 := stableJSONDigest(roots3)

	var gotRoot string
	var gotPartitions string
	for i := 0; i < 8; i++ {
		partition := "s0"
		localH3 := s0h3
		if i >= 4 {
			partition = "s1"
			localH3 = s1h3
		}
		r := porygonV38TruthRuntime(fmt.Sprintf("n%d", i), partition)
		state := &porygonPaperRuntimeState{
			baselineHeight: 0,
			txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
			certifiedSnapshots: map[uint64]map[string]string{
				3: copyRegistryStringMap(localH3),
				4: copyRegistryStringMap(map[string]string{partition + "/future": "v4"}),
			},
			certifiedPartitionRoots: map[uint64]map[string]string{3: copyRegistryStringMap(roots3), 4: copyRegistryStringMap(roots4)},
			certifiedRoots:          map[uint64]string{3: wantT3, 4: stableJSONDigest(roots4)},
			latestCertifiedHeight:   4,
		}
		pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 4, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
		porygonV38InstallPaperState(r, state, pipeline)

		before3 := stableJSONDigest(state.certifiedPartitionRoots[3])
		before4 := stableJSONDigest(state.certifiedPartitionRoots[4])
		if err := r.porygonPaperValidateDurableSnapshot(3, localH3); err != nil {
			t.Fatalf("node %d late D(B3) validation failed: %v", i, err)
		}
		if stableJSONDigest(state.certifiedPartitionRoots[3]) != before3 || stableJSONDigest(state.certifiedPartitionRoots[4]) != before4 || state.latestCertifiedHeight != 4 {
			t.Fatalf("node %d late D(B3) rewrote certified consensus truth", i)
		}
		h, root, partitions := r.porygonPaperStateAnchorForProposal(5)
		if h != 3 || root != wantT3 || stableJSONDigest(partitions) != stableJSONDigest(roots3) {
			t.Fatalf("node %d Proposal.T(3) drifted: height=%d root=%s partitions=%v", i, h, root, partitions)
		}
		if i == 0 {
			gotRoot = root
			gotPartitions = stableJSONDigest(partitions)
		} else if root != gotRoot || stableJSONDigest(partitions) != gotPartitions {
			t.Fatalf("fixed Storage Role split reappeared at node %d", i)
		}
		porygonV38CleanupRuntime(r)
	}
}

func TestPorygonV38CertifiedDeltaFoldsOnExactPreviousHeightAndIsImmutable(t *testing.T) {
	r := porygonV38TruthRuntime("n0", "s0")
	defer porygonV38CleanupRuntime(r)
	base := map[string]string{"s0/a": "v1"}
	remote := map[string]string{"s1/b": "v1"}
	baseRoots := map[string]string{"s0": statepkg.RootOfSnapshot(base), "s1": statepkg.RootOfSnapshot(remote)}
	state := &porygonPaperRuntimeState{
		baselineHeight: 0,
		txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
		certifiedSnapshots:      map[uint64]map[string]string{1: copyRegistryStringMap(base)},
		certifiedPartitionRoots: map[uint64]map[string]string{1: copyRegistryStringMap(baseRoots)},
		certifiedRoots:          map[uint64]string{1: stableJSONDigest(baseRoots)},
		latestCertifiedHeight:   1,
	}
	porygonV38InstallPaperState(r, state, &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 1, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}})

	wantLocalSnapshot := map[string]string{"s0/a": "v2"}
	observed := map[string]string{"s0": statepkg.RootOfSnapshot(wantLocalSnapshot), "s1": baseRoots["s1"]}
	gotSnapshot, gotRoot, err := r.porygonPaperRecordCertifiedDelta(2, []statepkg.StateKV{{Key: "s0/a", Value: "v2"}}, observed)
	if err != nil {
		t.Fatal(err)
	}
	if statepkg.RootOfSnapshot(gotSnapshot) != observed["s0"] || gotRoot != stableJSONDigest(observed) {
		t.Fatalf("certified h=2 state did not fold from exact h=1 canonical base")
	}
	conflict := copyRegistryStringMap(observed)
	conflict["s1"] = "conflicting-root"
	if _, _, err := r.porygonPaperRecordCertifiedDelta(2, []statepkg.StateKV{{Key: "s0/a", Value: "v2"}}, conflict); err == nil {
		t.Fatal("same-height certified root equivocation was accepted")
	}
}

func TestPorygonV38BackupProposalValidationWaitsForRequiredExecutionHeight(t *testing.T) {
	r := porygonV38TruthRuntime("n4", "s1")
	defer porygonV38CleanupRuntime(r)
	roots3 := map[string]string{"s0": "root-s0-h3", "s1": "root-s1-h3"}
	state := &porygonPaperRuntimeState{
		baselineHeight: 0,
		txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
		certifiedSnapshots:      map[uint64]map[string]string{3: {}},
		certifiedPartitionRoots: map[uint64]map[string]string{3: roots3},
		certifiedRoots:          map[uint64]string{3: stableJSONDigest(roots3)},
		latestCertifiedHeight:   3,
	}
	pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 2, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
	porygonV38InstallPaperState(r, state, pipeline)
	if ready, _ := r.porygonPaperProposalValidationReady(5); ready {
		t.Fatal("backup accepted B5 semantic verification before E(B3) was locally ready")
	}
	pipeline.mu.Lock()
	pipeline.executedHeight = 3
	pipeline.mu.Unlock()
	if ready, _ := r.porygonPaperProposalValidationReady(5); ready {
		t.Fatal("backup accepted B5 when only the executed-height cursor advanced without the certified B3 execution artifact")
	}
	ctxKey := porygonV392KeyForShard(t, "v38-ready-ctx", 1, 2)
	itxKey := porygonV392KeyForShard(t, "v38-ready-itx", 0, 2)
	origin, executed := porygonV41CertifiedOriginBlock(t, 3, ctxKey, itxKey)
	porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineExecuted)
	if ready, reason := r.porygonPaperProposalValidationReady(5); !ready {
		t.Fatalf("backup remained blocked after the certified B3 execution artifact became ready: %s", reason)
	}
}
