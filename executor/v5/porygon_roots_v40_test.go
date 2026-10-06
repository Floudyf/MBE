package v5

import (
	"fmt"
	"sort"
	"testing"

	statepkg "metaverse-chainlab/executor/realism/state"
)

type porygonV40Harness struct {
	runtime   *NodeRuntime
	partition string
}

func porygonV40FixedNodes() []NodePlan {
	nodes := make([]NodePlan, 0, 8)
	for i := 0; i < 8; i++ {
		partition := "s0"
		if i >= 4 {
			partition = "s1"
		}
		nodes = append(nodes, NodePlan{NodeID: fmt.Sprintf("n%d", i), ShardID: "porygon-global", ExecutionShardID: partition})
	}
	return nodes
}

func porygonV40DeltaForPartition(changes map[string]map[string]string, partition string) []statepkg.StateKV {
	rows := changes[partition]
	keys := make([]string, 0, len(rows))
	for key := range rows {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]statepkg.StateKV, 0, len(keys))
	for _, key := range keys {
		out = append(out, statepkg.StateKV{Key: key, Value: rows[key]})
	}
	return out
}

func TestPorygonV40EightValidatorTwoStorageFiveHeightCanonicalStateMachine(t *testing.T) {
	nodes := porygonV40FixedNodes()
	fixed := map[string]string{}
	for _, node := range nodes {
		fixed[node.NodeID] = node.ExecutionShardID
	}
	dynamicDiffersFromFixed := false
	for height := uint64(1); height <= 5; height++ {
		ec := porygonECDescriptorForHeight(nodes, height, "porygon-global", 2, 3)
		for dynamicShard, members := range ec.ExecutionShards {
			for _, member := range members {
				if fixed[member] != "" && fixed[member] != dynamicShard {
					dynamicDiffersFromFixed = true
				}
			}
		}
	}
	if !dynamicDiffersFromFixed {
		t.Fatal("test fixture did not separate dynamic ESC identity from fixed Storage Role identity")
	}

	s0Logical := porygonV392KeyForShard(t, "v40-s0", 0, 2)
	s1Logical := porygonV392KeyForShard(t, "v40-s1", 1, 2)
	s0Key := qualifyStateKey("s0", s0Logical)
	s1Key := qualifyStateKey("s1", s1Logical)

	s0State := map[string]string{s0Key: "s0-base"}
	s1State := map[string]string{s1Key: "s1-base"}
	fullRoots := map[string]string{
		"s0": statepkg.RootOfSnapshot(s0State),
		"s1": statepkg.RootOfSnapshot(s1State),
	}

	harnesses := make([]porygonV40Harness, 0, 8)
	for i, node := range nodes {
		partition := node.ExecutionShardID
		local := s0State
		if partition == "s1" {
			local = s1State
		}
		r := porygonV38TruthRuntime(node.NodeID, partition)
		r.plan = Plan{NodeConfigs: nodes}
		state := &porygonPaperRuntimeState{
			baselineHeight:  0,
			txs:             map[string]*PorygonTxRoundLifecycle{},
			proposalUpdates: map[uint64][]PorygonProposalUpdate{},
			certifiedSnapshots: map[uint64]map[string]string{
				0: copyRegistryStringMap(local),
			},
			certifiedPartitionRoots: map[uint64]map[string]string{
				0: copyRegistryStringMap(fullRoots),
			},
			certifiedRoots:        map[uint64]string{0: stableJSONDigest(fullRoots)},
			latestCertifiedHeight: 0,
		}
		pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 0, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
		porygonV38InstallPaperState(r, state, pipeline)
		harnesses = append(harnesses, porygonV40Harness{runtime: r, partition: partition})
		_ = i
	}
	defer func() {
		for _, h := range harnesses {
			porygonV38CleanupRuntime(h.runtime)
		}
	}()

	scenarios := []struct {
		name    string
		changes map[string]map[string]string
	}{
		{name: "only_s0", changes: map[string]map[string]string{"s0": {s0Key: "s0-h1"}}},
		{name: "only_s1", changes: map[string]map[string]string{"s1": {s1Key: "s1-h2"}}},
		{name: "both", changes: map[string]map[string]string{"s0": {s0Key: "s0-h3"}, "s1": {s1Key: "s1-h3"}}},
		{name: "maintenance_no_state_change", changes: map[string]map[string]string{}},
		{name: "cross_height_overwrite", changes: map[string]map[string]string{"s0": {s0Key: "s0-h1"}}},
	}

	for index, scenario := range scenarios {
		height := uint64(index + 1)
		previousRoots := copyRegistryStringMap(fullRoots)
		for key, value := range scenario.changes["s0"] {
			s0State[key] = value
		}
		for key, value := range scenario.changes["s1"] {
			s1State[key] = value
		}
		if len(scenario.changes["s0"]) > 0 {
			fullRoots["s0"] = statepkg.RootOfSnapshot(s0State)
		}
		if len(scenario.changes["s1"]) > 0 {
			fullRoots["s1"] = statepkg.RootOfSnapshot(s1State)
		}
		observedChangedRoots := map[string]string{}
		for partition := range scenario.changes {
			observedChangedRoots[partition] = fullRoots[partition]
		}
		wantGlobal := stableJSONDigest(fullRoots)
		var firstRootSetDigest string
		for nodeIndex, harness := range harnesses {
			delta := porygonV40DeltaForPartition(scenario.changes, harness.partition)
			gotSnapshot, gotGlobal, err := harness.runtime.porygonPaperRecordCertifiedDelta(height, delta, observedChangedRoots)
			if err != nil {
				t.Fatalf("height=%d scenario=%s node=%d partition=%s certification failed: %v", height, scenario.name, nodeIndex, harness.partition, err)
			}
			if gotGlobal != wantGlobal {
				t.Fatalf("height=%d scenario=%s node=%d global root=%s want=%s", height, scenario.name, nodeIndex, gotGlobal, wantGlobal)
			}
			merged := harness.runtime.porygonPaperCertifiedPartitionRootsAtHeight(height)
			if stableJSONDigest(merged) != stableJSONDigest(fullRoots) {
				t.Fatalf("height=%d scenario=%s node=%d incomplete merged roots: got=%v want=%v", height, scenario.name, nodeIndex, merged, fullRoots)
			}
			localRoot, ok := harness.runtime.porygonPaperLocalCertifiedRootAtHeight(height)
			if !ok || localRoot != fullRoots[harness.partition] {
				t.Fatalf("height=%d scenario=%s node=%d local canonical root=%q ok=%t want=%q", height, scenario.name, nodeIndex, localRoot, ok, fullRoots[harness.partition])
			}
			if statepkg.RootOfSnapshot(gotSnapshot) != fullRoots[harness.partition] {
				t.Fatalf("height=%d scenario=%s node=%d local snapshot did not match fixed Storage partition root", height, scenario.name, nodeIndex)
			}
			if len(scenario.changes[harness.partition]) == 0 && merged[harness.partition] != previousRoots[harness.partition] {
				t.Fatalf("height=%d scenario=%s node=%d untouched partition %s did not carry forward", height, scenario.name, nodeIndex, harness.partition)
			}
			anchorHeight, anchorRoot, anchorPartitions := harness.runtime.porygonPaperStateAnchorForProposal(height + 2)
			if anchorHeight != height || anchorRoot != wantGlobal || stableJSONDigest(anchorPartitions) != stableJSONDigest(fullRoots) {
				t.Fatalf("height=%d scenario=%s node=%d future Proposal.T did not consume complete canonical height state", height, scenario.name, nodeIndex)
			}
			if err := harness.runtime.porygonPaperValidateDurableSnapshot(height, gotSnapshot); err != nil {
				t.Fatalf("height=%d scenario=%s node=%d durable validation failed: %v", height, scenario.name, nodeIndex, err)
			}
			digest := stableJSONDigest(merged)
			if nodeIndex == 0 {
				firstRootSetDigest = digest
			} else if digest != firstRootSetDigest {
				t.Fatalf("height=%d scenario=%s fixed Storage replicas diverged on complete partition-root set", height, scenario.name)
			}
		}
	}
}
