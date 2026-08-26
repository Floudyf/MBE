package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func cgCycleFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 11, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessRead}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessRead}, {Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestCGCycleAwarePlanDetectsAndResolvesCycleDeterministically(t *testing.T) {
	block := cgCycleFixture()
	first, err := buildCGPlanWithWorkers(block, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildCGPlanWithWorkers(block, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.AbortedTransactionIDs) == 0 {
		t.Fatalf("cycle-aware CG must abort at least one cycle victim")
	}
	if stableDigest(first.AbortedTransactionIDs) != stableDigest(second.AbortedTransactionIDs) || stableDigest(first.Waves) != stableDigest(second.Waves) {
		t.Fatalf("cycle resolution must be deterministic: first=%+v second=%+v", first, second)
	}
	if first.Metrics.AbortCount != len(first.AbortedTransactionIDs) {
		t.Fatalf("abort metric mismatch: metrics=%d ids=%v", first.Metrics.AbortCount, first.AbortedTransactionIDs)
	}
	bound := block
	bindCGPlanForTest(t, &bound, first)
	if err := verifyCGPlanSmart(bound, first, 1); err != nil {
		t.Fatalf("validator-local worker count must not change CG semantic verification: %v", err)
	}
	covered := map[string]bool{}
	for _, wave := range first.Waves {
		for _, id := range wave {
			covered[id] = true
		}
	}
	for _, id := range first.AbortedTransactionIDs {
		if covered[id] {
			t.Fatalf("aborted transaction also appeared in executable waves: %s", id)
		}
		covered[id] = true
	}
	if len(covered) != len(block.TxList) {
		t.Fatalf("plan coverage=%d want=%d", len(covered), len(block.TxList))
	}
}

func TestCGPlanDigestIgnoresObservabilityTiming(t *testing.T) {
	plan, err := buildCGPlanWithWorkers(cgCycleFixture(), 2)
	if err != nil {
		t.Fatal(err)
	}
	clone := plan
	clone.Metrics.GraphConstructionMS += 12345
	clone.Metrics.SortingMS += 67890
	if literaturePlanDigest(clone) != plan.PlanDigest {
		t.Fatalf("non-semantic phase timing must not alter CG semantic plan digest")
	}
}

func TestCGCycleVictimUsesOnlyVerticesFromCyclicSCC(t *testing.T) {
	items := []literatureTxAccess{{TxID: "downstream0"}, {TxID: "source1"}, {TxID: "cycle2"}, {TxID: "cycle3"}}
	edges := map[int]map[int]bool{
		1: {2: true},
		2: {3: true},
		3: {2: true, 0: true},
	}
	waves, aborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	if len(aborted) != 1 || aborted[0] != 2 {
		t.Fatalf("cycle victim must be the lowest original ordinal inside a cyclic SCC: got=%v want=[2]", aborted)
	}
	for _, wave := range waves {
		for _, id := range wave {
			if id == "cycle2" {
				t.Fatalf("aborted cyclic transaction appeared in executable waves")
			}
		}
	}
}

func TestCGCycleVictimPolicyIsDeterministicWithoutRuntimeFeedback(t *testing.T) {
	items := []literatureTxAccess{{TxID: "t0"}, {TxID: "t1"}, {TxID: "t2"}, {TxID: "t3"}}
	edges := map[int]map[int]bool{
		0: {1: true},
		1: {2: true, 3: true},
		2: {0: true},
		3: {1: true},
	}
	firstWaves, firstAborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	secondWaves, secondAborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	if stableDigest(firstAborted) != stableDigest(secondAborted) || stableDigest(firstWaves) != stableDigest(secondWaves) {
		t.Fatalf("CG cycle resolution must be deterministic: first=%v/%v second=%v/%v", firstAborted, firstWaves, secondAborted, secondWaves)
	}
	if stableDigest(firstAborted) != stableDigest([]int{0, 1}) {
		t.Fatalf("deterministic cyclic-SCC victim sequence mismatch: got=%v want=[0 1]", firstAborted)
	}
}

func TestCGCycleResolverHandlesDenseSCCWithoutElementaryCycleMaterialization(t *testing.T) {
	const n = 128
	items := make([]literatureTxAccess, n)
	edges := make(map[int]map[int]bool, n)
	for i := 0; i < n; i++ {
		items[i] = literatureTxAccess{TxID: string([]byte{byte(i + 1)})}
		edges[i] = make(map[int]bool, n-1)
		for j := 0; j < n; j++ {
			if i != j {
				edges[i][j] = true
			}
		}
	}
	waves, aborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	if len(aborted) != n-1 {
		t.Fatalf("dense SCC abort count=%d want=%d", len(aborted), n-1)
	}
	for i := 0; i < n-1; i++ {
		if aborted[i] != i {
			t.Fatalf("dense SCC deterministic victim[%d]=%d want=%d", i, aborted[i], i)
		}
	}
	if len(waves) != 1 || len(waves[0]) != 1 {
		t.Fatalf("dense SCC surviving wave mismatch: %v", waves)
	}
}
