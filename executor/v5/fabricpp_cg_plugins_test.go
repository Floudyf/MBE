package v5

import (
	"encoding/json"
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func fabricPPTx(id string, access ...tx.AccessItem) tx.SignedTransaction {
	return tx.SignedTransaction{TxID: id, AccessListSchema: "direct_v1", AccessListSource: "fabricpp_fixture", AccessList: access}
}

func TestFabricPPCGUsesPaperReadWriteConflictOnly(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 1, BlockHash: "fabricpp-no-ww", TxList: []tx.SignedTransaction{
		fabricPPTx("w0", tx.AccessItem{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}),
		fabricPPTx("w1", tx.AccessItem{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}),
	}}
	plan, err := buildFabricPPCGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edges) != 0 {
		t.Fatalf("Fabric++ paper graph must not add standalone WW edges: %+v", plan.Edges)
	}
	if len(plan.Waves) != 2 || len(plan.Waves[0]) != 1 || len(plan.Waves[1]) != 1 {
		t.Fatalf("Fabric++ paper schedule must be encoded as a serial singleton-wave order: %+v", plan.Waves)
	}
	if len(plan.SerializationOrder) != 2 || plan.SerializationOrder[0] != "w1" || plan.SerializationOrder[1] != "w0" {
		t.Fatalf("deterministic Algorithm-1 traversal/invert order changed: %+v", plan.SerializationOrder)
	}
}

func TestFabricPPCGPaperEdgeAndSerializableOrderAreOpposite(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 2, BlockHash: "fabricpp-direction", TxList: []tx.SignedTransaction{
		fabricPPTx("writer", tx.AccessItem{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}),
		fabricPPTx("reader", tx.AccessItem{Key: "a", Mode: tx.AccessRead}),
	}}
	plan, err := buildFabricPPCGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edges) != 1 || plan.Edges[0] != (literatureGraphEdge{From: 0, To: 1}) {
		t.Fatalf("paper conflict must be writer->reader, got %+v", plan.Edges)
	}
	if len(plan.SerializationOrder) != 2 || plan.SerializationOrder[0] != "reader" || plan.SerializationOrder[1] != "writer" {
		t.Fatalf("paper requires reader before conflicting writer, got %+v", plan.SerializationOrder)
	}
}

func TestFabricPPCGRMWTwoCycleAbortsLowerOrdinalOnTie(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 3, BlockHash: "fabricpp-rmw-cycle", TxList: []tx.SignedTransaction{
		fabricPPTx("t0", tx.AccessItem{Key: "hot", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}),
		fabricPPTx("t1", tx.AccessItem{Key: "hot", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}),
	}}
	plan, err := buildFabricPPCGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edges) != 2 {
		t.Fatalf("two RMW transactions must create a bidirected RW conflict pair, got %+v", plan.Edges)
	}
	if len(plan.AbortedTransactionIDs) != 1 || plan.AbortedTransactionIDs[0] != "t0" {
		t.Fatalf("equal cycle participation must abort the smaller original ordinal, got %+v", plan.AbortedTransactionIDs)
	}
	if len(plan.SerializationOrder) != 1 || plan.SerializationOrder[0] != "t1" {
		t.Fatalf("unexpected residual serializable schedule: %+v", plan.SerializationOrder)
	}
}

func TestFabricPPCGPlanVerificationRejectsTamperedEdge(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 4, BlockHash: "fabricpp-verify", TxList: []tx.SignedTransaction{
		fabricPPTx("w", tx.AccessItem{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}),
		fabricPPTx("r", tx.AccessItem{Key: "a", Mode: tx.AccessRead}),
	}}
	plan, err := buildFabricPPCGPlan(block)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: fabricPPCGPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	if err := verifyFabricPPCGPlan(block, plan); err != nil {
		t.Fatalf("valid Fabric++ plan rejected: %v", err)
	}

	tampered := plan
	tampered.Edges = nil
	tampered.Metrics.EdgeCount = 0
	tampered = literatureFinalizePlan(tampered)
	raw, err = json.Marshal(tampered)
	if err != nil {
		t.Fatal(err)
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: fabricPPCGPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: tampered.PlanDigest, Payload: raw}
	if err := verifyFabricPPCGPlan(block, tampered); err == nil {
		t.Fatal("Fabric++ validator accepted a plan with a missing paper conflict edge")
	}
}

func TestFabricPPCGRegistryOwnsIndependentPlugins(t *testing.T) {
	registry := BuiltinRegistry()
	for _, item := range []struct{ category, id string }{
		{"execution", fabricPPCGExecutionID},
		{"scheduler", fabricPPCGSchedulerID},
		{"block_executor", fabricPPCGBlockExecutorID},
	} {
		plugin, err := registry.Create(item.category, item.id, map[string]any{})
		if err != nil {
			t.Fatalf("missing Fabric++ plugin %s:%s: %v", item.category, item.id, err)
		}
		if plugin.ID() != item.id {
			t.Fatalf("unexpected plugin id: got %s want %s", plugin.ID(), item.id)
		}
	}
}

func TestFabricPPCGPaperExampleCycleVictimsMatchAlgorithm1(t *testing.T) {
	items := make([]literatureTxAccess, 6)
	for i := range items {
		items[i] = literatureTxAccess{TxID: fmt.Sprintf("T%d", i), Ordinal: i}
	}
	// Paper Section 5.1 example cycles:
	// c1 = T0 -> T3 -> T0
	// c2 = T0 -> T3 -> T1 -> T0
	// c3 = T2 -> T4 -> T2
	edges := map[int]map[int]bool{
		0: map[int]bool{3: true},
		1: map[int]bool{0: true},
		2: map[int]bool{4: true},
		3: map[int]bool{0: true, 1: true},
		4: map[int]bool{2: true},
	}
	adjacency := fabricPPCGAdjacency(len(items), edges)
	set := newFabricPPCGCycleSet(len(items))
	for _, component := range fabricPPCGCyclicSCCs(len(items), edges) {
		if err := fabricPPCGEnumerateComponentCycles(component, adjacency, set); err != nil {
			t.Fatal(err)
		}
	}
	if set.cycleCount() != 3 {
		t.Fatalf("paper example must enumerate exactly three elementary cycles, got %d", set.cycleCount())
	}
	victims, err := fabricPPCGBreakCycles(set)
	if err != nil {
		t.Fatal(err)
	}
	if len(victims) != 2 || victims[0] != 0 || victims[1] != 2 {
		t.Fatalf("Algorithm 1 paper example must remove T0 then T2, got %+v", victims)
	}
}

func TestFabricPPCGDoesNotTurnWorkerScalingIntoAnAlgorithmExtension(t *testing.T) {
	if got := fabricPPCGExecutionWorkerCount(map[string]any{"worker_count": 32}, 32); got != 1 {
		t.Fatalf("Fabric++ reordering has no post-order parallel executor; got worker count %d", got)
	}
}
