package v5

import (
	"encoding/json"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestCGOfficialConstructionDeterministicAcrossRequestedWorkerCounts(t *testing.T) {
	block := graphFixture()
	one, err := buildCGPlanWithWorkers(block, 1)
	if err != nil {
		t.Fatal(err)
	}
	eight, err := buildCGPlanWithWorkers(block, 8)
	if err != nil {
		t.Fatal(err)
	}
	if stableDigest(one.Edges) != stableDigest(eight.Edges) || stableDigest(one.Waves) != stableDigest(eight.Waves) || stableDigest(one.AbortedTransactionIDs) != stableDigest(eight.AbortedTransactionIDs) {
		t.Fatalf("requested worker count changed official-reference CG semantics: one=%+v eight=%+v", one, eight)
	}
	if one.Metrics.PairChecks != 3 || eight.Metrics.PairChecks != 3 {
		t.Fatalf("CG pairwise comparison accounting changed: one=%d eight=%d", one.Metrics.PairChecks, eight.Metrics.PairChecks)
	}
	if one.Metrics.PlanningWorkerCount != 1 || eight.Metrics.PlanningWorkerCount != 1 {
		t.Fatalf("Nezha official CG graph construction is sequential: one=%d eight=%d", one.Metrics.PlanningWorkerCount, eight.Metrics.PlanningWorkerCount)
	}
}

func TestCGSmartValidatorAcceptsSharedDAGAndRejectsMissingOrExtraEdges(t *testing.T) {
	block := graphFixture()
	plan, err := buildCGPlanWithWorkers(block, 3)
	if err != nil {
		t.Fatal(err)
	}
	bindCGPlanForTest(t, &block, plan)
	if err := verifyCGPlanSmart(block, plan, 3); err != nil {
		t.Fatalf("valid shared DAG rejected: %v", err)
	}

	missing := plan
	missing.Edges = append([]literatureGraphEdge(nil), plan.Edges[1:]...)
	missing.Metrics.EdgeCount = len(missing.Edges)
	missing = literatureFinalizePlan(missing)
	missingBlock := graphFixture()
	bindCGPlanForTest(t, &missingBlock, missing)
	if err := verifyCGPlanSmart(missingBlock, missing, 3); err == nil {
		t.Fatal("smart validator accepted plan with missing dependency edge")
	}

	extra := plan
	extra.Edges = append(append([]literatureGraphEdge(nil), plan.Edges...), literatureGraphEdge{From: 0, To: 2})
	extra.Metrics.EdgeCount = len(extra.Edges)
	extra = literatureFinalizePlan(extra)
	extraBlock := graphFixture()
	bindCGPlanForTest(t, &extraBlock, extra)
	if err := verifyCGPlanSmart(extraBlock, extra, 3); err == nil {
		t.Fatal("smart validator accepted plan with extra dependency edge")
	}
}

func TestCGSmartValidatorCoversRW_WR_WWAddressDependencies(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 11, BlockHash: "cg-rw-wr-ww", TxList: []tx.SignedTransaction{
		{TxID: "w0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "r1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}}},
		{TxID: "w2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
	plan, err := buildCGPlanWithWorkers(block, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Edges) != 3 {
		t.Fatalf("expected RW/WR/WW complete dependency triangle, got %+v", plan.Edges)
	}
	bindCGPlanForTest(t, &block, plan)
	if err := verifyCGPlanSmart(block, plan, 2); err != nil {
		t.Fatal(err)
	}
}

func bindCGPlanForTest(t *testing.T, block *realblock.Block, plan literatureGraphPlan) {
	t.Helper()
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	block.ExecutionPlan = &realblock.ExecutionPlanEnvelope{
		AlgorithmID:   cgPlanAlgorithmID,
		PayloadDigest: stableTextDigest(string(raw)),
		PlanDigest:    plan.PlanDigest,
		Payload:       raw,
	}
}
