package v5

import (
	"context"
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func cgNezhaMultigraphFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 21, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{
			{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{
			{Key: "c", Mode: tx.AccessRead},
		}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{
			{Key: "c", Mode: tx.AccessRead},
			{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{
			{Key: "c", Mode: tx.AccessRead},
			{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"},
			{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"},
		}},
	}}
}

func TestCGNezhaOfficialMultigraphPreservesWWMultiplicityAndVictims(t *testing.T) {
	block := cgNezhaMultigraphFixture()
	plan, err := buildCGPlanWithWorkers(block, 8)
	if err != nil {
		t.Fatal(err)
	}
	if got := plan.AbortedTransactionIDs; !reflect.DeepEqual(got, []string{"t0", "t3"}) {
		t.Fatalf("official Nezha multigraph victims=%v want=[t0 t3]", got)
	}
	count := func(from, to int) int {
		total := 0
		for _, edge := range plan.Edges {
			if edge.From == from && edge.To == to {
				total++
			}
		}
		return total
	}
	if got := count(0, 3); got != 2 {
		t.Fatalf("WW multiplicity 0->3=%d want=2; edges=%v", got, plan.Edges)
	}
	if got := count(2, 3); got != 2 {
		t.Fatalf("WW multiplicity 2->3=%d want=2; edges=%v", got, plan.Edges)
	}
	if plan.Metrics.PlanningWorkerCount != 1 {
		t.Fatalf("official CG planning worker count=%d want=1", plan.Metrics.PlanningWorkerCount)
	}
}

func TestCGNezhaProjectionValidatorRejectsDroppedParallelWWArc(t *testing.T) {
	block := cgNezhaMultigraphFixture()
	plan, err := buildCGPlanWithWorkers(block, 1)
	if err != nil {
		t.Fatal(err)
	}
	bindCGPlanForTest(t, &block, plan)
	if err := verifyCGPlanSmart(block, plan, 8); err != nil {
		t.Fatalf("valid official multigraph projection rejected: %v", err)
	}

	tampered := plan
	tampered.Edges = append([]literatureGraphEdge(nil), plan.Edges...)
	remove, seen := -1, 0
	for i, edge := range tampered.Edges {
		if edge.From == 0 && edge.To == 3 {
			seen++
			if seen == 2 {
				remove = i
				break
			}
		}
	}
	if remove < 0 {
		t.Fatal("fixture did not produce duplicate 0->3 WW arcs")
	}
	tampered.Edges = append(tampered.Edges[:remove], tampered.Edges[remove+1:]...)
	tampered.Metrics.EdgeCount = len(tampered.Edges)
	tampered.PlanDigest = literaturePlanDigest(tampered)
	tamperedBlock := cgNezhaMultigraphFixture()
	bindCGPlanForTest(t, &tamperedBlock, tampered)
	if err := verifyCGPlanSmart(tamperedBlock, tampered, 8); err == nil {
		t.Fatal("projection validator accepted a dropped parallel WW arc")
	}
}

func TestCGNezhaReferenceOrderIsDistinctFromMBEExecutionFrontiers(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 22, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
	plan, err := buildCGPlanWithWorkers(block, 4)
	if err != nil {
		t.Fatal(err)
	}
	wantReference := []string{"t0", "t1", "t3", "t2"}
	wantFrontiers := [][]string{{"t0", "t1"}, {"t2", "t3"}}
	if !reflect.DeepEqual(plan.SerializationOrder, wantReference) {
		t.Fatalf("BasicTopologicalSort order=%v want=%v", plan.SerializationOrder, wantReference)
	}
	if !reflect.DeepEqual(plan.Waves, wantFrontiers) {
		t.Fatalf("MBE execution frontiers=%v want=%v", plan.Waves, wantFrontiers)
	}
	flattened := []string{}
	for _, frontier := range plan.Waves {
		flattened = append(flattened, frontier...)
	}
	if reflect.DeepEqual(flattened, plan.SerializationOrder) {
		t.Fatal("fixture must prove reference order and executor-frontier flattening are distinct truths")
	}
	bindCGPlanForTest(t, &block, plan)
	if err := verifyCGPlanSmart(block, plan, 8); err != nil {
		t.Fatal(err)
	}
	if err := verifyCGPreverifiedProjection(block, plan); err != nil {
		t.Fatal(err)
	}
}

func TestCGNezhaExecutionWorkerScalingDoesNotChangePlannerSemantics(t *testing.T) {
	block := realblock.Block{ShardID: "s0", Height: 23, TxList: []tx.SignedTransaction{
		{TxID: "a", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "x", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "b", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "y", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
	one, err := buildCGPlanWithWorkers(block, 1)
	if err != nil {
		t.Fatal(err)
	}
	eight, err := buildCGPlanWithWorkers(block, 8)
	if err != nil {
		t.Fatal(err)
	}
	if one.PlanDigest != eight.PlanDigest || !reflect.DeepEqual(one.AbortedTransactionIDs, eight.AbortedTransactionIDs) || !reflect.DeepEqual(one.SerializationOrder, eight.SerializationOrder) || !reflect.DeepEqual(one.Waves, eight.Waves) {
		t.Fatalf("requested planner worker count changed CG semantics: one=%+v eight=%+v", one, eight)
	}
	if one.Metrics.PlanningWorkerCount != 1 || eight.Metrics.PlanningWorkerCount != 1 {
		t.Fatalf("planning worker evidence must remain 1: %d/%d", one.Metrics.PlanningWorkerCount, eight.Metrics.PlanningWorkerCount)
	}

	result1, err := executeCGPlan(context.Background(), block, map[string]string{}, one, 1)
	if err != nil {
		t.Fatal(err)
	}
	result4, err := executeCGPlan(context.Background(), block, map[string]string{}, one, 4)
	if err != nil {
		t.Fatal(err)
	}
	if result1.WorkerCount != 1 || result4.WorkerCount != 4 {
		t.Fatalf("execution worker count not honored: one=%d four=%d", result1.WorkerCount, result4.WorkerCount)
	}
	if result1.ExecutionResult.StateRootAfter != result4.ExecutionResult.StateRootAfter {
		t.Fatalf("worker count changed final state root: one=%s four=%s", result1.ExecutionResult.StateRootAfter, result4.ExecutionResult.StateRootAfter)
	}
	if got := result4.ActualMetrics["cg_execution_adaptation_mode"]; got != cgExecutionAdaptationMode {
		t.Fatalf("cg_execution_adaptation_mode=%v", got)
	}
	if got := result4.ActualMetrics["cg_planning_worker_count"]; got != 1 {
		t.Fatalf("cg_planning_worker_count=%v", got)
	}
	if got := result4.ActualMetrics["cg_execution_worker_count"]; got != 4 {
		t.Fatalf("cg_execution_worker_count=%v", got)
	}
}
