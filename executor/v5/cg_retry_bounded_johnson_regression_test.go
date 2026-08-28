package v5

import (
	"context"
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func cgRetryFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 11, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessRead}, {Key: "hot", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "hot", Mode: tx.AccessRead}, {Key: "hot", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestCGCycleVictimIsDeferredForRetryInsteadOfTerminalized(t *testing.T) {
	planned, err := (cgScheduler{}).PlanBlock(cgRetryFixture())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Block.TxList) != 1 || len(planned.Deferred) != 1 {
		t.Fatalf("accepted=%d deferred=%d", len(planned.Block.TxList), len(planned.Deferred))
	}
	if planned.Block.ExecutionPlan == nil || planned.Block.ExecutionPlan.AlgorithmID != cgPlanAlgorithmID {
		t.Fatalf("wrong plan envelope")
	}
	cp, err := parseCGConsensusPlan(planned.Block.ExecutionPlan.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(transactionIDs(planned.Deferred), cp.GraphPlan.AbortedTransactionIDs) {
		t.Fatalf("deferred/victim mismatch")
	}
	if err := verifyCGConsensusPlan(planned.Block, cp, true); err != nil {
		t.Fatal(err)
	}
	result, err := (cgBlockExecutor{}).ExecuteBlock(context.Background(), BlockExecutionInput{
		Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 2, ExecutionPlanVerified: true,
	})
	if err != nil {
		t.Fatalf("execute accepted projection: %v", err)
	}
	if result.ExecutionResult.FailedTxs != 0 || len(result.ExecutionResult.Receipts) != len(planned.Block.TxList) {
		t.Fatalf("deferred victim leaked into terminal execution: failed=%d receipts=%d accepted=%d",
			result.ExecutionResult.FailedTxs, len(result.ExecutionResult.Receipts), len(planned.Block.TxList))
	}
	if got, ok := result.ActualMetrics["cg_cycle_deferred_retry_count"].(int); !ok || got != 1 {
		t.Fatalf("deferred retry metric=%v want=1", result.ActualMetrics["cg_cycle_deferred_retry_count"])
	}
	found := false
	for _, e := range planned.Events {
		if e.TxID == planned.Deferred[0].TxID {
			found = e.Blocked && e.QueueName == "mempool_deferred" && e.DecisionReason == "nezha_cg_cycle_victim_deferred_retry"
		}
	}
	if !found {
		t.Fatalf("deferred scheduler event missing")
	}
}

func TestCGRetryPayloadRemainsLegacyPlanParserCompatible(t *testing.T) {
	planned, err := (cgScheduler{}).PlanBlock(cgRetryFixture())
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Deferred) == 0 {
		t.Fatal("fixture must exercise deferred retry metadata")
	}
	graphPlan, err := literatureParsePlan(planned.Block.ExecutionPlan.Payload, cgPlanAlgorithmID)
	if err != nil {
		t.Fatalf("retry metadata broke literature plan parser: %v", err)
	}
	cp, err := parseCGConsensusPlan(planned.Block.ExecutionPlan.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if cp.Version != cgConsensusPlanVersion {
		t.Fatalf("retry metadata version=%q", cp.Version)
	}
	if !reflect.DeepEqual(cp.GraphPlan, graphPlan) {
		t.Fatal("retry metadata changed the parsed CG graph plan")
	}
}

func TestCGBoundedJohnsonPrefixStopsDenseMixedEnumeration(t *testing.T) {
	const n = 9
	adj := make([][]int, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i != j {
				adj[i] = append(adj[i], j)
			}
		}
	}
	row := adj[0][:0]
	for _, j := range adj[0] {
		if j != 1 {
			row = append(row, j)
		}
	}
	adj[0] = row
	comp := make([]int, n)
	for i := range comp {
		comp[i] = i
	}
	set, truncated, work, err := cgNezhaFindCyclesBoundedContext(comp, adj, nil, 8, 128)
	if err != nil {
		t.Fatal(err)
	}
	if !truncated {
		t.Fatal("expected truncation")
	}
	if work > 128 {
		t.Fatalf("work=%d", work)
	}
	if set.cycleOccurrenceCount() == 0 {
		t.Fatal("no cycle evidence")
	}
	victims, err := cgNezhaBreakCycles(set)
	if err != nil || len(victims) == 0 {
		t.Fatalf("victims=%v err=%v", victims, err)
	}
}

func TestCGConfiguredBudgetsAreFiniteAndPlanningWorkerRemainsOne(t *testing.T) {
	if cgJohnsonCycleOccurrenceBudget <= 0 || cgJohnsonTraversalWorkBudget <= 0 || cgJohnsonPlanTraversalWorkBudget <= 0 {
		t.Fatal("budgets must be finite")
	}
	if cgJohnsonPlanTraversalWorkBudget < cgJohnsonTraversalWorkBudget {
		t.Fatal("whole-plan budget must cover at least one bounded call")
	}
	if cgPlanningWorkerCount(nil) != 1 {
		t.Fatal("planner worker drift")
	}
}
