package v5

import (
	"context"
	"reflect"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func nezhaACGRetryFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 8, TxList: []tx.SignedTransaction{
		{TxID: "x", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "y", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "z", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessRead}, {Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestNezhaACGRetryPlanDefersHSVictims(t *testing.T) {
	block := nezhaACGRetryFixture()
	planner := acgScheduler{}
	planned, err := planner.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if got := transactionIDs(planned.Block.TxList); !reflect.DeepEqual(got, []string{"z"}) {
		t.Fatalf("accepted=%v want=[z]", got)
	}
	if got := transactionIDs(planned.Deferred); !reflect.DeepEqual(got, []string{"x", "y"}) {
		t.Fatalf("deferred=%v want=[x y]", got)
	}
	if planned.Block.ExecutionPlan == nil || planned.Block.ExecutionPlan.AlgorithmID != acgConsensusPlanAlgorithmID {
		t.Fatalf("unexpected execution plan envelope: %+v", planned.Block.ExecutionPlan)
	}
	if err := planner.VerifyBlockPlan(planned.Block); err != nil {
		t.Fatalf("first-pass plan verification failed: %v", err)
	}
	parsed, err := parseACGConsensusPlan(planned.Block.ExecutionPlan.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(parsed.GraphPlan.AbortedTransactionIDs, []string{"x", "y"}) {
		t.Fatalf("algorithm abort evidence=%v want=[x y]", parsed.GraphPlan.AbortedTransactionIDs)
	}
}

func TestNezhaACGRetryExecutorDoesNotTerminalizeDeferredVictims(t *testing.T) {
	block := nezhaACGRetryFixture()
	planner := acgScheduler{}
	planned, err := planner.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parseACGConsensusPlan(planned.Block.ExecutionPlan.Payload)
	if err != nil {
		t.Fatal(err)
	}
	result, err := executeACGPlan(context.Background(), planned.Block, map[string]string{"s0::a": "seed-a", "s0::b": "seed-b"}, parsed.GraphPlan, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ExecutionResult.Receipts) != 1 || result.ExecutionResult.Receipts[0].TxID != "z" {
		t.Fatalf("accepted receipts=%+v want only z", result.ExecutionResult.Receipts)
	}
	for _, receipt := range result.ExecutionResult.Receipts {
		if receipt.Error == "nezha_hs_aborted" {
			t.Fatalf("deferred HS victim was incorrectly terminalized: %+v", receipt)
		}
	}
	if got := result.ActualMetrics["nezha_hs_deferred_retry_count"]; got != 2 {
		t.Fatalf("deferred retry count=%v want=2", got)
	}
	if got := result.ActualMetrics["nezha_hs_retry_lifecycle"]; got != "fifo_deferred_to_later_block" {
		t.Fatalf("retry lifecycle=%v", got)
	}
	if got := result.ActualMetrics["nezha_hs_abort_count"]; got != 2 {
		t.Fatalf("HS abort evidence=%v want=2", got)
	}
}
