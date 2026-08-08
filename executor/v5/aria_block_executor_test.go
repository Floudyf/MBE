package v5

import (
	"context"
	"fmt"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestAriaBlockExecutorRecomputesFullBatchAndMaterializesSelectedDeltas(t *testing.T) {
	txs := make([]tx.SignedTransaction, 0, 8)
	for index := 0; index < 8; index++ {
		generated, _, _, err := tx.Generate(tx.GenerateOptions{
			Count: 1, Sender: fmt.Sprintf("aria-independent-sender-%d", index),
			Receiver: fmt.Sprintf("aria-independent-receiver-%d", index), StartNonce: 0,
			Value: 10, Seed: fmt.Sprintf("aria-independent-transfer-%d", index), StartTimeMS: int64(index + 1),
		})
		if err != nil {
			t.Fatalf("generate signed independent transaction %d: %v", index, err)
		}
		if len(generated) != 1 {
			t.Fatalf("expected one generated transaction, got %d", len(generated))
		}
		txs = append(txs, generated[0])
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range txs {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}
	config := map[string]any{
		"worker_count": 4, "block_size": len(txs), "candidate_scan_multiplier": 1,
		"reordering": true, "read_only_optimization": true, "retry_nonce_gaps": true,
	}
	producer := ariaBlockProducer{makeBasic("block_producer", ariaBlockProducerID, config)}
	candidate, err := producer.BuildCandidate(BlockProductionInput{
		Pool: pool, Proposer: realblock.NewProposer("n0", "s0"), Limit: len(txs),
		Now: time.UnixMilli(10), Context: context.Background(), BaseStateSnapshot: map[string]string{}, WorkerCount: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	plugin, err := BuiltinRegistry().Create("block_executor", execution.AriaBlockExecutorID, config)
	if err != nil {
		t.Fatal(err)
	}
	result, err := plugin.(BlockExecutorPlugin).ExecuteBlock(context.Background(), BlockExecutionInput{
		Block: candidate, BaseStateSnapshot: map[string]string{}, NodeID: "n0", ShardID: "s0", WorkerCount: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExecutionResult.BlockExecutorID != execution.AriaBlockExecutorID {
		t.Fatalf("wrong executor id: %#v", result.ExecutionResult)
	}
	if result.WorkerCount != 4 || result.PlanDigest == "" || len(result.StateDelta) == 0 {
		t.Fatalf("generic block-executor result is incomplete: %#v", result)
	}
	if intMetric(t, result.ActualMetrics, "aria_epoch_count") != 1 || intMetric(t, result.ActualMetrics, "aria_conflict_abort_count") != 0 {
		t.Fatalf("unexpected Aria metrics: %#v", result.ActualMetrics)
	}
	if got := intMetric(t, result.ActualMetrics, "aria_selected_apply_execution_attempts"); got != 0 {
		t.Fatalf("selected transactions were executed twice: %d", got)
	}
	if verified, _ := result.ActualMetrics["aria_validator_selection_verified"].(bool); !verified {
		t.Fatalf("validator did not report full-batch recomputation: %#v", result.ActualMetrics)
	}
	if materialized, _ := result.ActualMetrics["aria_selected_materialized_without_reexecution"].(bool); !materialized {
		t.Fatalf("selected private deltas were not materialized directly: %#v", result.ActualMetrics)
	}
	serial := execution.NewSerialExecutor().ExecuteBlock(candidate, map[string]string{})
	if result.ExecutionResult.StateRootAfter != serial.StateRootAfter {
		t.Fatalf("independent Aria result diverged from serial state: got=%s want=%s", result.ExecutionResult.StateRootAfter, serial.StateRootAfter)
	}
	if len(result.ScheduleEvents) != len(txs) {
		t.Fatalf("expected one schedule event per candidate transaction: %#v", result.ScheduleEvents)
	}
}
