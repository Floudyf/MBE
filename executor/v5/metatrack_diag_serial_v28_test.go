package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackDiagSerialV28KeepsDualTrackAndCapsBusinessInflight(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlockWithExecution(
		t,
		4,
		txs,
		map[string]any{
			"business_execution_delay_ms":                 15,
			metaTrackDiagnosticSingleBusinessExecutionV28: true,
		},
		dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)},
	)

	if got := intMetric(t, result.ActualMetrics, "configured_worker_count"); got != 4 {
		t.Fatalf("diagnostic must keep configured worker_count=4, got %d", got)
	}
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("diagnostic must cap business execution in-flight at 1, got %d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); !got {
		t.Fatal("diagnostic must keep the real dual-track runtime")
	}
	if got, _ := result.ActualMetrics["metatrack_single_serial_execution_enabled"].(bool); got {
		t.Fatal("diagnostic must not enable the no-dual-track strict FIFO runtime")
	}
	if got, _ := result.ActualMetrics["metatrack_diag_single_business_execution_v28"].(bool); !got {
		t.Fatal("diagnostic single-business runtime evidence missing")
	}
	if got := intMetric(t, result.ActualMetrics, "metatrack_classification_fast_unique_count"); got == 0 {
		t.Fatal("diagnostic must preserve dual-track Fast classification")
	}
	if got := result.ActualMetrics["metatrack_scheduler_evidence_scope"]; got != "actual_dual_track_runtime" {
		t.Fatalf("diagnostic must retain dual-track scheduler evidence scope, got %#v", got)
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackDiagSerialV28StillUsesFastPriority(t *testing.T) {
	txs := independentTransferTxs(2)
	// Put a structurally conservative transaction first in canonical block order
	// and a normal Fast-eligible transaction second. The diagnostic keeps one
	// business execution in flight, but must still dispatch the Fast transaction
	// first rather than becoming the strict canonical FIFO no-dual-track ablation.
	txs[0].AccessList = nil

	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		block.TxIDs = append(block.TxIDs, item.TxID)
	}
	executionPlugin := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	scheduler := builtinScheduler{makeBasic("scheduler", "fast_first_scheduler", nil)}
	schedule := scheduler.Schedule(txs, executionPlugin)
	classification := batchClassificationWithReadiness(txs, executionPlugin, nil)

	_, metrics, _, attempts, err := executeMetaTrackScheduleWithFullLocality(
		context.Background(),
		schedule,
		classification,
		block,
		map[string]string{},
		4,
		10*time.Millisecond,
		nil,
		nil,
		nil,
		false,
		false,
		false,
		false,
		true,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := intMetric(t, metrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("single-business diagnostic exceeded one in-flight execution: %d", got)
	}
	if len(attempts) < 2 {
		t.Fatalf("expected two final business attempts, got %#v", attempts)
	}
	if attempts[0].TxID != txs[1].TxID || attempts[0].Track != "fast" {
		t.Fatalf("Fast transaction must bypass earlier Conservative candidate under the diagnostic: first=%#v", attempts[0])
	}
	if attempts[1].TxID != txs[0].TxID || attempts[1].Track != "conservative" {
		t.Fatalf("Conservative transaction should execute after the Fast candidate: second=%#v", attempts[1])
	}
}

// Compile-time guard that the test continues to use real signed transactions.
var _ = tx.SignedTransaction{}
