package v5

import (
	"reflect"
	"testing"
)

func TestMetaTrackDiagParallelV27UsesOneLaneAndKeepsWorkerParallelism(t *testing.T) {
	txs := independentTransferTxs(4)
	executionPlugin := metaTrackSingleConservativeExecution{basicPlugin: makeBasic("execution", metaTrackSingleConservativeExecutionID, nil)}
	result := runMetaTrackExecutorTestBlockWithExecution(t, 4, txs, map[string]any{"business_execution_delay_ms": 15}, executionPlugin)

	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got < 2 || got > 4 {
		t.Fatalf("diagnostic single-lane runtime must preserve real worker parallelism, got max inflight=%d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); got {
		t.Fatal("diagnostic runtime must not report dual-track execution")
	}
	if got, _ := result.ActualMetrics["metatrack_single_serial_execution_enabled"].(bool); got {
		t.Fatal("diagnostic runtime must not enable the strict single-transaction serial limit")
	}
	if got := result.ActualMetrics["metatrack_scheduler_evidence_scope"]; got != "actual_single_conservative_runtime_v663" {
		t.Fatalf("unexpected diagnostic evidence scope: %#v", got)
	}
	for _, attempt := range result.BusinessAttempts {
		if attempt.FinalCompletion && attempt.Track != "conservative" {
			t.Fatalf("diagnostic final attempt escaped the unified conservative lane: %#v", attempt)
		}
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackDiagParallelV27PreservesDependencyDAG(t *testing.T) {
	txs := independentTransferTxs(3)
	// Force a deterministic nonce dependency without changing the access model.
	txs[1].Sender = txs[0].Sender
	txs[1].Nonce = txs[0].Nonce + 1

	full := batchClassificationWithReadiness(txs, dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}, nil)
	diag := batchClassificationWithReadiness(txs, metaTrackSingleConservativeExecution{basicPlugin: makeBasic("execution", metaTrackSingleConservativeExecutionID, nil)}, nil)

	if !reflect.DeepEqual(full.Dependencies, diag.Dependencies) {
		t.Fatalf("diagnostic must preserve the Full dependency DAG\nfull=%#v\ndiag=%#v", full.Dependencies, diag.Dependencies)
	}
	if !reflect.DeepEqual(full.StateWaitKeys, diag.StateWaitKeys) {
		t.Fatalf("diagnostic must preserve StateReady tokens\nfull=%#v\ndiag=%#v", full.StateWaitKeys, diag.StateWaitKeys)
	}
	for txID, decision := range diag.Decisions {
		if decision.Track != "conservative" {
			t.Fatalf("diagnostic transaction %s was not forced to the unified lane: %#v", txID, decision)
		}
	}
}
