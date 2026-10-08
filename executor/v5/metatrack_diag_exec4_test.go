package v5

import "testing"

// Four modes re-use the current exact-version DAG and materialization path.
// These tests exercise actual goroutine business-execution dispatch rather than
// checking only the frontend config names.
func TestMetaTrackExec4CurrentFullDualTrackParallel(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlockWithExecution(t, 4, txs, map[string]any{"business_execution_delay_ms": 15}, dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got < 2 || got > 4 {
		t.Fatalf("Full parallel execution expected 2..4 in-flight, got %d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); !got {
		t.Fatalf("Full dual-track evidence absent")
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackExec4UnifiedReadyParallel(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlockWithExecution(t, 4, txs, map[string]any{"business_execution_delay_ms": 15, "diagnostic_unified_ready_v1": true}, dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got < 2 || got > 4 {
		t.Fatalf("unified Ready runtime lost parallelism: %d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_diag_unified_ready_v1"].(bool); !got {
		t.Fatal("unified Ready runtime evidence absent")
	}
	if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); got {
		t.Fatal("unified Ready diagnostics must not advertise dual-track scheduling")
	}
	for _, attempt := range result.BusinessAttempts {
		if attempt.FinalCompletion && attempt.Track != "conservative" {
			t.Fatalf("unified Ready executed a Fast-lane business transaction: %#v", attempt)
		}
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackExec4DualTrackSingleBusiness(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlockWithExecution(t, 4, txs, map[string]any{"business_execution_delay_ms": 15, "diagnostic_single_business_execution_v1": true}, dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("dual-track single-business limit violated: %d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_actual_dual_track_runtime"].(bool); !got {
		t.Fatal("dual track disappeared")
	}
	if got, _ := result.ActualMetrics["metatrack_single_fifo_hol_enabled"].(bool); got {
		t.Fatal("dual-track single-business diagnostics incorrectly enabled strict FIFO")
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}

func TestMetaTrackExec4StrictFIFOIsOfficialA2Execution(t *testing.T) {
	txs := independentTransferTxs(4)
	result := runMetaTrackExecutorTestBlockWithExecution(t, 4, txs, map[string]any{"business_execution_delay_ms": 15}, metaTrackSingleExecution{makeBasic("execution", metaTrackSingleExecutionID, nil)})
	if got := intMetric(t, result.ActualMetrics, "max_inflight_business_executions"); got != 1 {
		t.Fatalf("strict FIFO limit violated: %d", got)
	}
	if got, _ := result.ActualMetrics["metatrack_single_fifo_hol_enabled"].(bool); !got {
		t.Fatal("strict FIFO HOL gate not running")
	}
	assertMetaTrackSerialEquivalent(t, result.ExecutionResult, txs)
}
