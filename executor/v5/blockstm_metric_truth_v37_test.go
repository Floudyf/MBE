package v5

import "testing"

// MBE_V37_BLOCKSTM_METRIC_RECOVERY_TRUTH
func TestV37BlockSTMMetricsFromMapRestoresFidelityAndTimingTruth(t *testing.T) {
	got := blockSTMMetricsFromMap(map[string]any{
		"worker_count":                        8,
		"reexecution_count":                   11,
		"unique_aborted_transaction_count":    5,
		"unique_reexecuted_transaction_count": 4,
		"dependency_wait_count":               7,
		"dependency_resume_count":             6,
		"dependency_suspend_count":            3,
		"scheduler_mode":                      "priority_heap_v1",
		"dependency_wait_mode":                "suspend_same_incarnation_v1",
		"transaction_execution_ms":            19,
		"materialization_ms":                  2,
		"state_commitment_ms":                 1,
		"business_execution_invocation_count": 13,
	})
	if got.UniqueAbortedTransactionCount != 5 || got.UniqueReexecutedTransactionCount != 4 {
		t.Fatalf("unique Block-STM truth was lost: %+v", got)
	}
	if got.DependencySuspendCount != 3 || got.SchedulerMode != "priority_heap_v1" || got.DependencyWaitMode != "suspend_same_incarnation_v1" {
		t.Fatalf("v37 Block-STM fidelity fields were lost: %+v", got)
	}
	if got.TransactionExecutionMS != 19 || got.MaterializationMS != 2 || got.StateCommitmentMS != 1 {
		t.Fatalf("Block-STM timing truth was lost: %+v", got)
	}
}
