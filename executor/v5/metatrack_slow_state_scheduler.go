package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_MT_WAIT_READY_V1: two dispatch policies; both reuse the EXISTING
// dependency/stateReady suspension model, immutable semantic safety decisions
// and unified Ready worker pool. No additional waiting queue or fetcher.
const metaTrackSlowStateSchedulerID = "metatrack_slow_state_scheduler" // historical method ID retained
const metaTrackWaitFIFOSchedulerID = "metatrack_wait_fifo_scheduler"
const metaTrackWaitReadyPolicy = "waiting_ready_bypass_v1"
const metaTrackWaitFIFOControlPolicy = "waiting_ready_fifo_parallel_v1"

func scheduleMetaTrackSlowState(_ []tx.SignedTransaction, _ ExecutionPlugin, _ BatchClassificationResult, result ScheduleResult) ScheduleResult {
	result.ReadyPriorityPolicy = metaTrackWaitReadyPolicy
	return result
}

func scheduleMetaTrackWaitFIFO(_ []tx.SignedTransaction, _ ExecutionPlugin, _ BatchClassificationResult, result ScheduleResult) ScheduleResult {
	result.ReadyPriorityPolicy = metaTrackWaitFIFOControlPolicy
	return result
}
