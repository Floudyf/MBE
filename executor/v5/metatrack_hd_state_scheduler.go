package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_MT_DUALPRIO_V1: candidate only. The classification and the signed
// dependency/StateReady gates remain untouched. H/D affects only Ready choice;
// the state prefetch ticket rank is a separate advisory for Home fetch batches.
const metaTrackHDStateSchedulerID = "metatrack_hd_state_scheduler"
const metaTrackHDStatePriorityPolicy = "ready_round_hd_and_critical_state_fetch_v1"

func scheduleMetaTrackHDState(items []tx.SignedTransaction, execution ExecutionPlugin, classification BatchClassificationResult, result ScheduleResult) ScheduleResult {
	result = scheduleMetaTrackDependencyInfluence(items, execution, classification, result)
	result.ReadyPriorityPolicy = metaTrackHDStatePriorityPolicy
	return result
}

// Pure deterministic rank. A late or unavailable exact version is never
// treated as ready by this rank; StateReady enforcement stays in the runtime.
func metaTrackHDStatePrefetchBefore(lh, ld, lw int, lid string, rh, rd, rw int, rid string) bool {
	if lh != rh {
		return lh > rh
	}
	if ld != rd {
		return ld > rd
	}
	if lw != rw {
		return lw > rw
	}
	return lid < rid
}
