package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_MT_DUAL_READY_V1
// Candidate-only ready arbitration.  It never changes signed dependency
// ownership, StateReady eligibility, consensus order, or execution semantics.
// The number of children with exactly one unfinished predecessor is maintained
// incrementally by the runtime.  It is a release OPPORTUNITY, not a promise
// that remote exact state is already available to the child.
const metaTrackUnlockReadySchedulerID = "metatrack_unlock_scheduler"
const metaTrackUnlockReadyPriorityPolicy = "ready_round_immediate_dependency_unlock_v1"

func scheduleMetaTrackUnlockReady(items []tx.SignedTransaction, execution ExecutionPlugin, classification BatchClassificationResult, result ScheduleResult) ScheduleResult {
	// Borrow exactly the established oldest-ready-round and signed canonical
	// ordinals.  Dynamic unlock count is computed at runtime, not from a static
	// H/D score and never from post-execution knowledge.
	result = scheduleMetaTrackReadyRoundControl(items, execution, classification, result)
	result.ReadyPriorityPolicy = metaTrackUnlockReadyPriorityPolicy
	return result
}

// metaTrackPeekUnlockReady chooses only within the OLDEST Ready Round.
// Tracks still retain the original semantic classification, but Fast is not
// an absolute priority over a Ready critical producer during actual contention.
func metaTrackPeekUnlockReady(fastReady, conservativeReady []string, roundByTx map[string]uint64, schedule ScheduleResult, unlockCount map[string]int) string {
	round, ok := metaTrackOldestReadyRound(fastReady, conservativeReady, roundByTx)
	if !ok {
		return ""
	}
	best := ""
	for _, queue := range [][]string{fastReady, conservativeReady} {
		for _, txID := range queue {
			if roundByTx[txID] != round {
				continue
			}
			if best == "" || unlockCount[txID] > unlockCount[best] ||
				(unlockCount[txID] == unlockCount[best] &&
					(schedule.DependencyInfluenceOrdinal[txID] < schedule.DependencyInfluenceOrdinal[best] ||
						(schedule.DependencyInfluenceOrdinal[txID] == schedule.DependencyInfluenceOrdinal[best] && txID < best))) {
				best = txID
			}
		}
	}
	return best
}

func metaTrackPopUnlockReady(fastReady, conservativeReady *[]string, roundByTx map[string]uint64, schedule ScheduleResult, unlockCount map[string]int) string {
	chosen := metaTrackPeekUnlockReady(*fastReady, *conservativeReady, roundByTx, schedule, unlockCount)
	if chosen == "" {
		return ""
	}
	for _, queue := range []*[]string{fastReady, conservativeReady} {
		for index, txID := range *queue {
			if txID == chosen {
				values := *queue
				*queue = append(values[:index], values[index+1:]...)
				return chosen
			}
		}
	}
	return ""
}
