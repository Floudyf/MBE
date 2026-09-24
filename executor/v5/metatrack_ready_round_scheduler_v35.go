package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_METATRACK_READY_ROUND_SCHEDULER_V35
// V35 keeps MetaTrack's dependency/StateReady eligibility unchanged and changes
// only runtime arbitration among already-ready transactions. Transactions
// accumulated since the previous dispatch pass form one Ready Round. Older
// rounds are always drained before newer rounds, preventing cross-round
// starvation. Control and candidate use the same unified worker-lane model;
// their only policy difference is the within-round choice under contention.
const metaTrackReadyRoundControlSchedulerID = "ready_round_control_scheduler"
const metaTrackReadyRoundControlPriorityPolicy = "ready_round_fast_first_then_canonical_v1"

func scheduleMetaTrackReadyRoundControl(items []tx.SignedTransaction, _ ExecutionPlugin, _ BatchClassificationResult, result ScheduleResult) ScheduleResult {
	tail, descendants, ordinal, valid, analysisError := buildMetaTrackDependencyInfluenceScheduleEvidence(items)
	result.ReadyPriorityPolicy = metaTrackReadyRoundControlPriorityPolicy
	result.DependencyInfluenceTailDepth = tail
	result.DependencyInfluenceDescCount = descendants
	result.DependencyInfluenceOrdinal = ordinal
	result.DependencyInfluenceValid = valid
	result.DependencyInfluenceError = analysisError
	result.Ordered = append([]tx.SignedTransaction(nil), items...)
	if !valid {
		result.Events = append(result.Events, ScheduleEvent{DecisionReason: "ready_round_control_invalid:" + analysisError})
		return result
	}
	result.Events = append(result.Events, ScheduleEvent{DecisionReason: "ready_round_control_enabled"})
	return result
}

func metaTrackOldestReadyRound(fastReady, conservativeReady []string, roundByTx map[string]uint64) (uint64, bool) {
	var best uint64
	found := false
	for _, values := range [][]string{fastReady, conservativeReady} {
		for _, txID := range values {
			round := roundByTx[txID]
			if !found || round < best {
				best = round
				found = true
			}
		}
	}
	return best, found
}

func metaTrackReadyRoundCandidateCount(fastReady, conservativeReady []string, roundByTx map[string]uint64) int {
	round, ok := metaTrackOldestReadyRound(fastReady, conservativeReady, roundByTx)
	if !ok {
		return 0
	}
	count := 0
	for _, values := range [][]string{fastReady, conservativeReady} {
		for _, txID := range values {
			if roundByTx[txID] == round {
				count++
			}
		}
	}
	return count
}

func metaTrackReadyRoundControlBefore(left, right string, leftFast, rightFast bool, schedule ScheduleResult) bool {
	if leftFast != rightFast {
		return leftFast
	}
	leftOrdinal, rightOrdinal := schedule.DependencyInfluenceOrdinal[left], schedule.DependencyInfluenceOrdinal[right]
	if leftOrdinal != rightOrdinal {
		return leftOrdinal < rightOrdinal
	}
	return left < right
}

func metaTrackPeekReadyRoundChoice(fastReady, conservativeReady []string, roundByTx map[string]uint64, schedule ScheduleResult, influence bool) string {
	round, ok := metaTrackOldestReadyRound(fastReady, conservativeReady, roundByTx)
	if !ok {
		return ""
	}
	best := ""
	bestFast := false
	consider := func(values []string, fast bool) {
		for _, txID := range values {
			if roundByTx[txID] != round {
				continue
			}
			if best == "" {
				best, bestFast = txID, fast
				continue
			}
			if influence {
				if metaTrackDependencyInfluenceBefore(txID, best, schedule) {
					best, bestFast = txID, fast
				}
				continue
			}
			if metaTrackReadyRoundControlBefore(txID, best, fast, bestFast, schedule) {
				best, bestFast = txID, fast
			}
		}
	}
	consider(fastReady, true)
	consider(conservativeReady, false)
	return best
}

func metaTrackPopReadyRoundChoice(fastReady, conservativeReady *[]string, roundByTx map[string]uint64, schedule ScheduleResult, influence bool) string {
	selected := metaTrackPeekReadyRoundChoice(*fastReady, *conservativeReady, roundByTx, schedule, influence)
	if selected == "" {
		return ""
	}
	remove := func(values *[]string) bool {
		for index, txID := range *values {
			if txID != selected {
				continue
			}
			current := *values
			*values = append(current[:index], current[index+1:]...)
			return true
		}
		return false
	}
	if !remove(fastReady) {
		remove(conservativeReady)
	}
	return selected
}
