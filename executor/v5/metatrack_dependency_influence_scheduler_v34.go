package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_METATRACK_DEPENDENCY_INFLUENCE_SCHEDULER_V34
// Candidate A/B scheduler. It never changes dependency construction, StateReady,
// exact-version semantics, PBFT, or final materialization. It only changes the
// choice among transactions that are already dependency/state ready.
//
// V34.2 deliberately leaves ScheduleResult.Ordered in canonical input order.
// H/D priority is consumed only by the runtime ready-queue selector, so the A/B
// variable is not contaminated by pre-dispatch transaction or state-fetch order.
const metaTrackDependencyInfluenceSchedulerID = "dependency_influence_scheduler"
const metaTrackDependencyInfluencePriorityPolicy = "tail_depth_descendant_count_then_canonical_v1"

func buildMetaTrackDependencyInfluenceScheduleEvidence(items []tx.SignedTransaction) (map[string]int, map[string]int, map[string]uint64, bool, string) {
	analysis := analyzeMetaTrackDependencyStructure(items)
	tail := make(map[string]int, len(items))
	descendants := make(map[string]int, len(items))
	ordinal := make(map[string]uint64, len(items))
	if !analysis.MatrixAnalysisValid {
		return tail, descendants, ordinal, false, analysis.MatrixAnalysisError
	}
	for index, item := range items {
		txID := txIdentifier(item)
		evidence, ok := analysis.ByTx[txID]
		if !ok || !evidence.MatrixAnalysisValid {
			return tail, descendants, ordinal, false, "missing_matrix_evidence:" + txID
		}
		tail[txID] = evidence.TailDepthH
		descendants[txID] = evidence.DescendantCountD
		if item.ExecutionRouting != nil && item.ExecutionRouting.RoutingOrdinal > 0 {
			ordinal[txID] = item.ExecutionRouting.RoutingOrdinal
		} else {
			ordinal[txID] = uint64(index + 1)
		}
	}
	return tail, descendants, ordinal, true, ""
}

func metaTrackDependencyInfluenceBefore(left, right string, schedule ScheduleResult) bool {
	leftH, rightH := schedule.DependencyInfluenceTailDepth[left], schedule.DependencyInfluenceTailDepth[right]
	if leftH != rightH {
		return leftH > rightH
	}
	leftD, rightD := schedule.DependencyInfluenceDescCount[left], schedule.DependencyInfluenceDescCount[right]
	if leftD != rightD {
		return leftD > rightD
	}
	leftOrdinal, rightOrdinal := schedule.DependencyInfluenceOrdinal[left], schedule.DependencyInfluenceOrdinal[right]
	if leftOrdinal != rightOrdinal {
		return leftOrdinal < rightOrdinal
	}
	return left < right
}

func metaTrackPopHighestDependencyInfluence(fastReady, conservativeReady *[]string, schedule ScheduleResult) string {
	bestTrack := 0
	bestIndex := -1
	bestID := ""
	consider := func(track int, values []string) {
		for index, txID := range values {
			if bestIndex < 0 || metaTrackDependencyInfluenceBefore(txID, bestID, schedule) {
				bestTrack = track
				bestIndex = index
				bestID = txID
			}
		}
	}
	consider(1, *fastReady)
	consider(2, *conservativeReady)
	if bestIndex < 0 {
		return ""
	}
	if bestTrack == 1 {
		values := *fastReady
		*fastReady = append(values[:bestIndex], values[bestIndex+1:]...)
	} else {
		values := *conservativeReady
		*conservativeReady = append(values[:bestIndex], values[bestIndex+1:]...)
	}
	return bestID
}

func scheduleMetaTrackDependencyInfluence(items []tx.SignedTransaction, _ ExecutionPlugin, _ BatchClassificationResult, result ScheduleResult) ScheduleResult {
	tail, descendants, ordinal, valid, analysisError := buildMetaTrackDependencyInfluenceScheduleEvidence(items)
	result.ReadyPriorityPolicy = metaTrackDependencyInfluencePriorityPolicy
	result.DependencyInfluenceTailDepth = tail
	result.DependencyInfluenceDescCount = descendants
	result.DependencyInfluenceOrdinal = ordinal
	result.DependencyInfluenceValid = valid
	result.DependencyInfluenceError = analysisError
	// Keep the signed/canonical schedule order intact. The runtime's nextReady
	// selector applies H -> D -> ordinal only after dependency + StateReady gates.
	result.Ordered = append([]tx.SignedTransaction(nil), items...)
	if !valid {
		result.Events = append(result.Events, ScheduleEvent{DecisionReason: "dependency_influence_invalid:" + analysisError})
		return result
	}
	result.Events = append(result.Events, ScheduleEvent{DecisionReason: "dependency_influence_ready_priority_enabled"})
	return result
}
