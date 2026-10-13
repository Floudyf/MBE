package v5

import "metaverse-chainlab/executor/realism/tx"

// MBE_MT_DYNAMIC_LANES_V2
// This is an execution-resource risk lane, NOT an authorization to bypass the
// existing Fast/Conservative semantic safety or exact StateReady gates.
// A signed remote-live output may require resource-heavy exact publication.
// It is a risk *candidate*, not proof of an observed Home queue bottleneck.
func metaTrackPotentialRemotePublishRisk(item tx.SignedTransaction) bool {
	if item.ExecutionRouting == nil {
		return false
	}
	for _, dependency := range item.ExecutionRouting.StateVersions {
		if dependency.LivenessClass == metaTrackVersionClassRemoteLive && dependency.RemoteValueSuccessorCount > 0 {
			return true
		}
	}
	return false
}

// Candidate-only, bounded competition-aware ready choice. Within the oldest
// Ready Round, a normal lane gets an execution opportunity when the remote-live
// lane already occupies every worker but one. Outside that situation, current
// unlock opportunities are preferred; equal opportunities favor normal work,
// then original canonical order. When there is no normal candidate, all worker
// slots remain work conserving. No timers or learned thresholds are needed.
func metaTrackPeekDynamicRiskReady(normalReady, riskReady []string, rounds map[string]uint64, schedule ScheduleResult, unlock map[string]int, riskInFlight, workerCount int) string {
	oldest, ok := metaTrackOldestReadyRound(normalReady, riskReady, rounds)
	if !ok {
		return ""
	}
	hasNormal := false
	for _, id := range normalReady {
		if rounds[id] == oldest {
			hasNormal = true
			break
		}
	}
	protectNormal := hasNormal && workerCount > 0 && riskInFlight >= workerCount-1
	best := ""
	bestRisk := false
	consider := func(values []string, risky bool) {
		for _, id := range values {
			if rounds[id] != oldest {
				continue
			}
			if protectNormal && risky {
				continue
			}
			if best == "" || unlock[id] > unlock[best] ||
				(unlock[id] == unlock[best] && bestRisk && !risky) ||
				(unlock[id] == unlock[best] && bestRisk == risky && metaTrackDependencyInfluenceOrdinalBefore(id, best, schedule)) {
				best = id
				bestRisk = risky
			}
		}
	}
	consider(normalReady, false)
	consider(riskReady, true)
	return best
}

func metaTrackDependencyInfluenceOrdinalBefore(left, right string, schedule ScheduleResult) bool {
	l, r := schedule.DependencyInfluenceOrdinal[left], schedule.DependencyInfluenceOrdinal[right]
	if l != r {
		return l < r
	}
	return left < right
}

func metaTrackPopDynamicRiskReady(normalReady, riskReady *[]string, rounds map[string]uint64, schedule ScheduleResult, unlock map[string]int, riskInFlight, workerCount int) string {
	id := metaTrackPeekDynamicRiskReady(*normalReady, *riskReady, rounds, schedule, unlock, riskInFlight, workerCount)
	if id == "" {
		return ""
	}
	for _, queue := range []*[]string{normalReady, riskReady} {
		for i, candidate := range *queue {
			if candidate == id {
				values := *queue
				*queue = append(values[:i], values[i+1:]...)
				return id
			}
		}
	}
	return ""
}
