package v5

// MBE_MT_DYN_FASTPATH_V24: only for the isolated candidate.  When all currently
// Ready jobs fit in free workers, arbitration cannot change admission.  The
// oldest Ready Round is preserved by looking only at queue heads (O(1)); no
// full scan, no competition policy, and no blocked event drain.
func metaTrackPopDynamicUncontendedReady(normalReady, riskReady *[]string, rounds map[string]uint64, schedule ScheduleResult) string {
	if len(*normalReady) == 0 && len(*riskReady) == 0 {
		return ""
	}
	fromNormal := len(*normalReady) != 0
	if fromNormal && len(*riskReady) != 0 {
		n, r := (*normalReady)[0], (*riskReady)[0]
		if rounds[r] < rounds[n] {
			fromNormal = false
		}
		if rounds[r] == rounds[n] {
			// With capacity for every job, preserve the normal-first lane policy.
			// No arbitrary risk penalty persists across Ready Rounds.
			fromNormal = true
		}
	}
	if !fromNormal {
		v := (*riskReady)[0]
		*riskReady = (*riskReady)[1:]
		return v
	}
	v := (*normalReady)[0]
	*normalReady = (*normalReady)[1:]
	return v
}
