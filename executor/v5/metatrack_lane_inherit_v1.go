package v5

// MBE_MT_LANE_FINAL_V1: bounded cross-track priority inheritance.
// A Conservative producer may borrow worker preference ONLY if it is already
// Ready and currently holds the last outstanding dependency of a Fast child.
// The child is still blocked and exact-version StateReady remains mandatory.
// The scan is deliberately bounded; failure to find evidence changes nothing.
func metaTrackLaneInheritIndexV1(fastReady, conservativeReady []string, reverse map[string][]string, depCount map[string]int, fastIDs, completed map[string]bool) int {
	if len(fastReady) == 0 || len(conservativeReady) == 0 {
		return -1
	}
	limit := len(conservativeReady)
	if limit > 32 {
		limit = 32
	}
	for i := 0; i < limit; i++ {
		producer := conservativeReady[i]
		for _, child := range reverse[producer] {
			if fastIDs[child] && !completed[child] && depCount[child] == 1 {
				return i
			}
		}
	}
	return -1
}
