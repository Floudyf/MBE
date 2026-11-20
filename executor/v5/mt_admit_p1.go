package v5

// mtAdmissionReadyClosureP1 computes the same least fixed point as the original
// producer-index admission scan. Only immutable dependencies inside one
// candidate are indexed; exact remote-version readiness is checked by the
// original admission probe on EVERY candidate attempt.
//
// Duplicate predecessor tokens are intentionally not deduplicated: a later
// occurrence counts as a separate edge, as in the original per-tx scan.
func mtAdmissionReadyClosureP1(requiredByTx [][]string, producerIndex map[string]int, externalReady map[string]bool) []bool {
	selected := make([]bool, len(requiredByTx))
	unresolved := make([]int, len(requiredByTx))
	blockedExternal := make([]bool, len(requiredByTx))
	dependents := make([][]int, len(requiredByTx))
	queue := make([]int, 0, len(requiredByTx))

	for index, tokens := range requiredByTx {
		for _, token := range tokens {
			if producer, internal := producerIndex[token]; internal {
				unresolved[index]++
				dependents[producer] = append(dependents[producer], index)
			} else if !externalReady[token] {
				blockedExternal[index] = true
			}
		}
		if unresolved[index] == 0 && !blockedExternal[index] {
			queue = append(queue, index)
		}
	}
	for len(queue) > 0 {
		index := queue[0]
		queue = queue[1:]
		if selected[index] {
			continue
		}
		selected[index] = true
		for _, child := range dependents[index] {
			unresolved[child]--
			if unresolved[child] == 0 && !blockedExternal[child] {
				queue = append(queue, child)
			}
		}
	}
	return selected
}
