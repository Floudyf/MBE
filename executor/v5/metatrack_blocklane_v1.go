package v5

// MBE_MT_BLOCKLANE_V1: candidate-only dependency-safe FIFO admission order.
// Never activated by Full, PBFT, or the four formal ablations.
import (
	"container/heap"
	"fmt"
	"metaverse-chainlab/executor/realism/tx"
)

type metaTrackBlockingIntHeapV1 []int

func (h metaTrackBlockingIntHeapV1) Len() int           { return len(h) }
func (h metaTrackBlockingIntHeapV1) Less(i, j int) bool { return h[i] < h[j] }
func (h metaTrackBlockingIntHeapV1) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *metaTrackBlockingIntHeapV1) Push(x any)        { *h = append(*h, x.(int)) }
func (h *metaTrackBlockingIntHeapV1) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}

func metaTrackBlockingOrderV1(ordered []tx.SignedTransaction, dependencies map[string][]string) ([]string, error) {
	positions := make(map[string]int, len(ordered))
	ids := make([]string, 0, len(ordered))
	for i, t := range ordered {
		id := txIdentifier(t)
		if id == "" {
			return nil, fmt.Errorf("blocklane fifo: empty tx id")
		}
		if _, exists := positions[id]; exists {
			return nil, fmt.Errorf("blocklane fifo: duplicate tx %s", id)
		}
		positions[id] = i
		ids = append(ids, id)
	}
	indegree := make([]int, len(ids))
	children := make([][]int, len(ids))
	for child, deps := range dependencies {
		c, ok := positions[child]
		if !ok {
			continue
		}
		seen := map[int]bool{}
		for _, parent := range deps {
			if parent == child {
				return nil, fmt.Errorf("blocklane fifo: self-dependency %s", child)
			}
			p, inside := positions[parent]
			if !inside || seen[p] {
				continue
			}
			seen[p] = true
			indegree[c]++
			children[p] = append(children[p], c)
		}
	}
	eligible := &metaTrackBlockingIntHeapV1{}
	heap.Init(eligible)
	for i, count := range indegree {
		if count == 0 {
			heap.Push(eligible, i)
		}
	}
	result := make([]string, 0, len(ids))
	for eligible.Len() > 0 {
		next := heap.Pop(eligible).(int)
		result = append(result, ids[next])
		for _, child := range children[next] {
			indegree[child]--
			if indegree[child] == 0 {
				heap.Push(eligible, child)
			}
		}
	}
	if len(result) != len(ids) {
		return nil, fmt.Errorf("blocklane fifo: dependency cycle or missing predecessor release")
	}
	return result, nil
}
