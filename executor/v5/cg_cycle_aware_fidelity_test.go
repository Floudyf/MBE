package v5

import (
	"fmt"
	"sort"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func cgCycleFixture() realblock.Block {
	return realblock.Block{ShardID: "s0", Height: 11, TxList: []tx.SignedTransaction{
		{TxID: "t0", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessRead}, {Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessRead}, {Key: "a", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
}

func TestCGCycleAwarePlanDetectsAndResolvesCycleDeterministically(t *testing.T) {
	block := cgCycleFixture()
	first, err := buildCGPlanWithWorkers(block, 1)
	if err != nil {
		t.Fatal(err)
	}
	second, err := buildCGPlanWithWorkers(block, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.AbortedTransactionIDs) == 0 {
		t.Fatalf("Nezha CG must abort at least one cycle victim")
	}
	if stableDigest(first.AbortedTransactionIDs) != stableDigest(second.AbortedTransactionIDs) || stableDigest(first.Waves) != stableDigest(second.Waves) {
		t.Fatalf("Nezha CG planning must be deterministic and planner-worker independent: first=%+v second=%+v", first, second)
	}
	if first.Metrics.AbortCount != len(first.AbortedTransactionIDs) {
		t.Fatalf("abort metric mismatch: metrics=%d ids=%v", first.Metrics.AbortCount, first.AbortedTransactionIDs)
	}
	if first.Metrics.PlanningWorkerCount != 1 || second.Metrics.PlanningWorkerCount != 1 {
		t.Fatalf("official-reference CG construction must remain sequential: first=%d second=%d", first.Metrics.PlanningWorkerCount, second.Metrics.PlanningWorkerCount)
	}
	bound := block
	bindCGPlanForTest(t, &bound, first)
	if err := verifyCGPlanSmart(bound, first, 8); err != nil {
		t.Fatalf("validator-local requested worker count must not change CG semantics: %v", err)
	}
	covered := map[string]bool{}
	for _, wave := range first.Waves {
		for _, id := range wave {
			covered[id] = true
		}
	}
	for _, id := range first.AbortedTransactionIDs {
		if covered[id] {
			t.Fatalf("aborted transaction also appeared in executable waves: %s", id)
		}
		covered[id] = true
	}
	if len(covered) != len(block.TxList) {
		t.Fatalf("plan coverage=%d want=%d", len(covered), len(block.TxList))
	}
}

func TestCGPlanDigestIgnoresObservabilityTiming(t *testing.T) {
	plan, err := buildCGPlanWithWorkers(cgCycleFixture(), 8)
	if err != nil {
		t.Fatal(err)
	}
	clone := plan
	clone.Metrics.GraphConstructionMS += 12345
	clone.Metrics.SortingMS += 67890
	if literaturePlanDigest(clone) != plan.PlanDigest {
		t.Fatalf("non-semantic phase timing must not alter CG semantic plan digest")
	}
}

func TestCGNezhaJohnsonVictimUsesMaximumCycleMembership(t *testing.T) {
	items := []literatureTxAccess{{TxID: "t0"}, {TxID: "t1"}, {TxID: "t2"}}
	edges := map[int]map[int]bool{
		0: {1: true},
		1: {0: true, 2: true},
		2: {1: true},
	}
	_, aborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	// t1 participates in both elementary cycles; t0 and t2 each participate in one.
	if stableDigest(aborted) != stableDigest([]int{1}) {
		t.Fatalf("Nezha JohnsonCE maximum-membership victim mismatch: got=%v want=[1]", aborted)
	}
}

func TestCGNezhaJohnsonTieBreakUsesLowestOriginalOrdinal(t *testing.T) {
	items := []literatureTxAccess{{TxID: "t0"}, {TxID: "t1"}, {TxID: "t2"}}
	edges := map[int]map[int]bool{
		0: {1: true},
		1: {2: true},
		2: {0: true},
	}
	_, aborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	if stableDigest(aborted) != stableDigest([]int{0}) {
		t.Fatalf("Nezha JohnsonCE tie-break mismatch: got=%v want=[0]", aborted)
	}
}

func TestCGNezhaJohnsonVictimSequenceMatchesReferenceRule(t *testing.T) {
	items := []literatureTxAccess{{TxID: "t0"}, {TxID: "t1"}, {TxID: "t2"}, {TxID: "t3"}}
	edges := map[int]map[int]bool{
		0: {1: true},
		1: {0: true, 2: true},
		2: {1: true, 3: true},
		3: {2: true},
	}
	_, aborted, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	// Initially t1 and t2 each occur in two cycles, so the official strict-'>'
	// scan keeps the lower original ordinal t1. The residual t2<->t3 cycle then
	// selects t2 by the same tie rule.
	if stableDigest(aborted) != stableDigest([]int{1, 2}) {
		t.Fatalf("Nezha JohnsonCE iterative victim sequence mismatch: got=%v want=[1 2]", aborted)
	}
}

func cgReferenceSCCsForTest(vertexCount int, edges map[int]map[int]bool) [][]int {
	reach := make([][]bool, vertexCount)
	for source := 0; source < vertexCount; source++ {
		reach[source] = make([]bool, vertexCount)
		var visit func(int)
		visit = func(vertex int) {
			if reach[source][vertex] {
				return
			}
			reach[source][vertex] = true
			children := make([]int, 0, len(edges[vertex]))
			for child := range edges[vertex] {
				children = append(children, child)
			}
			sort.Ints(children)
			for _, child := range children {
				visit(child)
			}
		}
		visit(source)
	}
	used := make([]bool, vertexCount)
	components := make([][]int, 0)
	for vertex := 0; vertex < vertexCount; vertex++ {
		if used[vertex] {
			continue
		}
		component := make([]int, 0)
		for candidate := vertex; candidate < vertexCount; candidate++ {
			if !used[candidate] && reach[vertex][candidate] && reach[candidate][vertex] {
				used[candidate] = true
				component = append(component, candidate)
			}
		}
		if len(component) > 1 {
			components = append(components, component)
		}
	}
	return components
}

func cgReferenceElementaryCyclesForTest(component []int, edges map[int]map[int]bool, vertexCount int) [][]int {
	inComponent := make([]bool, vertexCount)
	for _, vertex := range component {
		inComponent[vertex] = true
	}
	cycles := make([][]int, 0)
	for _, start := range component {
		visited := make([]bool, vertexCount)
		visited[start] = true
		path := []int{start}
		var visit func(int)
		visit = func(vertex int) {
			children := make([]int, 0, len(edges[vertex]))
			for child := range edges[vertex] {
				if inComponent[child] {
					children = append(children, child)
				}
			}
			sort.Ints(children)
			for _, child := range children {
				if child != start && child < start {
					continue
				}
				if child == start {
					if len(path) > 1 {
						cycles = append(cycles, append([]int(nil), path...))
					}
					continue
				}
				if visited[child] {
					continue
				}
				visited[child] = true
				path = append(path, child)
				visit(child)
				path = path[:len(path)-1]
				visited[child] = false
			}
		}
		visit(start)
	}
	return cycles
}

func cgReferenceVictimsForTest(vertexCount int, edges map[int]map[int]bool) []int {
	invalid := make([]bool, vertexCount)
	for _, component := range cgReferenceSCCsForTest(vertexCount, edges) {
		cycles := cgReferenceElementaryCyclesForTest(component, edges, vertexCount)
		active := make([]bool, len(cycles))
		counts := make([]int, vertexCount)
		remainingMembership := 0
		for cycleIndex, cycle := range cycles {
			active[cycleIndex] = true
			for _, vertex := range cycle {
				counts[vertex]++
				remainingMembership++
			}
		}
		for remainingMembership != 0 {
			victim := 0
			for vertex := 1; vertex < vertexCount; vertex++ {
				if counts[vertex] > counts[victim] {
					victim = vertex
				}
			}
			invalid[victim] = true
			for cycleIndex, cycle := range cycles {
				if !active[cycleIndex] {
					continue
				}
				contains := false
				for _, vertex := range cycle {
					if vertex == victim {
						contains = true
						break
					}
				}
				if !contains {
					continue
				}
				active[cycleIndex] = false
				for _, vertex := range cycle {
					counts[vertex]--
					remainingMembership--
				}
			}
		}
	}
	victims := make([]int, 0)
	for vertex, aborted := range invalid {
		if aborted {
			victims = append(victims, vertex)
		}
	}
	return victims
}

func TestCGNezhaJohnsonExhaustiveFourVertexReferenceEquivalence(t *testing.T) {
	const n = 4
	pairs := make([][2]int, 0, n*(n-1))
	for from := 0; from < n; from++ {
		for to := 0; to < n; to++ {
			if from != to {
				pairs = append(pairs, [2]int{from, to})
			}
		}
	}
	items := make([]literatureTxAccess, n)
	for i := range items {
		items[i] = literatureTxAccess{TxID: fmt.Sprintf("t%d", i)}
	}
	for mask := 0; mask < (1 << len(pairs)); mask++ {
		edges := map[int]map[int]bool{}
		for bit, pair := range pairs {
			if mask&(1<<bit) == 0 {
				continue
			}
			if edges[pair[0]] == nil {
				edges[pair[0]] = map[int]bool{}
			}
			edges[pair[0]][pair[1]] = true
		}
		_, got, err := cgResolveCyclesAndWaves(items, edges)
		if err != nil {
			t.Fatalf("mask=%d: %v", mask, err)
		}
		want := cgReferenceVictimsForTest(n, edges)
		if stableDigest(got) != stableDigest(want) {
			t.Fatalf("mask=%d victim mismatch: got=%v want=%v edges=%v", mask, got, want, edges)
		}
	}
}

func TestCGNezhaJohnsonDenseEightVertexReferenceClosure(t *testing.T) {
	const n = 8
	items := make([]literatureTxAccess, n)
	edges := make(map[int]map[int]bool, n)
	for from := 0; from < n; from++ {
		items[from] = literatureTxAccess{TxID: fmt.Sprintf("t%d", from)}
		edges[from] = map[int]bool{}
		for to := 0; to < n; to++ {
			if from != to {
				edges[from][to] = true
			}
		}
	}
	waves, got, err := cgResolveCyclesAndWaves(items, edges)
	if err != nil {
		t.Fatal(err)
	}
	want := cgReferenceVictimsForTest(n, edges)
	if stableDigest(got) != stableDigest(want) {
		t.Fatalf("dense reference victim mismatch: got=%v want=%v", got, want)
	}
	if len(got) != n-1 || len(waves) != 1 || len(waves[0]) != 1 {
		t.Fatalf("dense closure mismatch: aborted=%v waves=%v", got, waves)
	}
}
