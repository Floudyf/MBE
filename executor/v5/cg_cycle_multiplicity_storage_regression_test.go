package v5

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
)

func rawNezhaCycleReference(component []int, adjacency [][]int) ([][]int, error) {
	vertexCount := len(adjacency)
	vertices := append([]int(nil), component...)
	sort.Ints(vertices)
	if len(vertices) < 2 {
		return nil, nil
	}
	for _, vertex := range vertices {
		if vertex < 0 || vertex >= vertexCount {
			return nil, fmt.Errorf("vertex out of range: %d", vertex)
		}
	}
	// Preserve the exact special case in Nezha graph/johnsonce.go::FindCycles.
	if len(vertices) == 2 {
		return [][]int{{vertices[0], vertices[1]}}, nil
	}

	explore := make([]bool, vertexCount)
	for _, vertex := range vertices {
		explore[vertex] = true
	}
	cycles := make([][]int, 0)
	for _, start := range vertices {
		blocked := make([]bool, vertexCount)
		blockedMap := make([][]int, vertexCount)
		stack := make([]int, 0, len(vertices))

		var unblock func(int)
		unblock = func(vertex int) {
			blocked[vertex] = false
			for _, dependency := range blockedMap[vertex] {
				if blocked[dependency] {
					unblock(dependency)
				}
			}
			blockedMap[vertex] = nil
		}

		var find func(int) bool
		find = func(current int) bool {
			foundCycle := false
			stack = append(stack, current)
			blocked[current] = true
			for _, next := range adjacency[current] {
				if !explore[next] {
					continue
				}
				if next == start {
					foundCycle = true
					cycles = append(cycles, append([]int(nil), stack...))
				} else if !blocked[next] {
					if find(next) {
						foundCycle = true
					}
				}
			}
			if foundCycle {
				unblock(current)
			} else {
				for _, next := range adjacency[current] {
					if explore[next] {
						blockedMap[next] = append(blockedMap[next], current)
					}
				}
			}
			stack = stack[:len(stack)-1]
			return foundCycle
		}
		find(start)
		explore[start] = false
	}
	return cycles, nil
}

func cycleMembershipKey(vertices []int) string {
	values := append([]int(nil), vertices...)
	sort.Ints(values)
	return fmt.Sprint(values)
}

func aggregateRawCycleMembership(cycles [][]int, vertexCount int) (map[string]int64, []int64, int64) {
	multiplicity := map[string]int64{}
	membership := make([]int64, vertexCount)
	var remaining int64
	for _, cycle := range cycles {
		multiplicity[cycleMembershipKey(cycle)]++
		for _, vertex := range cycle {
			membership[vertex]++
			remaining++
		}
	}
	return multiplicity, membership, remaining
}

func aggregateCompressedCycleMembership(set *cgNezhaCycleSet) map[string]int64 {
	out := map[string]int64{}
	for i := 0; i < set.uniqueCycleCount(); i++ {
		out[cycleMembershipKey(set.cycleMembers(i))] = set.multiplicity[i]
	}
	return out
}

func rawBreakVictimSequence(cycles [][]int, vertexCount int) []int {
	active := make([]bool, len(cycles))
	membership := make([]int64, vertexCount)
	var remaining int64
	for i, cycle := range cycles {
		active[i] = true
		for _, vertex := range cycle {
			membership[vertex]++
			remaining++
		}
	}
	victims := make([]int, 0)
	for remaining != 0 {
		victim := 0
		for vertex := 1; vertex < vertexCount; vertex++ {
			if membership[vertex] > membership[victim] {
				victim = vertex
			}
		}
		if membership[victim] <= 0 {
			panic("raw reference has remaining membership without victim")
		}
		victims = append(victims, victim)
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
				membership[vertex]--
				remaining--
			}
		}
	}
	return victims
}

func compressedBreakVictimSequence(set *cgNezhaCycleSet) []int {
	membership := append([]int64(nil), set.membershipCount...)
	active := make([]bool, set.uniqueCycleCount())
	for i := range active {
		active[i] = true
	}
	remaining := set.remainingMembership
	victims := make([]int, 0)
	for remaining != 0 {
		victim := 0
		for vertex := 1; vertex < len(membership); vertex++ {
			if membership[vertex] > membership[victim] {
				victim = vertex
			}
		}
		if membership[victim] <= 0 {
			panic("compressed reference has remaining membership without victim")
		}
		victims = append(victims, victim)
		for _, cycleIndex := range set.cyclesByVertex[victim] {
			if !active[cycleIndex] {
				continue
			}
			active[cycleIndex] = false
			weight := set.multiplicity[cycleIndex]
			for _, vertex := range set.cycleMembers(cycleIndex) {
				membership[vertex] -= weight
				remaining -= weight
			}
		}
	}
	return victims
}

func TestCGMultiplicityStorageAggregatesMembershipRowsOnly(t *testing.T) {
	set := cgNewNezhaCycleSet(8)
	set.addCycle([]int{0, 3, 5})
	set.addCycle([]int{5, 0, 3})
	set.addCycle([]int{3, 5, 0})
	set.addCycle([]int{1, 2})
	set.buildSparseIndex()

	if got, want := set.cycleOccurrenceCount(), int64(4); got != want {
		t.Fatalf("occurrence count=%d want=%d", got, want)
	}
	if got, want := set.uniqueCycleCount(), 2; got != want {
		t.Fatalf("unique membership rows=%d want=%d", got, want)
	}
	got := aggregateCompressedCycleMembership(set)
	want := map[string]int64{"[0 3 5]": 3, "[1 2]": 1}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("compressed multiplicity=%v want=%v", got, want)
	}
	if got, want := set.membershipCount, []int64{3, 1, 1, 3, 0, 3, 0, 0}; !reflect.DeepEqual(got, want) {
		t.Fatalf("membership count=%v want=%v", got, want)
	}
}

func TestCGMultiplicityStoragePreservesOfficialTwoVertexFastPath(t *testing.T) {
	adjacency := [][]int{{1, 1, 1}, {0, 0, 0}}
	set, err := cgNezhaFindCycles([]int{0, 1}, adjacency)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := set.cycleOccurrenceCount(), int64(1); got != want {
		t.Fatalf("two-vertex occurrence count=%d want=%d", got, want)
	}
	if got, want := set.uniqueCycleCount(), 1; got != want {
		t.Fatalf("two-vertex unique rows=%d want=%d", got, want)
	}
	if got, want := set.multiplicity[0], int64(1); got != want {
		t.Fatalf("two-vertex multiplicity=%d want=%d", got, want)
	}
}

func TestCGMultiplicityStorageMatchesRawOfficialJohnsonReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20260827))
	for n := 3; n <= 6; n++ {
		for sample := 0; sample < 80; sample++ {
			adjacency := make([][]int, n)
			// Ring edges guarantee one SCC; random extra parallel arcs exercise
			// the exact NewBuildConflictGraph multigraph semantics without making
			// the small reference cases pathologically large.
			for from := 0; from < n; from++ {
				to := (from + 1) % n
				copies := 1 + rng.Intn(2)
				for i := 0; i < copies; i++ {
					adjacency[from] = append(adjacency[from], to)
				}
			}
			for from := 0; from < n; from++ {
				for to := 0; to < n; to++ {
					if from == to || to == (from+1)%n || rng.Intn(100) >= 24 {
						continue
					}
					copies := 1 + rng.Intn(2)
					for i := 0; i < copies; i++ {
						adjacency[from] = append(adjacency[from], to)
					}
				}
			}
			component := make([]int, n)
			for i := range component {
				component[i] = i
			}

			rawCycles, err := rawNezhaCycleReference(component, adjacency)
			if err != nil {
				t.Fatalf("n=%d sample=%d raw: %v", n, sample, err)
			}
			set, err := cgNezhaFindCycles(component, adjacency)
			if err != nil {
				t.Fatalf("n=%d sample=%d compressed: %v", n, sample, err)
			}
			rawMultiplicity, rawMembership, rawRemaining := aggregateRawCycleMembership(rawCycles, n)
			if got, want := set.cycleOccurrenceCount(), int64(len(rawCycles)); got != want {
				t.Fatalf("n=%d sample=%d occurrences=%d want=%d", n, sample, got, want)
			}
			if got := aggregateCompressedCycleMembership(set); !reflect.DeepEqual(got, rawMultiplicity) {
				t.Fatalf("n=%d sample=%d multiplicity mismatch\n got=%v\nwant=%v", n, sample, got, rawMultiplicity)
			}
			if !reflect.DeepEqual(set.membershipCount, rawMembership) {
				t.Fatalf("n=%d sample=%d membership=%v want=%v", n, sample, set.membershipCount, rawMembership)
			}
			if set.remainingMembership != rawRemaining {
				t.Fatalf("n=%d sample=%d remaining=%d want=%d", n, sample, set.remainingMembership, rawRemaining)
			}
			rawSequence := rawBreakVictimSequence(rawCycles, n)
			compressedSequence := compressedBreakVictimSequence(set)
			if !reflect.DeepEqual(compressedSequence, rawSequence) {
				t.Fatalf("n=%d sample=%d victim sequence=%v want=%v", n, sample, compressedSequence, rawSequence)
			}

			// Production BreakCycles returns the reference invalid-vertex set.
			productionSet, err := cgNezhaFindCycles(component, adjacency)
			if err != nil {
				t.Fatal(err)
			}
			gotVictims, err := cgNezhaBreakCycles(productionSet)
			if err != nil {
				t.Fatalf("n=%d sample=%d production BreakCycles: %v", n, sample, err)
			}
			wantVictims := append([]int(nil), rawSequence...)
			sort.Ints(wantVictims)
			if !reflect.DeepEqual(gotVictims, wantVictims) {
				t.Fatalf("n=%d sample=%d aborted=%v want=%v", n, sample, gotVictims, wantVictims)
			}
		}
	}
}
