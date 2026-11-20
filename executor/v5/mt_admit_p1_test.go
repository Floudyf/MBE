package v5

import (
	"math/rand"
	"reflect"
	"strconv"
	"testing"
)

// Preserve the historical fixed-point algorithm as a test oracle, including
// same-candidate chains, cycles, external unready versions, duplicate edges,
// and last-producer-wins for duplicate produced-version tokens.
func mtAdmissionReferenceP1(required [][]string, producers map[string]int, ready map[string]bool) []bool {
	selected := make([]bool, len(required))
	progress := true
	for progress {
		progress = false
		for index, tokens := range required {
			if selected[index] {
				continue
			}
			ok := true
			for _, token := range tokens {
				if producer, internal := producers[token]; internal {
					if !selected[producer] {
						ok = false
						break
					}
				} else if !ready[token] {
					ok = false
					break
				}
			}
			if ok {
				selected[index] = true
				progress = true
			}
		}
	}
	return selected
}

func TestMTAdmissionP1DifferentialClosure(t *testing.T) {
	rng := rand.New(rand.NewSource(20261102))
	for trial := 0; trial < 2000; trial++ {
		n := 1 + rng.Intn(70)
		required := make([][]string, n)
		producers := make(map[string]int)
		ready := make(map[string]bool)
		for i := 0; i < n; i++ {
			key := "v" + strconv.Itoa(rng.Intn(n+8))
			producers[key] = i // historical last producer wins
		}
		for i := range required {
			count := rng.Intn(11)
			for j := 0; j < count; j++ {
				key := "v" + strconv.Itoa(rng.Intn(n+16))
				required[i] = append(required[i], key)
				if rng.Intn(4) == 0 {
					required[i] = append(required[i], key) // repeated dependency
				}
				ready[key] = rng.Intn(2) == 0
			}
		}
		want := mtAdmissionReferenceP1(required, producers, ready)
		got := mtAdmissionReadyClosureP1(required, producers, ready)
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("trial %d: got=%v want=%v required=%v producers=%v ready=%v", trial, got, want, required, producers, ready)
		}
	}
}

func TestMTAdmissionP1BoundaryCases(t *testing.T) {
	cases := []struct {
		name      string
		required  [][]string
		producers map[string]int
		ready     map[string]bool
		want      []bool
	}{
		{"no_requirements", [][]string{{}, {}, {}}, map[string]int{}, map[string]bool{}, []bool{true, true, true}},
		{"reverse_chain", [][]string{{"v1"}, {"v2"}, {}}, map[string]int{"v1": 1, "v2": 2}, map[string]bool{}, []bool{true, true, true}},
		{"cycle", [][]string{{"v1"}, {"v0"}}, map[string]int{"v0": 0, "v1": 1}, map[string]bool{}, []bool{false, false}},
		{"external_unready", [][]string{{"remote"}, {"v0"}}, map[string]int{"v0": 0}, map[string]bool{}, []bool{false, false}},
		{"external_ready", [][]string{{"remote"}, {"v0"}}, map[string]int{"v0": 0}, map[string]bool{"remote": true}, []bool{true, true}},
		{"duplicate_edges", [][]string{{}, {"v0", "v0"}}, map[string]int{"v0": 0}, map[string]bool{}, []bool{true, true}},
		{"self_dependency", [][]string{{"v0"}}, map[string]int{"v0": 0}, map[string]bool{}, []bool{false}},
		{"empty_candidate", [][]string{}, map[string]int{}, map[string]bool{}, []bool{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mtAdmissionReadyClosureP1(tc.required, tc.producers, tc.ready)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got=%v want=%v", got, tc.want)
			}
		})
	}
}

// Isolated microbenchmark for a heavy candidate whose predecessors are in
// reverse order. This is NOT a 100K TPS claim.
func BenchmarkMTAdmissionP1Closure(b *testing.B) {
	const n = 1000
	required := make([][]string, n)
	producers := make(map[string]int, n)
	for i := 0; i < n; i++ {
		producers["v"+strconv.Itoa(i)] = i
		if i+1 < n {
			required[i] = []string{"v" + strconv.Itoa(i+1)}
		}
	}
	ready := map[string]bool{}
	b.Run("original", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = mtAdmissionReferenceP1(required, producers, ready)
		}
	})
	b.Run("indexed", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = mtAdmissionReadyClosureP1(required, producers, ready)
		}
	})
}
