package v5

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func mtP3BCompareDelta(t *testing.T, base map[string]string, writes []map[string]string, successes []bool) {
	t.Helper()
	if len(writes) != len(successes) {
		t.Fatal("test fixture invalid")
	}
	normalized := copyStringMap(base)
	written := make(map[string]struct{})
	for i, txWrites := range writes {
		if !successes[i] {
			continue
		}
		for key, value := range txWrites {
			normalized[key] = value
			written[key] = struct{}{}
		}
	}
	want := executionStateDelta(base, normalized)
	got := mtP3BSparseStateDelta(base, normalized, written)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sparse delta differs from legacy full scan: got=%#v want=%#v", got, want)
	}
	// normalized is an owned full copy: returning it as StateUpdates does not
	// alias the immutable block-start base. No copy is needed at handoff.
	if len(base) > 0 {
		for key := range base {
			normalized[key] = "mutated-output"
			if base[key] == "mutated-output" {
				t.Fatal("full state result aliases base")
			}
			break
		}
	}
}

func TestMTP3BSparseDeltaMatchesFullScanEdgeCases(t *testing.T) {
	cases := []struct {
		name    string
		base    map[string]string
		writes  []map[string]string
		success []bool
	}{
		{"no_writes", map[string]string{"s0::a": "1"}, nil, nil},
		{"single_write", map[string]string{"s0::a": "1", "s0::b": "2"}, []map[string]string{{"s0::a": "3"}}, []bool{true}},
		{"duplicate_write", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::a": "2"}, {"s0::a": "3"}}, []bool{true, true}},
		{"restore_original", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::a": "2"}, {"s0::a": "1"}}, []bool{true, true}},
		{"same_value", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::a": "1"}}, []bool{true}},
		{"new_empty", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::missing": ""}}, []bool{true}},
		{"new_value", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::b": "2"}}, []bool{true}},
		{"failed_write", map[string]string{"s0::a": "1"}, []map[string]string{{"s0::a": "2"}}, []bool{false}},
		{"cross_shard_qualified", map[string]string{"s0::a": "1", "s1::b": "x"}, []map[string]string{{"s1::b": "y", "s0::a": "2"}}, []bool{true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { mtP3BCompareDelta(t, tc.base, tc.writes, tc.success) })
	}
}

func TestMTP3BSparseDeltaMatchesFullScanRandomized(t *testing.T) {
	rng := rand.New(rand.NewSource(20261103))
	for trial := 0; trial < 1200; trial++ {
		n := 1 + rng.Intn(400)
		base := make(map[string]string, n)
		keys := make([]string, 0, n+50)
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("s%d::asset:%04d", i%2, i)
			keys = append(keys, key)
			base[key] = fmt.Sprint(rng.Intn(9))
		}
		for i := 0; i < 25; i++ {
			keys = append(keys, fmt.Sprintf("s0::new:%03d", i))
		}
		nTx := rng.Intn(70)
		writes := make([]map[string]string, nTx)
		successes := make([]bool, nTx)
		for i := 0; i < nTx; i++ {
			successes[i] = rng.Intn(5) != 0
			writes[i] = make(map[string]string)
			for j := 0; j < rng.Intn(8); j++ {
				key := keys[rng.Intn(len(keys))]
				writes[i][key] = fmt.Sprint(rng.Intn(9))
			}
		}
		mtP3BCompareDelta(t, base, writes, successes)
	}
}

func BenchmarkMTP3BSparseVsFullScan(b *testing.B) {
	base := make(map[string]string, 60000)
	for i := 0; i < 60000; i++ {
		base[fmt.Sprintf("s0::asset:%08d", i)] = "1"
	}
	normalized := copyStringMap(base)
	written := map[string]struct{}{}
	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("s0::asset:%08d", i)
		normalized[key] = "2"
		written[key] = struct{}{}
	}
	b.Run("legacy_full_scan", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = executionStateDelta(base, normalized)
		}
	})
	b.Run("sparse_written_keys", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_ = mtP3BSparseStateDelta(base, normalized, written)
		}
	})
}
