package v5

import "testing"

func TestMetaTrackDynamicFastPathV24(t *testing.T) {
	s := ScheduleResult{DependencyInfluenceOrdinal: map[string]uint64{"n1": 1, "n2": 2, "r1": 3, "r0": 4}}
	cases := []struct {
		normal, risk []string
		rounds       map[string]uint64
		first        string
	}{
		{[]string{"n1"}, []string{"r1"}, map[string]uint64{"n1": 2, "r1": 1}, "r1"},
		{[]string{"n1"}, []string{"r1"}, map[string]uint64{"n1": 1, "r1": 1}, "n1"},
		{nil, []string{"r1"}, map[string]uint64{"r1": 1}, "r1"},
		{[]string{"n2"}, nil, map[string]uint64{"n2": 1}, "n2"},
	}
	for i, c := range cases {
		a, b := append([]string(nil), c.normal...), append([]string(nil), c.risk...)
		v := metaTrackPopDynamicUncontendedReady(&a, &b, c.rounds, s)
		if v != c.first {
			t.Fatalf("case %d: got %q want %q", i, v, c.first)
		}
	}
}
