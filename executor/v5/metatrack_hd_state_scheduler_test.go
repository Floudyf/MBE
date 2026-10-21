package v5

import "testing"

func TestMTDualPrioRequestRank(t *testing.T) {
	tests := []struct {
		name string
		lh, ld, lw int
		lid string
		rh, rd, rw int
		rid string
		want bool
	}{
		{"tail_priority", 3, 0, 1, "a", 2, 100, 3, "b", true},
		{"descendant_tie", 3, 7, 1, "a", 3, 6, 5, "b", true},
		{"shared_waiters", 3, 7, 3, "a", 3, 7, 2, "b", true},
		{"canonical_id", 3, 7, 3, "a", 3, 7, 3, "b", true},
		{"reverse_tie", 3, 7, 3, "z", 3, 7, 3, "a", false},
		{"identical", 3, 7, 3, "a", 3, 7, 3, "a", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := metaTrackHDStatePrefetchBefore(tc.lh, tc.ld, tc.lw, tc.lid, tc.rh, tc.rd, tc.rw, tc.rid)
			if got != tc.want {
				t.Fatalf("candidate state rank got %t want %t", got, tc.want)
			}
		})
	}
}
