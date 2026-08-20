package v5

import "testing"

func TestCatchupRetryPeerRotation(t *testing.T) {
	base := []string{"n0", "n1", "n2"}
	cases := []struct {
		ordinal int64
		first   string
	}{
		{1, "n0"},
		{2, "n1"},
		{3, "n2"},
		{4, "n0"},
	}
	for _, tc := range cases {
		got := rotateCatchupPeers(base, tc.ordinal)
		if len(got) != 3 || got[0] != tc.first {
			t.Fatalf("ordinal=%d got=%v want first=%s", tc.ordinal, got, tc.first)
		}
	}
	if base[0] != "n0" {
		t.Fatalf("rotation mutated source slice: %v", base)
	}
}
