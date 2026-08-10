package v5

import "testing"

func TestGroundhogPaperScanLimitUsesReadyPoolDepth(t *testing.T) {
	for _, tc := range []struct{ depth, blockSize, want int }{{0, 1000, 0}, {8, 2, 8}, {1000, 1000, 1000}, {5000, 1000, 5000}} {
		if got := groundhogPaperScanLimit(tc.depth, tc.blockSize); got != tc.want {
			t.Fatalf("depth=%d block=%d got=%d want=%d", tc.depth, tc.blockSize, got, tc.want)
		}
	}
}
