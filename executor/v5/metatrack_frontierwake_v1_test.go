package v5

import "testing"

// Only future streaming proposals are delayed. Current/stale or unrelated
// proposals still go through their existing fail-closed validation path.
func TestMTFrontierWakeV1Decision(t *testing.T) {
	cases := []struct {
		name      string
		streaming bool
		proposed  uint64
		next      uint64
		want      bool
	}{
		{"future_streaming_parent_pending", true, 299, 298, true},
		{"current_streaming_validate_now", true, 299, 299, false},
		{"stale_streaming_validate_now", true, 298, 299, false},
		{"unrelated_method_future_preserve", false, 299, 298, false},
		{"far_future_existing_bound", true, 308, 298, true},
		{"zero_height_unrelated", true, 0, 1, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := mtPBFTDeferFutureStreamingV1(tc.streaming, tc.proposed, tc.next)
			if got != tc.want {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}
