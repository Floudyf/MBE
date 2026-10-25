package v5

import "testing"

// Unit contract for physical publication policy; signed metadata remains unchanged.
func TestMetaTrackSupplyP1HomePublicationClass(t *testing.T) {
	cases := []struct {
		class        string
		conservative bool
		expected     string
	}{
		{metaTrackVersionClassLocalTransient, false, metaTrackVersionClassLocalTransient},
		{metaTrackVersionClassDeadIntermediate, false, metaTrackVersionClassDeadIntermediate},
		{metaTrackVersionClassRemoteLive, false, metaTrackVersionClassRemoteLive},
		{metaTrackVersionClassFinalPersistent, false, metaTrackVersionClassFinalPersistent},
		{metaTrackVersionClassLocalTransient, true, metaTrackVersionClassRemoteLive},
		{metaTrackVersionClassDeadIntermediate, true, metaTrackVersionClassRemoteLive},
		{metaTrackVersionClassRemoteLive, true, metaTrackVersionClassRemoteLive},
		{metaTrackVersionClassFinalPersistent, true, metaTrackVersionClassFinalPersistent},
	}
	for _, tc := range cases {
		if got := metaTrackConservativeHomeClassV1(tc.conservative, tc.class); got != tc.expected {
			t.Fatalf("class=%s conservative=%v: got=%s want=%s", tc.class, tc.conservative, got, tc.expected)
		}
	}
}
