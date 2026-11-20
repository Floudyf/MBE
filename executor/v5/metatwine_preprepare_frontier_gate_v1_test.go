package v5

import (
	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"testing"
)

func TestMTFrontierGateNeedsDeferralV1(t *testing.T) {
	cases := []struct {
		name            string
		height, durable uint64
		busy, want      bool
	}{
		{"n2_h38_before_h37_durable", 38, 36, false, true},
		{"n3_h37_during_h36_frontier_publish", 37, 36, true, true},
		{"current_height_ready", 37, 36, false, false},
		{"future_h38_durable_h37", 38, 37, false, false},
		{"stale_h36", 36, 36, true, false},
		{"zero", 0, 0, true, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := mtFrontierGateNeedsDeferralV1(tc.height, tc.durable, tc.busy); got != tc.want {
				t.Fatalf("deferral got=%v want=%v", got, tc.want)
			}
		})
	}
}

// The below test only exercises the existing deferred-record layout. It is
// deliberately not a substitute for a signed PRE-PREPARE integration test.
func TestMTFrontierGateRetainsOriginalPrePrepareShapeV1(t *testing.T) {
	pre := pbft.PrePrepare{View: 2, Sequence: 38, Height: 38, LeaderID: "n0", BlockHash: "b38", Signature: "sig38", Block: realblock.Block{Height: 38, BlockHash: "b38"}}
	retained := deferredPrePrepare{FromNode: pre.LeaderID, View: pre.View, Sequence: pre.Sequence, Block: pre.Block, Signature: pre.Signature}
	if retained.Block.Height != pre.Height || retained.Block.BlockHash != pre.BlockHash || retained.Signature != pre.Signature || retained.View != pre.View || retained.Sequence != pre.Sequence {
		t.Fatal("signed PRE-PREPARE cannot be fully restored for replay")
	}
}
