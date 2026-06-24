package execution_sharding

import "testing"

func TestDualTrackClassificationAndScheduler(t *testing.T) {
	c := DualTrackConfig{Enabled: true, FastTrackMaxAccessSize: 2, ConservativeOnConflictHint: true, ConservativeOnMissingAccessSet: true}
	if ClassifyDualTrack(DualTrackTransaction{ID: "fast", AccessKeys: []string{"k"}}, c).Track != Fast {
		t.Fatal("small access should be fast")
	}
	for _, tx := range []DualTrackTransaction{{ID: "empty"}, {ID: "large", AccessKeys: []string{"a", "b", "c"}}, {ID: "risk", AccessKeys: []string{"a"}, ConflictHint: "high"}} {
		if ClassifyDualTrack(tx, c).Track != Conservative {
			t.Fatal("unsafe transaction should be conservative")
		}
	}
	out, m := Schedule([]TrackDecision{{TxID: "slow", Track: Conservative, ExecutionShard: 0}, {TxID: "fast", Track: Fast, ExecutionShard: 0}}, 2)
	if len(out) != 2 || out[0].Track != Fast || m.FastExecuted != 1 || m.ConservativeExecuted != 1 {
		t.Fatalf("bad schedule %+v %+v", out, m)
	}
}
