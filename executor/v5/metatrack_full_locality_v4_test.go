package v5

import (
	"metaverse-chainlab/executor/realism/state"
	"testing"
)

func TestMetaTrackFullLocalityV4SafeFoldPreservesExactVersions(t *testing.T) {
	items := []state.StateKV{{Key: "s0::k", Value: "1", ProducedVersion: 1, UpdateSemantics: "set"}, {Key: "s0::k", Value: "2", ProducedVersion: 2, UpdateSemantics: "set"}}
	out, folded, skipped := foldSafeMetaTrackRemoteItems(items)
	if len(out) != 2 || folded != 0 || skipped != 1 {
		t.Fatalf("exact-version chain must not be physically folded: len=%d folded=%d skipped=%d", len(out), folded, skipped)
	}
}
func TestMetaTrackFullLocalityV4SafeFoldMergesCommutativeItems(t *testing.T) {
	items := []state.StateKV{{Key: "s0::k", Delta: 2, UpdateSemantics: "commutative_delta", TxIDs: []string{"a"}, RoutingOrdinal: 1}, {Key: "s0::k", Delta: 3, UpdateSemantics: "commutative_delta", TxIDs: []string{"b"}, RoutingOrdinal: 2}}
	out, folded, skipped := foldSafeMetaTrackRemoteItems(items)
	if len(out) != 1 || folded != 1 || skipped != 0 || out[0].Delta != 5 || out[0].RoutingOrdinal != 2 {
		t.Fatalf("unexpected safe fold: %#v folded=%d skipped=%d", out, folded, skipped)
	}
}
