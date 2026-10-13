package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// Actual DCL 2026-10-11: two committed market_sale_counter +1 accesses
// appeared in a single certified s1 materialization, whose old collapse gave
// value=1 but generic annotation applied delta=2. Both paths must now agree on
// the business value 2 and its cryptographic partition root.
func TestPorygonDCLMarketTwoAddsV1(t *testing.T) {
	key := "market:0x73bd876068a0cb92c1333248da460ace91e1c4c0"
	access := []tx.AccessItem{{Key: key, Mode: tx.AccessCommutativeDelta, UpdateSemantics: "market_sale_counter", Delta: 1}}
	one := porygonTypedUpdateV1(PorygonStateUpdate{TxID: "65ddb272", OriginalIndex: 1, Key: key, Value: "1"}, access)
	two := porygonTypedUpdateV1(PorygonStateUpdate{TxID: "04da9ec3", OriginalIndex: 2, Key: key, Value: "1"}, access)
	collapsed := porygonPaperCollapseOrderedStateUpdates([]PorygonStateUpdate{one, two})
	if len(collapsed) != 1 || collapsed[0].UpdateSemantics != porygonDeltaSemanticV1 || collapsed[0].Delta != 2 {
		t.Fatalf("expected exact single +2; got %+v", collapsed)
	}
	if err := porygonPaperValidateCollapsedStateUpdates(collapsed); err != nil {
		t.Fatal(err)
	}
	partitionRoot := porygonPaperProspectivePartitionRoot(map[string]string{}, "s1", collapsed)
	expected := state.RootOfSnapshot(map[string]string{"s1::" + key: "2"})
	legacy := state.RootOfSnapshot(map[string]string{"s1::" + key: "1"})
	if partitionRoot != expected || partitionRoot == legacy {
		t.Fatalf("root diverged: got=%s correct=%s legacy=%s", partitionRoot, expected, legacy)
	}
	materialized := map[string]string{}
	if err := porygonApplyCanonicalKVV1(materialized, porygonStateUpdateKVV1(collapsed[0], "s1")); err != nil {
		t.Fatal(err)
	}
	if materialized["s1::"+key] != "2" || state.RootOfSnapshot(materialized) != partitionRoot {
		t.Fatal("materialization and Storage Role root differ")
	}
	// Shared generic annotation must not mutate this already certified delta.
	original := []state.StateKV{{Key: "s1::" + key, Value: "1", UpdateSemantics: porygonDeltaSemanticV1, Delta: 2}}
	annotated := []state.StateKV{{Key: "s1::" + key, Value: "1", UpdateSemantics: porygonDeltaSemanticV1, Delta: 1, TxIDs: []string{"a", "b"}}}
	fixed, err := porygonPreserveCertifiedDeltaV1(annotated, original)
	if err != nil || fixed[0].Delta != 2 || len(fixed[0].TxIDs) != 2 {
		t.Fatalf("certified delta lost: %+v, %v", fixed, err)
	}
}

func TestPorygonMixedAndRollbackV1(t *testing.T) {
	key := "counter:market"
	add := PorygonStateUpdate{Key: key, UpdateSemantics: porygonDeltaSemanticV1, Delta: 2}
	set := PorygonStateUpdate{Key: key, Value: "7"}
	// A rollback/final absolute write overrides prior increment.
	rows := porygonPaperCollapseOrderedStateUpdates([]PorygonStateUpdate{add, set})
	if len(rows) != 1 || rows[0].Value != "7" || rows[0].UpdateSemantics != "" {
		t.Fatalf("rollback must be final absolute write: %+v", rows)
	}
	// A later add following an explicit write yields one absolute 9.
	rows = porygonPaperCollapseOrderedStateUpdates([]PorygonStateUpdate{set, add})
	if len(rows) != 1 || rows[0].Value != "9" || rows[0].UpdateSemantics != "" {
		t.Fatalf("set then add mismatch: %+v", rows)
	}
	bad := porygonTypedUpdateV1(PorygonStateUpdate{Key: key, Value: "4"}, []tx.AccessItem{
		{Key: key, Mode: tx.AccessCommutativeDelta, Delta: 1}, {Key: key, Mode: tx.AccessReadWrite},
	})
	if err := porygonPaperValidateCollapsedStateUpdates([]PorygonStateUpdate{bad}); err == nil {
		t.Fatal("mixed access semantics must fail closed")
	}
}

func TestPorygonNoOtherMethodSemanticChangeV1(t *testing.T) {
	// Non-commutative and recovery absolute writes remain unchanged by the
	// Porygon-specific typed update helper.
	write := porygonTypedUpdateV1(PorygonStateUpdate{Key: "balance:alice", Value: "11"}, []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite}})
	if write.UpdateSemantics != "" || write.Delta != 0 {
		t.Fatal("unexpected translation of absolute write")
	}
	stateMap := map[string]string{"s1::balance:alice": "10"}
	if err := porygonApplyCanonicalKVV1(stateMap, porygonStateUpdateKVV1(write, "s1")); err != nil {
		t.Fatal(err)
	}
	if stateMap["s1::balance:alice"] != "11" {
		t.Fatal("absolute write not materialized")
	}
}
