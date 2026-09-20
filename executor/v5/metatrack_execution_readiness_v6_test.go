package v5

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackV6BlindWriteConflictsAreOrderingOnly(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{TxID: "w1", AccessList: []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "w2", AccessList: []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}
	got := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if len(got.Dependencies["w2"]) != 0 {
		t.Fatalf("blind WAW must not block execution: %#v", got.Dependencies)
	}
	if got.OrderingOnlyEdgeCount == 0 {
		t.Fatalf("blind WAW ordering evidence missing: %#v", got)
	}
}

func TestMetaTrackV6RMWKeepsTrueValueDependency(t *testing.T) {
	execution := dualTrackExecution{makeBasic("execution", "dual_track_execution", nil)}
	items := []tx.SignedTransaction{
		{TxID: "writer", AccessList: []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "rmw", AccessList: []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}}},
	}
	got := execution.ClassifyBatch(BatchClassificationInput{Transactions: items})
	if !reflect.DeepEqual(got.Dependencies["rmw"], []string{"writer"}) {
		t.Fatalf("RMW must wait for its predecessor value: %#v", got.Dependencies)
	}
}

func TestMetaTrackV6InBlockExactVersionUsesLocalHandoff(t *testing.T) {
	key := "asset:k"
	writer := tx.SignedTransaction{
		TxID:       "writer",
		AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 1,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 0, ProducedVersion: 1}},
		},
	}
	reader := tx.SignedTransaction{
		TxID:       "reader",
		AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessRead, UpdateSemantics: "validate"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 2,
			StateVersions:  []tx.StateVersionDependency{{Key: key, RequiredVersion: 1}},
		},
	}
	token := stateReadinessToken(reader, reader.AccessList[0])
	result := BatchClassificationResult{
		Decisions: map[string]ExecutionDecision{
			"writer": {Track: "fast", Reason: "test"},
			"reader": {Track: "fast", Reason: "test"},
		},
		Dependencies:  map[string][]string{},
		ReasonCodes:   map[string][]string{},
		StateWaitKeys: map[string][]string{"reader": []string{token}},
	}
	bindMetaTrackInBlockVersionHandoffs([]tx.SignedTransaction{writer, reader}, &result)
	if !reflect.DeepEqual(result.Dependencies["reader"], []string{"writer"}) {
		t.Fatalf("missing in-block producer dependency: %#v", result.Dependencies)
	}
	if !reflect.DeepEqual(result.StateWaitKeys["reader"], []string{token}) {
		t.Fatalf("in-block exact version must preserve StateReady token for exact value delivery: %#v", result.StateWaitKeys)
	}
	if result.InBlockVersionHandoffCount != 1 {
		t.Fatalf("handoff count=%d, want 1", result.InBlockVersionHandoffCount)
	}
}

func TestMetaTrackV6StateReadyOnlyWaitsForValuesActuallyRead(t *testing.T) {
	blind := tx.AccessItem{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}
	read := tx.AccessItem{Key: "asset:k", Mode: tx.AccessRead, UpdateSemantics: "validate"}
	rmw := tx.AccessItem{Key: "asset:k", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}
	if metaTrackStateReadyRequired(blind, true, true) {
		t.Fatal("versioned blind write must not wait for predecessor value")
	}
	if !metaTrackStateReadyRequired(read, true, false) {
		t.Fatal("local exact-version read must wait for exact value")
	}
	if !metaTrackStateReadyRequired(rmw, true, true) {
		t.Fatal("remote exact-version RMW must wait for exact value")
	}
	if !metaTrackStateReadyRequired(read, false, true) {
		t.Fatal("unversioned remote read still requires remote value")
	}
}
