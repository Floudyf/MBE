package v5

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func projectionFrontierV612Tx(id string, sequence, ordinal uint64, readsExactValue bool, deps ...tx.StateVersionDependency) tx.SignedTransaction {
	mode := tx.AccessWrite
	if readsExactValue {
		mode = tx.AccessReadWrite
	}
	accesses := make([]tx.AccessItem, 0, len(deps))
	for _, dep := range deps {
		accesses = append(accesses, tx.AccessItem{Key: dep.Key, Mode: mode, UpdateSemantics: "set"})
	}
	return tx.SignedTransaction{TxID: id, AccessList: accesses, ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: ordinal, RouteBatchSequence: sequence, StateVersions: deps}}
}

func TestProjectionFrontierV612BuildsLayersAndReleasesAfterProjectionCompletion(t *testing.T) {
	items := []tx.SignedTransaction{
		projectionFrontierV612Tx("a", 1, 1, false, tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		projectionFrontierV612Tx("a2", 1, 2, false, tx.StateVersionDependency{Key: "q", ProducedVersion: 2}),
		projectionFrontierV612Tx("b", 2, 3, true, tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 3}),
		projectionFrontierV612Tx("c", 3, 4, false, tx.StateVersionDependency{Key: "z", ProducedVersion: 4}),
	}
	plan, err := buildMetaTrackProjectionFrontierV612Plan(items)
	if err != nil {
		t.Fatal(err)
	}
	if plan.ProjectionCount != 3 || plan.LayerCount != 2 || plan.MaxLayerProjectionWidth != 2 || plan.CrossProjectionEdgeCount != 1 {
		t.Fatalf("shape=%#v", plan)
	}
	if plan.transactionReady("b", map[string]bool{"a": true}) {
		t.Fatal("projection 2 must wait for the complete predecessor projection")
	}
	if !plan.transactionReady("b", map[string]bool{"a": true, "a2": true}) {
		t.Fatal("projection 2 should release after predecessor projection completion")
	}
	if !plan.transactionReady("c", map[string]bool{}) {
		t.Fatal("independent projection should remain in layer zero")
	}
}

func TestProjectionFrontierV612IgnoresBlindWriteOrderingEdgeForRelease(t *testing.T) {
	items := []tx.SignedTransaction{
		projectionFrontierV612Tx("a", 1, 1, false, tx.StateVersionDependency{Key: "k", ProducedVersion: 1}),
		projectionFrontierV612Tx("b", 2, 2, false, tx.StateVersionDependency{Key: "k", RequiredVersion: 1, ProducedVersion: 2}),
	}
	plan, err := buildMetaTrackProjectionFrontierV612Plan(items)
	if err != nil {
		t.Fatal(err)
	}
	if plan.CrossProjectionEdgeCount != 0 || plan.LayerCount != 1 {
		t.Fatalf("blind write should preserve existing no-read release semantics: %#v", plan)
	}
}

func TestProjectionFrontierV612RejectsOrdinalInversion(t *testing.T) {
	items := []tx.SignedTransaction{
		projectionFrontierV612Tx("producer", 2, 2, false, tx.StateVersionDependency{Key: "k", ProducedVersion: 2}),
		projectionFrontierV612Tx("consumer", 1, 1, true, tx.StateVersionDependency{Key: "k", RequiredVersion: 2}),
	}
	_, err := buildMetaTrackProjectionFrontierV612Plan(items)
	if err == nil || !strings.Contains(err.Error(), "ordinal inversion") {
		t.Fatalf("expected ordinal inversion, got %v", err)
	}
}

func TestProjectionFrontierV612UsesSignedDeadIntermediateClass(t *testing.T) {
	dep := tx.StateVersionDependency{Key: "k", ProducedVersion: 7, LivenessClass: metaTrackVersionClassDeadIntermediate}
	if dep.LivenessClass != metaTrackVersionClassDeadIntermediate {
		t.Fatal("signed liveness class not preserved")
	}
}
