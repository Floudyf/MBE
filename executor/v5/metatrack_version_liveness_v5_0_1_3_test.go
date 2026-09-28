package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackVersionLivenessV5013ResealsFrontierAndPlan(t *testing.T) {
	records := []WorkloadRecord{
		livenessRecord(0, "p", "", "asset:1", tx.AccessWrite, 0, 1),
		livenessRecord(1, "c", "", "asset:1", tx.AccessReadWrite, 1, 2),
	}
	records[0].RoutingOrdinal = 1
	records[1].RoutingOrdinal = 2
	plan := BatchRoutingPlan{BatchIndex: 0, TransactionPlacements: []TransactionPlacement{
		{TxIndex: 0, ExecutionShard: "s0"},
		{TxIndex: 1, ExecutionShard: "s0"},
	}}
	applyMetaTrackDeclaredAccessFrontierV2(&plan, records)
	plan.PlanDigest = routingPlanDigest(plan)
	oldFrontier := plan.TransactionPlacements[0].FrontierDigest
	oldPlan := plan.PlanDigest

	resealed, ok := resealMetaTrackVersionLivenessPlan(records, plan)
	if !ok {
		t.Fatal("version-liveness reseal unexpectedly failed")
	}
	if resealed.PlanDigest == "" || resealed.PlanDigest == oldPlan {
		t.Fatalf("route plan digest was not resealed: old=%s new=%s", oldPlan, resealed.PlanDigest)
	}
	if resealed.TransactionPlacements[0].FrontierDigest == "" || resealed.TransactionPlacements[0].FrontierDigest == oldFrontier {
		t.Fatalf("frontier digest was not resealed: old=%s new=%s", oldFrontier, resealed.TransactionPlacements[0].FrontierDigest)
	}
	if got := resealed.TransactionPlacements[0].FrontierDigest; got != metaTrackDeclaredAccessFrontierDigest(records[0], "s0") {
		t.Fatalf("frontier digest does not bind final StateVersions: got=%s", got)
	}
	if got := resealed.PlanDigest; got != routingPlanDigest(resealed) {
		t.Fatalf("route plan digest does not bind resealed placements: got=%s", got)
	}

	routing := &tx.ExecutionRoutingMetadata{
		ControlPolicy:  metaTrackDeclaredAccessFrontierPolicy,
		RoutingOrdinal: records[0].RoutingOrdinal,
		ExecutionShard: "s0",
		StateVersions:  append([]tx.StateVersionDependency(nil), records[0].StateVersions...),
		FrontierDigest: resealed.TransactionPlacements[0].FrontierDigest,
	}
	item := tx.SignedTransaction{TxID: "p", LogicalTxID: "p", AccessList: append([]tx.AccessItem(nil), records[0].AccessList...), ExecutionRouting: routing}
	if err := validateMetaTrackDeclaredAccessFrontierBinding(item); err != nil {
		t.Fatalf("resealed frontier binding rejected: %v", err)
	}
}

func TestMetaTrackVersionLivenessV5013RejectsTamperedLivenessDigest(t *testing.T) {
	records := []WorkloadRecord{
		livenessRecord(0, "p", "s0", "asset:1", tx.AccessWrite, 0, 1),
		livenessRecord(1, "c", "s0", "asset:1", tx.AccessReadWrite, 1, 2),
	}
	if !annotateMetaTrackVersionLivenessRecords(records) {
		t.Fatal("annotation failed")
	}
	item := tx.SignedTransaction{TxID: "p", ExecutionRouting: &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", StateVersions: append([]tx.StateVersionDependency(nil), records[0].StateVersions...)}}
	if err := validateMetaTrackVersionLivenessMetadata(item); err != nil {
		t.Fatalf("valid liveness metadata rejected: %v", err)
	}
	item.ExecutionRouting.StateVersions[0].LocalValueSuccessorCount++
	if err := validateMetaTrackVersionLivenessMetadata(item); err == nil {
		t.Fatal("tampered liveness metadata must fail closed")
	}
}

func TestMetaTrackVersionLivenessV5013RejectsSemanticClassMismatchWithFreshDigest(t *testing.T) {
	dependency := tx.StateVersionDependency{Key: "asset:1", ProducedVersion: 7, LivenessClass: metaTrackVersionClassLocalTransient, RemoteValueSuccessorCount: 1, ValueSuccessorCount: 1}
	dependency.LivenessDigest = metaTrackVersionLivenessDigest(dependency)
	if err := validateMetaTrackVersionLivenessDependency(dependency); err == nil {
		t.Fatal("local_transient with a remote successor must fail even with a fresh digest")
	}
	dependency = tx.StateVersionDependency{Key: "asset:2", ProducedVersion: 8, LivenessClass: metaTrackVersionClassDeadIntermediate, LocalOrderingSuccessorCount: 1}
	dependency.LivenessDigest = metaTrackVersionLivenessDigest(dependency)
	if err := validateMetaTrackVersionLivenessDependency(dependency); err == nil {
		t.Fatal("dead_intermediate with a successor must fail even with a fresh digest")
	}
}

func TestMetaTrackVersionLivenessV5013RejectsIncompleteProducerMetadata(t *testing.T) {
	item := tx.SignedTransaction{TxID: "p", ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "asset:1", ProducedVersion: 9}}}}
	if metaTrackTransactionLivenessMetadataComplete(item) {
		t.Fatal("producer without class/digest must not be complete")
	}
	if err := validateMetaTrackVersionLivenessMetadata(item); err == nil {
		t.Fatal("producer without class/digest must fail validation")
	}
}

func TestMetaTrackVersionLivenessV5013RejectsRequiredBindingToDeadIntermediate(t *testing.T) {
	item := tx.SignedTransaction{TxID: "c", ExecutionRouting: &tx.ExecutionRoutingMetadata{StateVersions: []tx.StateVersionDependency{{Key: "asset:1", RequiredVersion: 5, RequiredLivenessClass: metaTrackVersionClassDeadIntermediate, RequiredLivenessDigest: "signed"}}}}
	if err := validateMetaTrackVersionLivenessMetadata(item); err == nil {
		t.Fatal("a successor cannot require a dead intermediate")
	}
}

func TestMetaTrackVersionLivenessV5013FutureBatchReferencesOnlyPreviousBatchFinalWriter(t *testing.T) {
	lastWriter := map[string]uint64{}
	first := WorkloadRecord{AccessList: []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessWrite, UpdateSemantics: "set"}}}
	second := WorkloadRecord{AccessList: []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessWrite, UpdateSemantics: "set"}}}
	nextBatchRead := WorkloadRecord{AccessList: []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	firstDeps := stateVersionDependenciesForRecord(first, 1, lastWriter)
	secondDeps := stateVersionDependenciesForRecord(second, 2, lastWriter)
	nextDeps := stateVersionDependenciesForRecord(nextBatchRead, 3, lastWriter)
	if firstDeps[0].RequiredVersion != 0 || firstDeps[0].ProducedVersion != 1 {
		t.Fatalf("first writer ticket=%#v", firstDeps[0])
	}
	if secondDeps[0].RequiredVersion != 1 || secondDeps[0].ProducedVersion != 2 {
		t.Fatalf("second writer ticket=%#v", secondDeps[0])
	}
	if nextDeps[0].RequiredVersion != 2 {
		t.Fatalf("future batch must inherit only the previous batch final writer, got=%#v", nextDeps[0])
	}
}
