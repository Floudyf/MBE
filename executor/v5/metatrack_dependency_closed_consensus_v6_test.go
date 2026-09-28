package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func withMetaTrackVersionDeps(t *testing.T, item tx.SignedTransaction, deps []tx.StateVersionDependency) tx.SignedTransaction {
	t.Helper()
	routing := *item.ExecutionRouting
	routing.StateVersions = append([]tx.StateVersionDependency(nil), deps...)
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	return item
}

func TestMetaTrackDependencyClosedSelectorPacksSafeProjectionPrefix(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 2, 1)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 2, 1)
	second = withMetaTrackVersionDeps(t, second, []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1, ProducedVersion: 2}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || len(deferred) != 0 || len(projections) != 2 {
		t.Fatalf("unexpected closure selection: selected=%d deferred=%d projections=%d", len(selected), len(deferred), len(projections))
	}
	if _, err := validateMetaTrackDependencyClosedProjectionBlock(realblock.Block{ShardID: "s0", TxList: selected}, true); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackDependencyClosedValidatorRejectsFutureVersionBackEdge(t *testing.T) {
	item := signedMetaTrackBatchTestTx(t, "bad", 2, 1, "plan-1", "s0", 1, 1)
	item = withMetaTrackVersionDeps(t, item, []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 3, ProducedVersion: 2}})
	if _, err := validateMetaTrackDependencyClosedProjectionBlock(realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{item}}, true); err == nil {
		t.Fatal("future exact-version dependency must fail closed")
	}
}

func TestMetaTrackIndexedLivenessMatchesV5Classification(t *testing.T) {
	left := []WorkloadRecord{
		livenessRecord(0, "p", "s0", "asset:1", tx.AccessWrite, 0, 1),
		livenessRecord(1, "c", "s0", "asset:1", tx.AccessReadWrite, 1, 2),
	}
	right := append([]WorkloadRecord(nil), left...)
	right[0].AccessList = append([]tx.AccessItem(nil), left[0].AccessList...)
	right[1].AccessList = append([]tx.AccessItem(nil), left[1].AccessList...)
	right[0].StateVersions = append([]tx.StateVersionDependency(nil), left[0].StateVersions...)
	right[1].StateVersions = append([]tx.StateVersionDependency(nil), left[1].StateVersions...)
	if !annotateMetaTrackVersionLivenessRecords(left) || !annotateMetaTrackVersionLivenessRecordsIndexed(right) {
		t.Fatal("liveness annotation failed")
	}
	for i := range left {
		for j := range left[i].StateVersions {
			a, b := left[i].StateVersions[j], right[i].StateVersions[j]
			if a.LivenessClass != b.LivenessClass || a.LivenessDigest != b.LivenessDigest || a.RequiredLivenessClass != b.RequiredLivenessClass || a.RequiredLivenessDigest != b.RequiredLivenessDigest {
				t.Fatalf("indexed liveness diverged: old=%#v new=%#v", a, b)
			}
		}
	}
}
