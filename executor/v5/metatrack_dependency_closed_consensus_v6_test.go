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

func withMetaTrackExactRead(t *testing.T, item tx.SignedTransaction, key string, deps []tx.StateVersionDependency) tx.SignedTransaction {
	t.Helper()
	item.AccessList = []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	return withMetaTrackVersionDeps(t, item, deps)
}

func withMetaTrackSenderNonce(t *testing.T, item tx.SignedTransaction, sender string, nonce uint64) tx.SignedTransaction {
	t.Helper()
	item.Sender = sender
	item.Nonce = nonce
	routing := *item.ExecutionRouting
	routing.SenderID = sender
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	return item
}

func TestMetaTrackDependencyClosedSelectorReleasesOnlyReadyProjectionPrefix(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 2, 1)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 2, 1)
	second = withMetaTrackExactRead(t, second, "asset", []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1, ProducedVersion: 2}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || txIdentifier(selected[0]) != "p1" || len(deferred) != 1 || txIdentifier(deferred[0]) != "p2" || len(projections) != 1 || projections[0].Sequence != 1 {
		t.Fatalf("ready-prefix selection mismatch: selected=%d deferred=%d projections=%#v", len(selected), len(deferred), projections)
	}
	if _, err := validateMetaTrackDependencyClosedProjectionBlock(realblock.Block{ShardID: "s0", TxList: selected}, true); err != nil {
		t.Fatal(err)
	}
}

func TestMetaTrackDependencyClosedSelectorPacksContiguousIndependentReadyProjections(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 3, 1)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 3, 1)
	second = withMetaTrackVersionDeps(t, second, []tx.StateVersionDependency{{Key: "other", ProducedVersion: 2}})
	third := signedMetaTrackBatchTestTx(t, "p3", 3, 3, "plan-3", "s0", 3, 1)
	third = withMetaTrackExactRead(t, third, "asset", []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1, ProducedVersion: 3}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{third, second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || txIdentifier(selected[0]) != "p1" || txIdentifier(selected[1]) != "p2" || len(deferred) != 1 || txIdentifier(deferred[0]) != "p3" || len(projections) != 2 || projections[0].Sequence != 1 || projections[1].Sequence != 2 {
		t.Fatalf("contiguous ready-prefix mismatch: selected=%d deferred=%d projections=%#v", len(selected), len(deferred), projections)
	}
}

func TestMetaTrackDependencyClosedSelectorDoesNotSkipBlockedSequence(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 3, 1)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 3, 1)
	second = withMetaTrackExactRead(t, second, "asset", []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1, ProducedVersion: 2}})
	third := signedMetaTrackBatchTestTx(t, "p3", 3, 3, "plan-3", "s0", 3, 1)
	third = withMetaTrackVersionDeps(t, third, []tx.StateVersionDependency{{Key: "other", ProducedVersion: 3}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{third, second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || txIdentifier(selected[0]) != "p1" || len(deferred) != 2 || len(projections) != 1 || projections[0].Sequence != 1 {
		t.Fatalf("selector skipped a blocked signed RouteBatch sequence: selected=%d deferred=%d projections=%#v", len(selected), len(deferred), projections)
	}
}

func TestMetaTrackDependencyClosedSelectorUsesSchedulerRAWDependency(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 2, 1)
	first.AccessList = []tx.AccessItem{{Key: "asset", Mode: tx.AccessWrite, UpdateSemantics: "set"}}
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 2, 1)
	second.AccessList = []tx.AccessItem{{Key: "asset", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	// RequiredVersion intentionally remains zero: this dependency must come from
	// the existing scheduler RAW DAG, not from the V612 exact-version frontier.
	second = withMetaTrackVersionDeps(t, second, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 2}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || txIdentifier(selected[0]) != "p1" || len(deferred) != 1 || len(projections) != 1 {
		t.Fatalf("scheduler RAW dependency was not lifted to projection readiness: selected=%d deferred=%d projections=%#v", len(selected), len(deferred), projections)
	}
}

func TestMetaTrackDependencyClosedSelectorUsesSchedulerNonceDependency(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 2, 1)
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 2, 1)
	first = withMetaTrackSenderNonce(t, first, "alice", 1)
	second = withMetaTrackSenderNonce(t, second, "alice", 2)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "a", ProducedVersion: 1}})
	second = withMetaTrackVersionDeps(t, second, []tx.StateVersionDependency{{Key: "b", ProducedVersion: 2}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 1 || txIdentifier(selected[0]) != "p1" || len(deferred) != 1 || len(projections) != 1 {
		t.Fatalf("sender nonce dependency was not lifted to projection readiness: selected=%d deferred=%d projections=%#v", len(selected), len(deferred), projections)
	}
}

func TestMetaTrackDependencyClosedSelectorKeepsBlindWriteInReadyPrefix(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "p1", 1, 1, "plan-1", "s0", 2, 1)
	first = withMetaTrackVersionDeps(t, first, []tx.StateVersionDependency{{Key: "asset", ProducedVersion: 1}})
	second := signedMetaTrackBatchTestTx(t, "p2", 2, 2, "plan-2", "s0", 2, 1)
	second.AccessList = []tx.AccessItem{{Key: "asset", Mode: tx.AccessWrite, UpdateSemantics: "set"}}
	second = withMetaTrackVersionDeps(t, second, []tx.StateVersionDependency{{Key: "asset", RequiredVersion: 1, ProducedVersion: 2}})
	selected, deferred, projections, err := selectMetaTrackDependencyClosedPBFTProjections([]tx.SignedTransaction{second, first}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || len(deferred) != 0 || len(projections) != 2 {
		t.Fatalf("blind-write ordering-only edge must not become an execution barrier: selected=%d deferred=%d projections=%d", len(selected), len(deferred), len(projections))
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
