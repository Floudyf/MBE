package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func livenessRecord(index int, id, shard, key string, mode tx.AccessMode, required, produced uint64) WorkloadRecord {
	return WorkloadRecord{
		Index:          index,
		LogicalID:      id,
		ExecutionShard: shard,
		AccessList:     []tx.AccessItem{{Key: key, Mode: mode, UpdateSemantics: "set"}},
		StateVersions: []tx.StateVersionDependency{{
			Key:             key,
			RequiredVersion: required,
			ProducedVersion: produced,
		}},
	}
}

func findLivenessDependency(t *testing.T, records []WorkloadRecord, key string, version uint64) tx.StateVersionDependency {
	t.Helper()
	for _, record := range records {
		for _, dep := range record.StateVersions {
			if dep.Key == key && dep.ProducedVersion == version {
				return dep
			}
		}
	}
	t.Fatalf("missing dependency %s@%d", key, version)
	return tx.StateVersionDependency{}
}

func TestMetaTrackVersionLivenessV5ClassifiesRouteBatchGraph(t *testing.T) {
	records := []WorkloadRecord{
		livenessRecord(0, "local-p", "s0", "a", tx.AccessWrite, 0, 1),
		livenessRecord(1, "local-c", "s0", "a", tx.AccessReadWrite, 1, 2),
		livenessRecord(2, "dead-p", "s0", "d", tx.AccessWrite, 0, 3),
		livenessRecord(3, "dead-next", "s0", "d", tx.AccessWrite, 3, 4),
		livenessRecord(4, "remote-p", "s0", "r", tx.AccessWrite, 0, 5),
		livenessRecord(5, "remote-c", "s1", "r", tx.AccessRead, 5, 0),
		livenessRecord(6, "remote-final", "s0", "r", tx.AccessWrite, 5, 6),
		livenessRecord(7, "fallback-p", "s0", "f", tx.AccessWrite, 0, 7),
		livenessRecord(8, "fallback-remote", "s1", "f", tx.AccessWrite, 7, 8),
	}
	if !annotateMetaTrackVersionLivenessRecords(records) {
		t.Fatal("route-batch liveness annotation unexpectedly unavailable")
	}
	if got := findLivenessDependency(t, records, "a", 1).LivenessClass; got != metaTrackVersionClassLocalTransient {
		t.Fatalf("a@1 class=%s", got)
	}
	if got := findLivenessDependency(t, records, "a", 2).LivenessClass; got != metaTrackVersionClassFinalPersistent {
		t.Fatalf("a@2 class=%s", got)
	}
	if got := records[1].StateVersions[0].RequiredLivenessClass; got != metaTrackVersionClassLocalTransient {
		t.Fatalf("a@1 required liveness class on successor=%s", got)
	}
	if got := findLivenessDependency(t, records, "d", 3).LivenessClass; got != metaTrackVersionClassLocalTransient {
		// The following blind write is a local ordering/fallback successor, so the
		// predecessor is transient rather than dead until that successor completes.
		t.Fatalf("d@3 class=%s", got)
	}
	if got := findLivenessDependency(t, records, "r", 5).LivenessClass; got != metaTrackVersionClassRemoteLive {
		t.Fatalf("r@5 class=%s", got)
	}
	fallback := findLivenessDependency(t, records, "f", 7)
	if fallback.LivenessClass != metaTrackVersionClassRemoteLive || fallback.RemoteOrderingSuccessorCount != 1 {
		t.Fatalf("f@7 remote ordering fallback = %#v", fallback)
	}
	for _, record := range records {
		for _, dep := range record.StateVersions {
			if dep.ProducedVersion > 0 && (dep.LivenessClass == "" || dep.LivenessDigest == "") {
				t.Fatalf("missing signed liveness metadata: %#v", dep)
			}
		}
	}
}

func TestMetaTrackVersionLivenessV5RefusesIncompletePlacement(t *testing.T) {
	records := []WorkloadRecord{livenessRecord(0, "p", "", "a", tx.AccessWrite, 0, 1)}
	if annotateMetaTrackVersionLivenessRecords(records) {
		t.Fatal("liveness annotation must fail closed when execution placement is incomplete")
	}
}

func TestMetaTrackVersionLivenessV5SnapshotResolvesTransientPredecessor(t *testing.T) {
	snapshot := map[string]string{"s0::asset:1": "42"}
	value, ok := metaTrackExactSnapshotValue(snapshot, "asset:1")
	if !ok || value != "42" {
		t.Fatalf("snapshot resolution value=%q ok=%t", value, ok)
	}
}

func TestMetaTrackVersionLivenessV5OnlyElidesHomeWhenAllLocalSuccessorsAreInSameBlock(t *testing.T) {
	producerDependency := tx.StateVersionDependency{Key: "asset:1", ProducedVersion: 11, LivenessClass: metaTrackVersionClassLocalTransient, LocalValueSuccessorCount: 1}
	producer := tx.SignedTransaction{TxID: "p", ExecutionRouting: &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{producerDependency}}}
	successor := tx.SignedTransaction{TxID: "c", ExecutionRouting: &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: "asset:1", RequiredVersion: 11, RequiredLivenessClass: metaTrackVersionClassLocalTransient}}}}
	if !metaTrackLocalTransientIsSameBlock(realblock.Block{TxList: []tx.SignedTransaction{producer, successor}}, producer, producerDependency) {
		t.Fatal("all signed local successors in the same PBFT block should permit transient Home elision")
	}
	if metaTrackLocalTransientIsSameBlock(realblock.Block{TxList: []tx.SignedTransaction{producer}}, producer, producerDependency) {
		t.Fatal("a successor outside the current PBFT block must preserve the predecessor durably")
	}
}
