package v5

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func optmeV21VersionedTx(id string, deps ...tx.StateVersionDependency) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID: id,
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			StateVersions: deps,
		},
	}
}

func TestOptMEV21PublicationWavesPreservePerKeyVersionOrder(t *testing.T) {
	items := []tx.SignedTransaction{
		optmeV21VersionedTx("a1", tx.StateVersionDependency{Key: "a", ProducedVersion: 1}),
		optmeV21VersionedTx("b1", tx.StateVersionDependency{Key: "b", ProducedVersion: 2}),
		optmeV21VersionedTx("a2", tx.StateVersionDependency{Key: "a", RequiredVersion: 1, ProducedVersion: 3}),
		optmeV21VersionedTx("ab", tx.StateVersionDependency{Key: "a", RequiredVersion: 3, ProducedVersion: 4}, tx.StateVersionDependency{Key: "b", RequiredVersion: 2, ProducedVersion: 4}),
		optmeV21VersionedTx("c1", tx.StateVersionDependency{Key: "c", ProducedVersion: 5}),
	}
	got := optmeMethodPreservingPublicationWaves(items)
	want := [][]int{{0, 1, 4}, {2}, {3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("waves=%v want=%v", got, want)
	}
}

func TestOptMEV21PublicationWavesIgnoreReadOnlyVersionMetadata(t *testing.T) {
	items := []tx.SignedTransaction{
		optmeV21VersionedTx("read", tx.StateVersionDependency{Key: "a", RequiredVersion: 7}),
		optmeV21VersionedTx("write", tx.StateVersionDependency{Key: "a", RequiredVersion: 7, ProducedVersion: 8}),
	}
	got := optmeMethodPreservingPublicationWaves(items)
	want := [][]int{{1}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("waves=%v want=%v", got, want)
	}
}

func TestOptMEV21PublicationWavesKeepMultiKeyTransactionAfterAllPredecessors(t *testing.T) {
	items := []tx.SignedTransaction{
		optmeV21VersionedTx("a1", tx.StateVersionDependency{Key: "a", ProducedVersion: 1}),
		optmeV21VersionedTx("a2", tx.StateVersionDependency{Key: "a", RequiredVersion: 1, ProducedVersion: 2}),
		optmeV21VersionedTx("b1", tx.StateVersionDependency{Key: "b", ProducedVersion: 3}),
		optmeV21VersionedTx("ab", tx.StateVersionDependency{Key: "a", RequiredVersion: 2, ProducedVersion: 4}, tx.StateVersionDependency{Key: "b", RequiredVersion: 3, ProducedVersion: 4}),
	}
	got := optmeMethodPreservingPublicationWaves(items)
	want := [][]int{{0, 2}, {1}, {3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("waves=%v want=%v", got, want)
	}
}
