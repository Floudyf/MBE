package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestCalvinV36OwnedMaterializationPreservesProducedVersionOrder(t *testing.T) {
	working := map[string]string{}
	rows := []CalvinStatelessInboundWrite{
		{Key: "k", Value: "new", RoutingOrdinal: 20, TxID: "tx-new"},
		{Key: "k", Value: "old", RoutingOrdinal: 10, TxID: "tx-old"},
	}
	calvinStatelessApplyOwnedWrites(working, "s1", rows)
	if got := working[qualifyStateKey("s1", "k")]; got != "new" {
		t.Fatalf("latest consensus write lost: got=%q want=new", got)
	}
}

func TestCalvinV36LocalOwnedRowsUseConsensusProducedVersion(t *testing.T) {
	item := tx.SignedTransaction{TxID: "tx-local"}
	deps := []tx.StateVersionDependency{{Key: "k", RequiredVersion: 7, ProducedVersion: 11}}
	rows, err := calvinStatelessLocalOwnedWriteRows(item, deps, map[string]string{"k": "value"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].RoutingOrdinal != 11 || rows[0].TxID != item.TxID || rows[0].Value != "value" {
		t.Fatalf("unexpected local materialization row: %#v", rows)
	}
}

func TestCalvinV36OrderingNoopCannotOverwriteOwnedState(t *testing.T) {
	working := map[string]string{qualifyStateKey("s1", "k"): "base"}
	rows := []CalvinStatelessInboundWrite{
		{Key: "k", Value: "ignored", RoutingOrdinal: 10, TxID: "tx-noop", OrderingNoop: true},
	}
	calvinStatelessApplyOwnedWrites(working, "s1", rows)
	if got := working[qualifyStateKey("s1", "k")]; got != "base" {
		t.Fatalf("ordering noop mutated state: %q", got)
	}
}
