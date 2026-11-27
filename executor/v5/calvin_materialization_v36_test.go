package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
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

func TestCalvinDurableHomeValueIncludesRemoteCounterWrites(t *testing.T) {
	const key = "market:counter"
	const qualified = "s0::market:counter"
	home := func(string) string { return "s0" }
	access := tx.AccessItem{Key: key, Mode: tx.AccessCommutativeDelta, UpdateSemantics: "market_sale_counter", Delta: 1}
	block := realblock.Block{ShardID: "calvin-global", Height: 1, TxList: []tx.SignedTransaction{
		{TxID: "local", AccessList: []tx.AccessItem{access}},
		{TxID: "remote", AccessList: []tx.AccessItem{access}},
	}}
	scheduler := statelessCalvinScheduler{calvinScheduler{basicPlugin: makeBasic("scheduler", calvinStatelessSchedulerID, nil)}}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	localVersion, _ := calvinConsensusVersion(1, 0)
	remoteVersion, _ := calvinConsensusVersion(1, 1)
	base := map[string]string{qualified: "3"}
	working := copyStringMap(base)
	calvinStatelessApplyOwnedWrites(working, "s0", []CalvinStatelessInboundWrite{
		{Key: key, Value: "4", RoutingOrdinal: localVersion, TxID: "local"},
		{Key: key, Value: "5", RoutingOrdinal: remoteVersion, TxID: "remote"},
	})
	partialTrace := []execution.TxDelta{
		{TxID: "local", Success: true, WriteSet: map[string]string{key: "4"}},
		{TxID: "remote", Success: true},
	}
	delta := annotateStateDeltaTxIDs([]state.StateKV{{Key: qualified, Value: working[qualified]}}, partialTrace, block.TxList)
	if len(delta) != 1 || delta[0].Delta != 1 || delta[0].UpdateSemantics != "commutative_delta" {
		t.Fatalf("fixture did not recreate the partial delta: %#v", delta)
	}
	delta, err = annotateCalvinStatelessLocalStateDelta(delta, planned.Block, "s0", home)
	if err != nil {
		t.Fatal(err)
	}
	durable, err := applyStateDeltaToSnapshot(base, delta, "s0", block.Height)
	if err != nil {
		t.Fatal(err)
	}
	if durable[qualified] != "5" || delta[0].PreviousVersion != localVersion || delta[0].ProducedVersion != remoteVersion {
		t.Fatalf("durable value or consensus versions differ: value=%q delta=%#v", durable[qualified], delta)
	}
}
