package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestTxAlloV202PlacementRemovesStaleCrossWrapper(t *testing.T) {
	routing := txalloRouting{}
	in := WorkloadRecord{Payload: "v5_cross:s1:dataset_event:mine", CrossShard: true, SourceShard: "s1", TargetShard: "s1"}
	got := routing.ApplyAccountPlacement(in, TransactionPlacement{HomeShard: "s0", ExecutionShard: "s0", TargetShard: "s0"})
	if got.CrossShard {
		t.Fatal("TxAllo-intra placement retained stale cross-shard flag")
	}
	if got.Payload != "dataset_event:mine" {
		t.Fatalf("stale wrapper not removed: %q", got.Payload)
	}
	if got.SourceShard != "s0" || got.TargetShard != "s0" {
		t.Fatalf("placement shards not authoritative: source=%q target=%q", got.SourceShard, got.TargetShard)
	}
}

func TestTxAlloV202PlacementRebuildsTargetFromTxAllo(t *testing.T) {
	routing := txalloRouting{}
	in := WorkloadRecord{Payload: "v5_cross:s0:dataset_event:mine", CrossShard: true, SourceShard: "s0", TargetShard: "s0"}
	got := routing.ApplyAccountPlacement(in, TransactionPlacement{HomeShard: "s0", ExecutionShard: "s0", TargetShard: "s1"})
	if !got.CrossShard {
		t.Fatal("TxAllo-cross placement lost cross-shard flag")
	}
	if got.Payload != "v5_cross:s1:dataset_event:mine" {
		t.Fatalf("TxAllo target did not replace stale workload target: %q", got.Payload)
	}
}

func TestStatelessTxAlloV202NoLegacyBusinessRelay(t *testing.T) {
	p := txalloNoRelayCrossShard{basicPlugin: makeBasic("cross_shard", txalloNoRelayCrossShardID, nil)}
	item := tx.SignedTransaction{TxID: "x", Payload: "v5_cross:s1:dataset_event:mine"}
	if p.IsCrossShard(item) {
		t.Fatal("Stateless-TxAllo must not enter legacy business Relay/Finalize")
	}
}
