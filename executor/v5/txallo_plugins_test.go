package v5

import "testing"

func TestTxAlloPluginsRegisterThroughBuiltinRegistry(t *testing.T) {
	r := BuiltinRegistry()
	for _, x := range []struct{ cat, id string }{{"sharding", txalloShardingID}, {"routing", txalloRoutingID}, {"routing", txalloStatelessRoutingID}} {
		if _, err := r.Create(x.cat, x.id, nil); err != nil {
			t.Fatalf("%s/%s: %v", x.cat, x.id, err)
		}
	}
}

func TestTxAlloStatefulAndStatelessRoutingShareFrozenMapping(t *testing.T) {
	sh := &txalloAccountSharding{aliases: map[string]string{}, evidence: map[string]any{}, allocator: newTxAlloAllocator([]string{"s0", "s1"}, 2, 1, 1e-9)}
	sh.allocator.Mapping = map[string]string{"alice": "s1", "bob": "s0"}
	input := BatchRoutingInput{Records: []WorkloadRecord{{Index: 0, LogicalID: "x", SenderID: "alice", ReceiverID: "bob"}}, ShardIDs: []string{"s0", "s1"}, Sharding: sh}
	a := txalloRouting{stateless: false}.PlanBatch(input)
	b := txalloRouting{stateless: true}.PlanBatch(input)
	if a.PlanDigest != b.PlanDigest || len(a.TransactionPlacements) != 1 || a.TransactionPlacements[0].HomeShard != "s1" || a.TransactionPlacements[0].TargetShard != "s0" {
		t.Fatalf("mapping mismatch stateful=%#v stateless=%#v", a, b)
	}
}

func TestTxAlloStatefulDoesNotAdvertiseStatelessDirectExecution(t *testing.T) {
	stateful := txalloRouting{stateless: false}
	stateless := txalloRouting{stateless: true}
	if stateful.StatelessDirectExecution() {
		t.Fatal("stateful TxAllo must not activate generic stateless remote-state substrate")
	}
	if !stateless.StatelessDirectExecution() {
		t.Fatal("Stateless-TxAllo must activate generic stateless remote-state substrate")
	}
	if stateful.BatchRoutingArtifactFamily() != "txallo" || stateless.BatchRoutingArtifactFamily() != "txallo" {
		t.Fatal("TxAllo routing evidence must stay in the TxAllo artifact family")
	}
}
