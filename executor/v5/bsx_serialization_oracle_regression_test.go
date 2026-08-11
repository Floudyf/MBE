package v5

import (
	"context"
	"sort"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestBSXFinalStateMatchesItsOwnDeterministicSerializationOrder(t *testing.T) {
	b := block.Block{ShardID: "s0", Height: 11, BlockHash: "bsx-oracle", TxList: []tx.SignedTransaction{
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
	}}
	base := map[string]string{"s0::a": "seed-a", "s0::b": "seed-b"}
	plan, err := buildBSXPlan(b)
	if err != nil {
		t.Fatal(err)
	}
	got, err := executeLiteratureGraphPlan(context.Background(), b, base, plan, 4, bsxBlockExecutorID)
	if err != nil {
		t.Fatal(err)
	}

	byID := make(map[string]tx.SignedTransaction, len(b.TxList))
	indexByID := make(map[string]int, len(b.TxList))
	for index, item := range b.TxList {
		byID[item.TxID] = item
		indexByID[item.TxID] = index
	}
	working := literatureCopyStringMap(base)
	serial := execution.NewSerialExecutor()
	for _, id := range plan.SerializationOrder {
		item, ok := byID[id]
		if !ok {
			t.Fatalf("serialization order references unknown tx %s", id)
		}
		_, delta := serial.ExecuteTransaction(b, item, working, indexByID[id])
		keys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			working[literatureQualifiedKey(b.ShardID, key)] = delta.WriteSet[key]
		}
	}
	oracleRoot := state.RootOfSnapshot(working)
	if got.ExecutionResult.StateRootAfter != oracleRoot {
		t.Fatalf("BSX deterministic-serialization oracle mismatch: got=%s oracle=%s order=%v", got.ExecutionResult.StateRootAfter, oracleRoot, plan.SerializationOrder)
	}
}
