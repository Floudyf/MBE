package execution

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestSerialCopyElisionPreservesOrderedTransactionSemantics(t *testing.T) {
	b := block.Block{ShardID: "s0", Height: 1, BlockHash: "serial-copy-elision", TxList: []tx.SignedTransaction{
		{TxID: "t1", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "t2", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "a", Mode: tx.AccessRead}, {Key: "b", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
		{TxID: "t3", AccessListSchema: "direct_v1", AccessListSource: "fixture", AccessList: []tx.AccessItem{{Key: "c", Mode: tx.AccessWrite, UpdateSemantics: "set"}}},
	}}
	base := map[string]string{"s0::a": "seed", "s0::c": "old"}
	executor := NewSerialExecutor()
	got := executor.ExecuteBlock(b, base)

	working := copySnapshot(base)
	var wantReceipts []Receipt
	var wantDeltas []TxDelta
	for index, item := range b.TxList {
		receipt, delta := executor.ExecuteTransaction(b, item, working, index)
		keys := make([]string, 0, len(delta.WriteSet))
		for key := range delta.WriteSet {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			qualified := key
			if !strings.Contains(qualified, "::") {
				qualified = b.ShardID + "::" + qualified
			}
			working[qualified] = delta.WriteSet[key]
		}
		receipt.StateRootAfterTx = state.RootOfSnapshot(working)
		delta.Receipt = receipt
		wantReceipts = append(wantReceipts, receipt)
		wantDeltas = append(wantDeltas, delta)
	}
	if got.StateRootAfter != state.RootOfSnapshot(working) {
		t.Fatalf("final root mismatch")
	}
	if !reflect.DeepEqual(got.Receipts, wantReceipts) {
		t.Fatalf("receipts changed under copy elision\ngot=%#v\nwant=%#v", got.Receipts, wantReceipts)
	}
	if !reflect.DeepEqual(got.TxDeltas, wantDeltas) {
		t.Fatalf("tx deltas changed under copy elision")
	}
}
