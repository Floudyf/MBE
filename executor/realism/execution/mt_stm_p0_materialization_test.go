package execution

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// mtP0ComparableDeltas preserves all logical reads/writes and receipts, while
// separating scheduler-specific MVMemory provenance from consensus semantics.
// Read observation values, digests and keys remain strictly checked.
func mtP0ComparableDeltas(deltas []TxDelta) []TxDelta {
	out := make([]TxDelta, len(deltas))
	for i, delta := range deltas {
		out[i] = delta
		out[i].ReadSet = append([]ReadObservation(nil), delta.ReadSet...)
		for j := range out[i].ReadSet {
			out[i].ReadSet[j].Source = ""
		}
		sort.Slice(out[i].ReadSet, func(a, b int) bool {
			x, y := out[i].ReadSet[a], out[i].ReadSet[b]
			if x.Key != y.Key {
				return x.Key < y.Key
			}
			if x.Value != y.Value {
				return x.Value < y.Value
			}
			return x.ValueDigest < y.ValueDigest
		})
	}
	return out
}

// mtP0CheckSameExecutionOracle independently reconstructs every after-tx root,
// the full final-state map and the sorted durable state delta from the *same*
// verified TxDeltas. It is therefore immune to different valid worker schedules
// between separate executions and directly isolates P0 materialization.
func mtP0CheckSameExecutionOracle(t *testing.T, b block.Block, base map[string]string, got Result) {
	t.Helper()
	if len(got.TxDeltas) != len(b.TxList) || len(got.Receipts) != len(b.TxList) {
		t.Fatalf("incomplete ordered output: txdeltas=%d receipts=%d tx=%d", len(got.TxDeltas), len(got.Receipts), len(b.TxList))
	}
	working := make(map[string]string, len(base))
	for k, v := range base {
		working[k] = v
	}
	if got.StateRootBefore != state.RootOfSnapshot(base) {
		t.Fatalf("before root wrong: got=%s full=%s", got.StateRootBefore, state.RootOfSnapshot(base))
	}
	for i, delta := range got.TxDeltas {
		if delta.TxID != b.TxList[i].TxID || delta.OriginalIndex != i {
			t.Fatalf("tx order changed at %d: %+v", i, delta)
		}
		keys := make([]string, 0, len(delta.WriteSet))
		for k := range delta.WriteSet {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			working[qualifyKey(b.ShardID, k)] = delta.WriteSet[k]
		}
		after := state.RootOfSnapshot(working)
		if got.Receipts[i].StateRootAfterTx != after || delta.Receipt.StateRootAfterTx != after {
			t.Fatalf("tx %d root incorrect: receipt=%s delta=%s reference=%s", i, got.Receipts[i].StateRootAfterTx, delta.Receipt.StateRootAfterTx, after)
		}
		if !reflect.DeepEqual(got.Receipts[i], delta.Receipt) {
			t.Fatalf("receipt and transaction delta differ at %d: receipt=%+v delta=%+v", i, got.Receipts[i], delta.Receipt)
		}
	}
	if !reflect.DeepEqual(working, got.StateUpdates) {
		t.Fatalf("full state output differs from same-run reference, sizes=%d/%d", len(working), len(got.StateUpdates))
	}
	if root := state.RootOfSnapshot(working); root != got.StateRootAfter {
		t.Fatalf("final root differs: reference=%s got=%s", root, got.StateRootAfter)
	}
	if receipts := ReceiptRoot(got.Receipts); receipts != got.ReceiptRoot {
		t.Fatalf("receipt root differs: reference=%s got=%s", receipts, got.ReceiptRoot)
	}
	changed := make([]string, 0)
	for k, v := range working {
		if base[k] != v {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	expected := make([]StateUpdate, 0, len(changed))
	for _, key := range changed {
		expected = append(expected, StateUpdate{Key: key, Value: working[key]})
	}
	if !reflect.DeepEqual(expected, got.StateDelta) {
		t.Fatalf("durable sorted delta mismatch: reference=%v got=%v", expected, got.StateDelta)
	}
}

// TestMTP0IncrementalMaterializationEquivalent compares the complete legacy
// Block-STM result against the v4 single-context-only incremental path.
// Result.StateUpdates remains a FULL final-state map, while StateDelta remains
// a canonical sorted changed-key list (including no-op and failure semantics).
func TestMTP0IncrementalMaterializationEquivalent(t *testing.T) {
	for _, size := range []int{0, 97, 4096} {
		for _, workers := range []int{1, 4} {
			t.Run(fmt.Sprintf("state_%d_workers_%d", size, workers), func(t *testing.T) {
				base := map[string]string{
					"s0::balance:alice": "1000000",
					"s0::nonce:alice":   "0",
					"s0::balance:carol": "1000000",
					"s0::nonce:carol":   "0",
				}
				for i := 0; i < size; i++ {
					base[fmt.Sprintf("s0::unrelated:%06d", i)] = fmt.Sprintf("payload-%d", i)
				}
				original := make(map[string]string, len(base))
				for k, v := range base {
					original[k] = v
				}
				items := append([]tx.SignedTransaction{}, mustGenerateForExecutionTest(t, "p0-hot", 5, "alice", "bob", 1, "v5_safe")...)
				items = append(items, mustGenerateForExecutionTest(t, "p0-independent", 3, "carol", "dave", 1, "v5_safe")...)
				items = append(items, withValue(mustGenerateForExecutionTest(t, "p0-failed", 1, "failed-sender", "failed-receiver", 1, "v5_safe"), 0)...)
				items = append(items, tx.SignedTransaction{
					TxID: "p0-direct-1", Sender: "access-a", Receiver: "access-b",
					AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_p0_test",
					AccessList: []tx.AccessItem{{Key: "asset:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "replace"}},
				}, tx.SignedTransaction{
					TxID: "p0-direct-2", Sender: "access-c", Receiver: "access-d",
					AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_p0_test",
					AccessList: []tx.AccessItem{{Key: "asset:shared", Mode: tx.AccessReadWrite, UpdateSemantics: "replace"}},
				}, tx.SignedTransaction{
					TxID: "p0-delta", Sender: "access-e", Receiver: "access-f",
					AccessListSchema: "mbe_workload_record_v3", AccessListSource: "mt_p0_test",
					AccessList: []tx.AccessItem{{Key: "counter:shared", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 3}},
				})
				b := blockForExecutionTest(items)
				ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
				defer cancel()

				// Both executions use exactly the same readiness and stable-prefix
				// callbacks. Only baseCommitment selects the legacy full-root check
				// versus the P0 incremental materializer.
				makeRunner := func() (*BlockSTMExecutor, *[]int) {
					stable := make([]int, 0, len(items))
					ex := NewBlockSTMExecutor(workers)
					ex.ExecutionMode, ex.OracleMode = "performance", "off"
					ex.TxReadiness = func(_ context.Context, _ int) (map[string]string, error) { return nil, nil }
					ex.OnStableTx = func(_ context.Context, i int, delta TxDelta) error {
						if delta.TxID != items[i].TxID {
							return fmt.Errorf("stable callback transaction mismatch at %d", i)
						}
						stable = append(stable, i)
						return nil
					}
					return ex, &stable
				}
				old, oldStable := makeRunner()
				// nil certified commitment forces the pre-P0 materialization path.
				originalResult, err := old.ExecuteBlockWithCommitment(ctx, b, base, nil)
				if err != nil {
					t.Fatalf("legacy full path: %v", err)
				}
				mtP0CheckSameExecutionOracle(t, b, base, originalResult)
				fresh, freshStable := makeRunner()
				result, err := fresh.ExecuteBlockWithCommitment(ctx, b, base, state.NewCommitment(base))
				if err != nil {
					t.Fatalf("incremental path: %v", err)
				}
				mtP0CheckSameExecutionOracle(t, b, base, result)
				for name, stable := range map[string][]int{"legacy": *oldStable, "incremental": *freshStable} {
					if len(stable) != len(items) {
						t.Fatalf("%s stable prefix size %d want %d", name, len(stable), len(items))
					}
					for i, index := range stable {
						if index != i {
							t.Fatalf("%s stable prefix out of order: %v", name, stable)
						}
					}
				}
				if originalResult.StateRootBefore != result.StateRootBefore || originalResult.StateRootAfter != result.StateRootAfter || originalResult.ReceiptRoot != result.ReceiptRoot || originalResult.PlanDigest != result.PlanDigest {
					t.Fatalf("root/plan diverged: old=%s/%s/%s new=%s/%s/%s", originalResult.StateRootBefore, originalResult.StateRootAfter, originalResult.ReceiptRoot, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot)
				}
				if !reflect.DeepEqual(originalResult.Receipts, result.Receipts) {
					t.Fatalf("ordered receipt mismatch: old=%+v new=%+v", originalResult.Receipts, result.Receipts)
				}
				oldLogical, newLogical := mtP0ComparableDeltas(originalResult.TxDeltas), mtP0ComparableDeltas(result.TxDeltas)
				if !reflect.DeepEqual(oldLogical, newLogical) {
					if len(oldLogical) != len(newLogical) {
						t.Fatalf("logical TxDeltas count changed: old=%d new=%d", len(oldLogical), len(newLogical))
					}
					for i := range oldLogical {
						if !reflect.DeepEqual(oldLogical[i], newLogical[i]) {
							t.Fatalf("logical tx %d changed; old=%+v new=%+v", i, oldLogical[i], newLogical[i])
						}
					}
					t.Fatal("logical TxDeltas count changed")
				}
				if !reflect.DeepEqual(originalResult.StateUpdates, result.StateUpdates) || !reflect.DeepEqual(originalResult.StateDelta, result.StateDelta) {
					t.Fatalf("physical state outputs changed: legacy_delta=%v fast_delta=%v", originalResult.StateDelta, result.StateDelta)
				}
				if got := state.RootOfSnapshot(result.StateUpdates); got != result.StateRootAfter {
					t.Fatalf("independent full-snapshot root mismatch: %s vs %s", got, result.StateRootAfter)
				}
				if !reflect.DeepEqual(original, base) {
					t.Fatal("execution modified immutable block-start base")
				}
				if result.StateRootVersion != originalResult.StateRootVersion || result.SuccessfulTxs != originalResult.SuccessfulTxs || result.FailedTxs != originalResult.FailedTxs {
					t.Fatal("result contract changed")
				}
				if result.FailedTxs == 0 {
					t.Fatal("failed-transaction side-effect fixture was not exercised")
				}
			})
		}
	}
}

// Missing authenticated base commitment MUST stay on the checked legacy path.
func TestMTP0NilCommitmentFallbackEquivalent(t *testing.T) {
	base := map[string]string{"s0::balance:alice": "1000000", "s0::nonce:alice": "0"}
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "p0-nil", 4, "alice", "bob", 1, "v5_safe"))
	old := NewBlockSTMExecutor(2)
	old.ExecutionMode, old.OracleMode = "performance", "off"
	original, err := old.ExecuteBlock(context.Background(), b, base)
	if err != nil {
		t.Fatal(err)
	}
	fresh := NewBlockSTMExecutor(2)
	fresh.ExecutionMode, fresh.OracleMode = "performance", "off"
	fresh.TxReadiness = func(_ context.Context, _ int) (map[string]string, error) { return nil, nil }
	fresh.OnStableTx = func(_ context.Context, _ int, _ TxDelta) error { return nil }
	got, err := fresh.ExecuteBlockWithCommitment(context.Background(), b, base, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got.StateRootAfter != original.StateRootAfter || got.ReceiptRoot != original.ReceiptRoot || !reflect.DeepEqual(got.StateDelta, original.StateDelta) || !reflect.DeepEqual(got.StateUpdates, original.StateUpdates) {
		t.Fatal("nil-commitment fallback changed exact results")
	}
}
