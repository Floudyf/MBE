package execution

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestEngineDeterministicExecution(t *testing.T) {
	txs, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "alice", Receiver: "bob", Value: 1, Seed: "exec"})
	if err != nil {
		t.Fatal(err)
	}
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	block.AssignHash(&b)
	db1 := state.NewDB(t.TempDir(), "s0")
	db2 := state.NewDB(t.TempDir(), "s0")
	r1 := NewEngine().ExecuteBlock(b, db1)
	r2 := NewEngine().ExecuteBlock(b, db2)
	if r1.StateRootAfter != r2.StateRootAfter || r1.ReceiptRoot != r2.ReceiptRoot {
		t.Fatalf("execution not deterministic")
	}
	if r1.SuccessfulTxs != 2 || r1.FailedTxs != 0 {
		t.Fatalf("unexpected receipt counts: %+v", r1)
	}
}

func TestSerialBlockExecutorMatchesLegacyEngine(t *testing.T) {
	cases := []struct {
		name  string
		items []tx.SignedTransaction
	}{
		{name: "single_valid_transfer", items: mustGenerateForExecutionTest(t, "single", 1, "alice", "bob", 1, "v5_safe")},
		{name: "multiple_continuous_nonces", items: mustGenerateForExecutionTest(t, "nonces", 4, "alice", "bob", 1, "v5_safe")},
		{name: "nonce_mismatch", items: withNonce(mustGenerateForExecutionTest(t, "nonce-mismatch", 1, "alice", "bob", 1, "v5_safe"), 2)},
		{name: "invalid_value", items: withValue(mustGenerateForExecutionTest(t, "invalid", 1, "alice", "bob", 1, "v5_safe"), 0)},
		{name: "insufficient_balance", items: mustGenerateForExecutionTest(t, "insufficient", 1, "alice", "bob", 2_000_000, "v5_safe")},
		{name: "shared_sender", items: append(mustGenerateForExecutionTest(t, "shared-sender-a", 2, "alice", "bob", 1, "v5_safe"), mustGenerateForExecutionTest(t, "shared-sender-b", 2, "alice", "carol", 1, "v5_safe")...)},
		{name: "shared_receiver", items: append(mustGenerateForExecutionTest(t, "shared-receiver-a", 1, "alice", "carol", 1, "v5_safe"), mustGenerateForExecutionTest(t, "shared-receiver-b", 1, "bob", "carol", 1, "v5_safe")...)},
		{name: "sender_equals_receiver", items: mustGenerateForExecutionTest(t, "self", 1, "alice", "alice", 1, "v5_safe")},
		{name: "new_account_initialization", items: mustGenerateForExecutionTest(t, "new-account", 1, "new-sender", "new-receiver", 1, "v5_safe")},
		{name: "cross_shard_source_transaction", items: mustGenerateForExecutionTest(t, "cross", 2, "alice", "bob", 1, "v5_cross:s0->s1")},
		{name: "relay_transaction", items: withSourceKind(mustGenerateForExecutionTest(t, "relay", 1, "relay-sender", "relay-receiver", 1, "v5_relay"), "relay_certificate")},
		{name: "synthetic_workload", items: mustGenerateForExecutionTest(t, "synthetic", 3, "synthetic-sender", "synthetic-receiver", 1, "v5_safe")},
		{name: "canonical_trace_replay", items: withSourceKind(mustGenerateForExecutionTest(t, "dataset", 3, "dataset-sender", "dataset-receiver", 1, "dataset_event:asset_sale"), "canonical_trace_replay")},
		{name: "hot_key_sequence", items: mustGenerateForExecutionTest(t, "hot", 16, "hot-sender", "hot-receiver", 1, "v5_commutative")},
		{name: "empty_block", items: nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assertSerialEquivalent(t, blockForExecutionTest(tc.items))
		})
	}
}

func TestSerialBlockExecutorDuplicateBlockMatchesLegacy(t *testing.T) {
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "duplicate", 1, "alice", "bob", 1, "v5_safe"))
	legacyDB := state.NewDB(t.TempDir(), "s0")
	serialDB := state.NewDB(t.TempDir(), "s0")
	legacy := NewEngine()
	serial := NewSerialExecutor()
	_ = legacy.ExecuteBlock(b, legacyDB)
	first := serial.ExecuteBlock(b, serialDB.Snapshot())
	serialDB.ApplyDeterministicBatch(toStateKV(first.StateDelta))
	legacySecond := legacy.ExecuteBlock(b, legacyDB)
	serialSecond := serial.ExecuteBlock(b, serialDB.Snapshot())
	serialDB.ApplyDeterministicBatch(toStateKV(serialSecond.StateDelta))
	if !reflect.DeepEqual(legacySecond.Receipts, serialSecond.Receipts) || !reflect.DeepEqual(legacyDB.Snapshot(), serialDB.Snapshot()) {
		t.Fatalf("duplicate execution diverged legacy=%+v serial=%+v", legacySecond, serialSecond)
	}
}

func assertSerialEquivalent(t *testing.T, b block.Block) {
	t.Helper()
	legacyDB := state.NewDB(t.TempDir(), "s0")
	serialDB := state.NewDB(t.TempDir(), "s0")
	legacyResult := NewEngine().ExecuteBlock(b, legacyDB)
	serialResult := NewSerialExecutor().ExecuteBlock(b, serialDB.Snapshot())
	serialDB.ApplyDeterministicBatch(toStateKV(serialResult.StateDelta))
	if !reflect.DeepEqual(legacyResult.Receipts, serialResult.Receipts) {
		t.Fatalf("receipts diverged\nlegacy=%+v\nserial=%+v", legacyResult.Receipts, serialResult.Receipts)
	}
	if legacyResult.StateRootBefore != serialResult.StateRootBefore || legacyResult.StateRootAfter != serialResult.StateRootAfter || legacyResult.ReceiptRoot != serialResult.ReceiptRoot {
		t.Fatalf("roots diverged legacy=%+v serial=%+v", legacyResult, serialResult)
	}
	if legacyResult.SuccessfulTxs != serialResult.SuccessfulTxs || legacyResult.FailedTxs != serialResult.FailedTxs {
		t.Fatalf("counts diverged legacy=%+v serial=%+v", legacyResult, serialResult)
	}
	if !reflect.DeepEqual(legacyDB.Snapshot(), serialDB.Snapshot()) {
		t.Fatalf("final state diverged\nlegacy=%v\nserial=%v", legacyDB.Snapshot(), serialDB.Snapshot())
	}
	if serialResult.Plan.EngineID != SerialBlockExecutorID || serialResult.Plan.WorkerCount != 1 || serialResult.PlanDigest == "" {
		t.Fatalf("missing serial execution plan: %+v", serialResult.Plan)
	}
}

func blockForExecutionTest(items []tx.SignedTransaction) block.Block {
	b := block.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: items}
	for _, item := range items {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	block.AssignHash(&b)
	return b
}

func mustGenerateForExecutionTest(t *testing.T, seed string, count int, sender, receiver string, value int64, payload string) []tx.SignedTransaction {
	t.Helper()
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: count, Sender: sender, Receiver: receiver, Value: value, StateKeys: []string{"account:" + sender, "account:" + receiver}, Seed: seed})
	if err != nil {
		t.Fatal(err)
	}
	for index := range items {
		items[index].Payload = payload
	}
	return items
}

func withNonce(items []tx.SignedTransaction, nonce uint64) []tx.SignedTransaction {
	items[0].Nonce = nonce
	return items
}

func withValue(items []tx.SignedTransaction, value int64) []tx.SignedTransaction {
	items[0].Value = value
	return items
}

func withSourceKind(items []tx.SignedTransaction, sourceKind string) []tx.SignedTransaction {
	for index := range items {
		items[index].SourceKind = sourceKind
	}
	return items
}

func toStateKV(items []StateUpdate) []state.StateKV {
	out := make([]state.StateKV, 0, len(items))
	for _, item := range items {
		out = append(out, state.StateKV{Key: item.Key, Value: item.Value})
	}
	return out
}
