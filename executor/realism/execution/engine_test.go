package execution

import (
	"context"
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution/blockstm"
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
		{name: "cross_shard_target_commit", items: mustGenerateForExecutionTest(t, "target-commit", 1, "relay-sender", "relay-receiver", 1, "v5_cross:s0")},
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

func TestCrossShardTargetCommitDoesNotConsumeSourceNonce(t *testing.T) {
	item := mustGenerateForExecutionTest(t, "target-commit-no-nonce", 1, "relay-sender", "relay-receiver", 1, "v5_cross:s0")[0]
	b := blockForExecutionTest([]tx.SignedTransaction{item})
	serial := NewSerialExecutor().ExecuteBlock(b, nil)
	if serial.SuccessfulTxs != 1 || len(serial.Receipts) != 1 || !serial.Receipts[0].Success {
		t.Fatalf("target commit should be durable-successful: %#v", serial)
	}
	if _, ok := serial.StateUpdates["s0::relay_commit:"+item.TxID]; !ok {
		t.Fatalf("target commit did not record durable marker: %#v", serial.StateUpdates)
	}
	if _, ok := serial.StateUpdates["s0::nonce:"+item.Sender]; ok {
		t.Fatalf("target commit must not consume source sender nonce: %#v", serial.StateUpdates)
	}

	sourceItem := item
	sourceItem.Payload = "v5_cross:s1"
	sourceBlock := blockForExecutionTest([]tx.SignedTransaction{sourceItem})
	source := NewSerialExecutor().ExecuteBlock(sourceBlock, nil)
	if source.StateUpdates["s0::nonce:"+sourceItem.Sender] != "1" {
		t.Fatalf("source-side cross-shard transaction should still consume nonce: %#v", source.StateUpdates)
	}
}

func TestBlockSTMExecutorMatchesSerialAcrossWorkerCounts(t *testing.T) {
	cases := []struct {
		name  string
		items []tx.SignedTransaction
	}{
		{name: "independent", items: append(mustGenerateForExecutionTest(t, "bstm-independent-a", 1, "alice", "bob", 1, "v5_safe"), mustGenerateForExecutionTest(t, "bstm-independent-b", 1, "carol", "dave", 1, "v5_safe")...)},
		{name: "same_sender_continuous_nonces", items: mustGenerateForExecutionTest(t, "bstm-nonce", 4, "alice", "bob", 1, "v5_safe")},
		{name: "nonce_conflict", items: withNonce(mustGenerateForExecutionTest(t, "bstm-nonce-conflict", 2, "alice", "bob", 1, "v5_safe"), 3)},
		{name: "balance_conflict", items: mustGenerateForExecutionTest(t, "bstm-balance", 2, "alice", "bob", 800_000, "v5_safe")},
		{name: "invalid_value", items: withValue(mustGenerateForExecutionTest(t, "bstm-invalid", 1, "alice", "bob", 1, "v5_safe"), 0)},
		{name: "receiver_hotspot", items: append(mustGenerateForExecutionTest(t, "bstm-receiver-a", 1, "alice", "hot", 1, "v5_safe"), mustGenerateForExecutionTest(t, "bstm-receiver-b", 1, "bob", "hot", 1, "v5_safe")...)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := blockForExecutionTest(tc.items)
			serial := NewSerialExecutor().ExecuteBlock(b, map[string]string{})
			for _, workers := range []int{1, 2, 4, 8} {
				executor := NewBlockSTMExecutor(workers)
				got, err := executor.ExecuteBlock(testContext(t), b, map[string]string{})
				if err != nil {
					t.Fatalf("worker %d failed: %v", workers, err)
				}
				if !reflect.DeepEqual(serial.Receipts, got.Receipts) || serial.ReceiptRoot != got.ReceiptRoot || serial.StateRootAfter != got.StateRootAfter || !reflect.DeepEqual(serial.StateDelta, got.StateDelta) {
					t.Fatalf("worker %d diverged\nserial=%+v\ngot=%+v", workers, serial, got)
				}
				if got.BlockExecutorID != BlockSTMExecutorID || got.WorkerCount != workers || got.Plan.WorkerCount != workers || got.PlanDigest == "" {
					t.Fatalf("worker %d missing block-stm identity/plan: %+v", workers, got)
				}
			}
		})
	}
}

func TestBlockSTMExecutorRecordsAbortAndReexecutionOnHotNonceSequence(t *testing.T) {
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "bstm-hot-nonce", 4, "alice", "bob", 1, "v5_safe"))
	executor := NewBlockSTMExecutor(4)
	got, err := executor.ExecuteBlock(testContext(t), b, map[string]string{})
	if err != nil {
		t.Fatal(err)
	}
	serial := NewSerialExecutor().ExecuteBlock(b, map[string]string{})
	if got.StateRootAfter != serial.StateRootAfter || got.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("block-stm not serial equivalent")
	}
	if executor.Metrics.AbortCount == 0 || executor.Metrics.ReexecutionCount == 0 || executor.Metrics.ValidationFailureCount == 0 {
		t.Fatalf("expected real validation abort/reexecution metrics, got %+v", executor.Metrics)
	}
	if got.BlockSTMMetrics.AbortCount != executor.Metrics.AbortCount || got.BlockSTMMetrics.ReexecutionCount != executor.Metrics.ReexecutionCount {
		t.Fatalf("block-stm result did not carry kernel metrics: result=%+v executor=%+v", got.BlockSTMMetrics, executor.Metrics)
	}
	if executor.Metrics.ValidationTaskCount != len(b.TxList) || executor.Metrics.ExecutionTaskCount != len(b.TxList) {
		t.Fatalf("expected execution and validation tasks for every transaction, got %+v", executor.Metrics)
	}
	if executor.Metrics.MaximumParallelWidth < 1 || executor.Metrics.MaximumParallelWidth > executor.Metrics.WorkerCount {
		t.Fatalf("unexpected maximum parallel width: %+v", executor.Metrics)
	}
}

func TestBlockSTMSpeculativeExecutionOrderIsDeterministic(t *testing.T) {
	if got := speculativeExecutionOrder(4, 1); !reflect.DeepEqual(got, []int{0, 1, 2, 3}) {
		t.Fatalf("single worker should execute in serial order, got %#v", got)
	}
	if got := speculativeExecutionOrder(4, 4); !reflect.DeepEqual(got, []int{3, 2, 1, 0}) {
		t.Fatalf("multi-worker speculation should expose higher-index speculation first, got %#v", got)
	}
}

func TestBlockSTMCapturedReadsRecordObservedMVVersion(t *testing.T) {
	memory := blockstm.NewMVMemory()
	memory.Write("balance:alice", blockstm.Version{Txn: 0, Incarnation: 0}, "999999")
	logicalBase := map[string]string{"balance:alice": "1000000"}
	base := map[string]string{"s0::balance:alice": "1000000"}
	overlay := newTxOverlay("s0", speculativeSnapshot(memory, base, logicalBase, "s0", 1))

	if got := overlay.balance("alice"); got != 999999 {
		t.Fatalf("speculative snapshot did not expose lower MV version, got %d", got)
	}
	captured := capturedFromOverlayWithMemory(overlay, memory, logicalBase, 1)

	if len(captured.Reads) != 1 {
		t.Fatalf("expected one captured read, got %#v", captured.Reads)
	}
	read := captured.Reads[0]
	if read.FromBase || read.Version != (blockstm.Version{Txn: 0, Incarnation: 0}) || read.Value != "999999" {
		t.Fatalf("captured read did not preserve observed MV version: %#v", read)
	}
}

func TestBlockSTMExecutorHonorsCanceledContext(t *testing.T) {
	b := blockForExecutionTest(mustGenerateForExecutionTest(t, "bstm-cancel", 8, "alice", "bob", 1, "v5_safe"))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewBlockSTMExecutor(4).ExecuteBlock(ctx, b, map[string]string{}); err == nil {
		t.Fatal("expected canceled context to stop block-stm execution")
	}
}

func TestCommutativeDeltaAccessProducesRealStateUpdateAndBlockSTMEquivalence(t *testing.T) {
	sender := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	receiver := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	accessList := append(tx.DefaultTransferAccessList(sender, receiver), tx.AccessItem{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 3})
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: sender, Receiver: receiver, Value: 1, StateKeys: []string{"account:" + sender, "account:" + receiver, "coaccess:hot-update"}, AccessList: accessList, Seed: "commutative-delta"})
	if err != nil {
		t.Fatal(err)
	}
	b := blockForExecutionTest(items)
	serial := NewSerialExecutor().ExecuteBlock(b, nil)
	blockstmResult, err := NewBlockSTMExecutor(4).ExecuteBlock(testContext(t), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := serial.StateUpdates["s0::coaccess:hot-update"]; got != "6" {
		t.Fatalf("commutative delta did not produce real state update: %q", got)
	}
	assertExecutionEquivalent(t, serial, blockstmResult)
}

func TestPureCommutativeDeltaTransactionSkipsTransferWrites(t *testing.T) {
	sender := "0xcccccccccccccccccccccccccccccccccccccccc"
	receiver := "0xdddddddddddddddddddddddddddddddddddddddd"
	accessList := []tx.AccessItem{{Key: "coaccess:hot-update", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 2}}
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 3, Sender: sender, Receiver: receiver, Value: 1, StateKeys: []string{"coaccess:hot-update"}, AccessList: accessList, Seed: "pure-commutative-delta"})
	if err != nil {
		t.Fatal(err)
	}
	b := blockForExecutionTest(items)
	legacyDB := state.NewDB(t.TempDir(), "s0")
	legacy := NewEngine().ExecuteBlock(b, legacyDB)
	serial := NewSerialExecutor().ExecuteBlock(b, nil)
	blockstmResult, err := NewBlockSTMExecutor(4).ExecuteBlock(testContext(t), b, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := legacyDB.Snapshot()["s0::coaccess:hot-update"]; got != "6" {
		t.Fatalf("legacy pure commutative delta did not update hot key: %q", got)
	}
	for _, key := range []string{"s0::balance:" + sender, "s0::nonce:" + sender, "s0::balance:" + receiver} {
		if value := legacyDB.Snapshot()[key]; value != "" {
			t.Fatalf("pure commutative delta should not write transfer key %s=%q", key, value)
		}
	}
	if !reflect.DeepEqual(legacy.Receipts, serial.Receipts) || legacy.StateRootAfter != serial.StateRootAfter || legacy.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("legacy and serial pure commutative semantics diverged\nlegacy=%+v\nserial=%+v", legacy, serial)
	}
	assertExecutionEquivalent(t, serial, blockstmResult)
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

func assertExecutionEquivalent(t *testing.T, expected, actual Result) {
	t.Helper()
	if !reflect.DeepEqual(expected.Receipts, actual.Receipts) {
		t.Fatalf("receipts diverged\nexpected=%+v\nactual=%+v", expected.Receipts, actual.Receipts)
	}
	if expected.StateRootBefore != actual.StateRootBefore || expected.StateRootAfter != actual.StateRootAfter || expected.ReceiptRoot != actual.ReceiptRoot {
		t.Fatalf("roots diverged expected=%+v actual=%+v", expected, actual)
	}
	if !reflect.DeepEqual(expected.StateDelta, actual.StateDelta) {
		t.Fatalf("state delta diverged\nexpected=%+v\nactual=%+v", expected.StateDelta, actual.StateDelta)
	}
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return ctx
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
