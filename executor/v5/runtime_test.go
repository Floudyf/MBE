package v5

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackRuntimeFetchesRemoteHomeStateBeforeExecution(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	homeAddr := freeLocalAddr(t)
	homeValidatorAddr := freeLocalAddr(t)
	execAddr := freeLocalAddr(t)
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: homeAddr, DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
			{NodeID: "n-s0v", ShardID: "s0", Role: "validator", Leader: false, ListenAddr: homeValidatorAddr, DataDir: filepath.Join(root, "n-s0v"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
			{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: execAddr, DataDir: filepath.Join(root, "n-s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
		},
	}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	homeValidator, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	exec, err := newNodeRuntime(plan, plan.NodeConfigs[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer home.Stop()
	if err := homeValidator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer homeValidator.Stop()
	if err := exec.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer exec.Stop()
	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	home.db.Set(remoteKey, "41")
	item := tx.SignedTransaction{TxID: "tx-remote", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1, StateKeys: []string{remoteKey}, AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	block := realblock.Block{BlockHash: "block-remote", Height: 1, ShardID: "s1", TxList: []tx.SignedTransaction{item}}
	got, err := exec.prepareMetaTrackStateSnapshot(ctx, block, exec.db.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if got["s1::"+remoteKey] != "41" {
		t.Fatalf("remote state value was not injected into execution snapshot: %#v", got)
	}
	home.db.Set(remoteKey, "99")
	replayed := tx.SignedTransaction{TxID: "tx-remote-validator-replay", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1, StateKeys: []string{remoteKey}, AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	response, _, err := exec.fetchRemoteState(ctx, block, replayed, replayed.AccessList[0], "s0")
	if err != nil {
		t.Fatal(err)
	}
	if response.Value != "41" {
		t.Fatalf("same-block remote replay must use the original state witness, got %q", response.Value)
	}
	if len(exec.remoteStateRows) != 1 {
		t.Fatalf("expected one runtime remote state row, got %d", len(exec.remoteStateRows))
	}
	if !transportSaw(home.transport.Log.Entries(), "receive", stateFetchRequestMessage) {
		t.Fatal("home shard did not receive state fetch request over p2p")
	}
	if !transportSaw(exec.transport.Log.Entries(), "receive", stateFetchResponseMessage) {
		t.Fatal("execution shard did not receive state fetch response over p2p")
	}
}

func TestMetaTrackRemoteStateFetchFreezesWholeBlockSnapshot(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	homeAddr := freeLocalAddr(t)
	execAddr := freeLocalAddr(t)
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: homeAddr, DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0"}, PluginProfile: profile},
			{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: execAddr, DataDir: filepath.Join(root, "n-s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
		},
	}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	exec, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer home.Stop()
	if err := exec.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer exec.Stop()

	keyA := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	keyB := ""
	for i := 0; i < 4096; i++ {
		candidate := fmt.Sprintf("metatrack-remote-second-%d", i)
		if candidate != keyA && []string{"s0", "s1"}[stableKey([]string{candidate})%2] == "s0" {
			keyB = candidate
			break
		}
	}
	if keyB == "" {
		t.Fatal("could not find second remote key for s0")
	}
	home.db.Set(keyA, "41")
	home.db.Set(keyB, "50")
	block := realblock.Block{BlockHash: "block-remote-snapshot", Height: 1, ShardID: "s1"}
	first := tx.SignedTransaction{TxID: "tx-remote-a", AccessList: []tx.AccessItem{{Key: keyA, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	firstResponse, _, err := exec.fetchRemoteState(ctx, block, first, first.AccessList[0], "s0")
	if err != nil {
		t.Fatal(err)
	}
	if firstResponse.Value != "41" {
		t.Fatalf("unexpected first witness value: %q", firstResponse.Value)
	}

	home.db.Set(keyB, "99")
	second := tx.SignedTransaction{TxID: "tx-remote-b", AccessList: []tx.AccessItem{{Key: keyB, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	secondResponse, _, err := exec.fetchRemoteState(ctx, block, second, second.AccessList[0], "s0")
	if err != nil {
		t.Fatal(err)
	}
	if secondResponse.Value != "50" {
		t.Fatalf("same-block remote fetch should read the frozen snapshot, got %q", secondResponse.Value)
	}
	replayed := tx.SignedTransaction{TxID: "tx-remote-b-replay", AccessList: []tx.AccessItem{{Key: keyB, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	replayedResponse, _, err := exec.fetchRemoteState(ctx, block, replayed, replayed.AccessList[0], "s0")
	if err != nil {
		t.Fatal(err)
	}
	if replayedResponse.Value != secondResponse.Value {
		t.Fatalf("same-block replay value changed: %q != %q", replayedResponse.Value, secondResponse.Value)
	}
	if replayedResponse.WitnessDigest != secondResponse.WitnessDigest {
		t.Fatalf("same-block replay witness digest should be request-id independent: %q != %q", replayedResponse.WitnessDigest, secondResponse.WitnessDigest)
	}
}

func TestMetaTrackRemoteWritebackPreservesDefaultAccountBalance(t *testing.T) {
	item := tx.SignedTransaction{TxID: "tx-new-account-remote", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1, StateKeys: tx.DefaultStateKeys("alice", "bob"), AccessList: tx.DefaultTransferAccessList("alice", "bob")}
	block := realblock.Block{BlockHash: "remote-new-account-block", Height: 1, ShardID: "s1", TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}}
	result := execution.NewSerialExecutor().ExecuteBlock(block, map[string]string{})
	if len(result.Receipts) != 1 || !result.Receipts[0].Success {
		t.Fatalf("expected standard transfer to succeed from default account state: %#v", result.Receipts)
	}
	annotated := annotateStateDeltaTxIDs(stateKVsFromExecutionDelta(result.StateDelta), result.TxDeltas, block.TxList)
	remote := remoteWritebackItemsForTest(block, annotated, result.TxDeltas)
	remoteByKey := map[string]state.StateKV{}
	for _, item := range remote {
		remoteByKey[item.Key] = item
	}
	if item := remoteByKey["balance:alice"]; item.ApplyOrigin != "metatrack_remote_state" || item.DeltaKind != "account_balance_delta" || !item.HasInitialValue || item.InitialValue != 1_000_000 {
		t.Fatalf("sender balance remote delta missing explicit account init metadata: %#v", item)
	}
	if item := remoteByKey["nonce:alice"]; item.ApplyOrigin != "metatrack_remote_state" || item.DeltaKind != "account_nonce_delta" || !item.HasInitialValue || item.InitialValue != 0 {
		t.Fatalf("sender nonce remote delta missing explicit nonce init metadata: %#v", item)
	}
	if item := remoteByKey["balance:bob"]; item.ApplyOrigin != "metatrack_remote_state" || item.DeltaKind != "account_balance_delta" || !item.HasInitialValue || item.InitialValue != 0 {
		t.Fatalf("receiver balance remote delta should declare zero initial account value: %#v", item)
	}

	applied, err := applyStateDeltaToSnapshot(map[string]string{}, remote, "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := applied["s0::balance:alice"]; got != "999999" {
		t.Fatalf("missing sender balance must use default initial balance before debit, got %q", got)
	}
	if got := applied["s0::nonce:alice"]; got != "1" {
		t.Fatalf("missing sender nonce must advance from zero, got %q", got)
	}
	if got := applied["s0::balance:bob"]; got != "1" {
		t.Fatalf("missing receiver balance must advance from zero, got %q", got)
	}
	if sum := mustParseStateInt(t, applied["s0::balance:alice"]) + mustParseStateInt(t, applied["s0::balance:bob"]); sum != 1_000_000 {
		t.Fatalf("remote default-account transfer did not conserve balance, sum=%d snapshot=%#v", sum, applied)
	}
	ordinary, err := applyStateDeltaToSnapshot(map[string]string{}, []state.StateKV{{Key: "balance:charlie", UpdateSemantics: "commutative_delta", Delta: -1}}, "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := ordinary["s0::balance:charlie"]; got != "-1" {
		t.Fatalf("unmarked balance-like delta should not receive account default, got %q", got)
	}
	contract, err := applyStateDeltaToSnapshot(map[string]string{}, []state.StateKV{{Key: "contract:balance:shadow", UpdateSemantics: "commutative_delta", Delta: -1}}, "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if got := contract["s0::contract:balance:shadow"]; got != "-1" {
		t.Fatalf("ordinary non-account key should not receive account default, got %q", got)
	}
}

func TestMetaTrackRemoteAccountDeltaInitialValueIgnoresObservedCurrentValue(t *testing.T) {
	transaction := tx.SignedTransaction{TxID: "tx-observed-current", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1}
	txDeltas := []execution.TxDelta{{
		TxID:          transaction.TxID,
		OriginalIndex: 0,
		Success:       true,
		ReadSet: []execution.ReadObservation{
			{Key: "s1::balance:alice", Value: "999999", ValueDigest: stateValueDigest("999999"), Source: "snapshot"},
			{Key: "s1::nonce:alice", Value: "7", ValueDigest: stateValueDigest("7"), Source: "snapshot"},
			{Key: "s1::balance:bob", Value: "42", ValueDigest: stateValueDigest("42"), Source: "snapshot"},
		},
	}}
	transactions := []tx.SignedTransaction{transaction}
	cases := []struct {
		name         string
		item         state.StateKV
		key          string
		initialValue int64
	}{
		{name: "sender balance debit", key: "balance:alice", item: state.StateKV{Key: "s1::balance:alice", Value: "999998", TxIDs: []string{transaction.TxID}, UpdateSemantics: "commutative_delta", Delta: -1}, initialValue: 1_000_000},
		{name: "receiver balance credit", key: "balance:bob", item: state.StateKV{Key: "s1::balance:bob", Value: "43", TxIDs: []string{transaction.TxID}, UpdateSemantics: "commutative_delta", Delta: 1}, initialValue: 0},
		{name: "sender nonce increment", key: "nonce:alice", item: state.StateKV{Key: "s1::nonce:alice", Value: "8", TxIDs: []string{transaction.TxID}, UpdateSemantics: "commutative_delta", Delta: 1}, initialValue: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := metaTrackRemoteWritebackItem(tc.item, tc.key, transactions, txDeltas)
			if got.ApplyOrigin != "metatrack_remote_state" || !got.HasInitialValue || got.InitialValue != tc.initialValue {
				t.Fatalf("remote account delta InitialValue used observed current value: %#v", got)
			}
		})
	}
}

func TestMetaTrackRemoteWritebackDoesNotLoseTwoDebitsFromSameOldBalance(t *testing.T) {
	first := tx.SignedTransaction{TxID: "tx-remote-debit-1", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1, StateKeys: tx.DefaultStateKeys("alice", "bob"), AccessList: tx.DefaultTransferAccessList("alice", "bob")}
	second := tx.SignedTransaction{TxID: "tx-remote-debit-2", Sender: "alice", Receiver: "bob", Nonce: 1, Value: 1, StateKeys: tx.DefaultStateKeys("alice", "bob"), AccessList: tx.DefaultTransferAccessList("alice", "bob")}
	blockA := realblock.Block{BlockHash: "remote-old-balance-a", Height: 1, ShardID: "s1", TxIDs: []string{first.TxID}, TxList: []tx.SignedTransaction{first}}
	blockB := realblock.Block{BlockHash: "remote-old-balance-b", Height: 2, ShardID: "s1", TxIDs: []string{second.TxID}, TxList: []tx.SignedTransaction{second}}
	resultA := execution.NewSerialExecutor().ExecuteBlock(blockA, map[string]string{"s1::balance:alice": "100", "s1::nonce:alice": "0"})
	resultB := execution.NewSerialExecutor().ExecuteBlock(blockB, map[string]string{"s1::balance:alice": "100", "s1::nonce:alice": "1"})
	if !resultA.Receipts[0].Success || !resultB.Receipts[0].Success {
		t.Fatalf("expected both remote transfers to succeed, receipts=%#v %#v", resultA.Receipts, resultB.Receipts)
	}
	remote := append(
		remoteWritebackItemsForTest(blockA, annotateStateDeltaTxIDs(stateKVsFromExecutionDelta(resultA.StateDelta), resultA.TxDeltas, blockA.TxList), resultA.TxDeltas),
		remoteWritebackItemsForTest(blockB, annotateStateDeltaTxIDs(stateKVsFromExecutionDelta(resultB.StateDelta), resultB.TxDeltas, blockB.TxList), resultB.TxDeltas)...,
	)

	applied, err := applyStateDeltaToSnapshot(map[string]string{"s0::balance:alice": "100", "s0::nonce:alice": "0"}, remote, "s0", 2)
	if err != nil {
		t.Fatal(err)
	}
	if got := applied["s0::balance:alice"]; got != "98" {
		t.Fatalf("two stale absolute debit writebacks must not collapse to one debit, got sender balance %q snapshot=%#v", got, applied)
	}
	if got := applied["s0::nonce:alice"]; got != "2" {
		t.Fatalf("two stale nonce writebacks must not collapse to one increment, got nonce %q snapshot=%#v", got, applied)
	}
	if got := applied["s0::balance:bob"]; got != "2" {
		t.Fatalf("receiver remote deltas should aggregate, got %q snapshot=%#v", got, applied)
	}
	if sum := mustParseStateInt(t, applied["s0::balance:alice"]) + mustParseStateInt(t, applied["s0::balance:bob"]); sum != 100 {
		t.Fatalf("remote writeback lost balance conservation, sum=%d snapshot=%#v", sum, applied)
	}
}

func TestMetaTrackRemoteStateDeltaIdempotentPendingAndReadySet(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	request := StateDeltaApplyRequest{RequestID: "apply-idempotent-a", TxID: "tx-idempotent", TxIDs: []string{"tx-idempotent"}, BlockHash: "remote-idempotent-block", Key: "balance:alice", Value: "99", UpdateSemantics: "commutative_delta", Delta: -1, HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::balance:alice", SourceHeight: 1}
	duplicate := request
	duplicate.RequestID = "apply-idempotent-b"

	if ack := home.handleStateDeltaApplyRequest(request); !ack.Success {
		t.Fatalf("first remote delta should be accepted: %#v", ack)
	}
	if ack := home.handleStateDeltaApplyRequest(duplicate); !ack.Success {
		t.Fatalf("duplicate remote delta should be idempotently acknowledged: %#v", ack)
	}
	if len(home.pendingStateDeltas) != 1 {
		t.Fatalf("duplicate remote delta entered pending queue more than once: %#v", home.pendingStateDeltas)
	}
	ready := home.readyRemoteStateDeltasForConsensus(1)
	if len(ready) != 1 {
		t.Fatalf("duplicate remote delta entered consensus-ready set more than once: %#v", ready)
	}
	if ready[0].DeltaID != stateDeltaApplyKey(request) {
		t.Fatalf("ready delta id changed across duplicate delivery: %#v", ready[0])
	}
	home.markRemoteStateDeltasApplied(ready)
	if len(home.pendingStateDeltas) != 0 {
		t.Fatalf("applied remote delta was not removed from pending queue: %#v", home.pendingStateDeltas)
	}
	if ack := home.handleStateDeltaApplyRequest(duplicate); !ack.Success {
		t.Fatalf("already-applied duplicate should stay idempotent: %#v", ack)
	}
	if len(home.pendingStateDeltas) != 0 {
		t.Fatalf("already-applied duplicate re-entered pending queue: %#v", home.pendingStateDeltas)
	}
}

func TestMetaTrackRemoteWritebackMatchesSerialAndBlockSTMBackends(t *testing.T) {
	item := tx.SignedTransaction{TxID: "tx-backend-equivalent", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1, StateKeys: tx.DefaultStateKeys("alice", "bob"), AccessList: tx.DefaultTransferAccessList("alice", "bob")}
	block := realblock.Block{BlockHash: "remote-backend-equivalent-block", Height: 1, ShardID: "s1", TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}}
	base := map[string]string{"s1::balance:alice": "100", "s1::nonce:alice": "0"}
	serialResult := execution.NewSerialExecutor().ExecuteBlock(block, base)
	stmResult, err := execution.NewBlockSTMExecutor(2).ExecuteBlock(context.Background(), block, base)
	if err != nil {
		t.Fatal(err)
	}
	if len(serialResult.Receipts) != len(stmResult.Receipts) || len(serialResult.TxDeltas) != len(stmResult.TxDeltas) {
		t.Fatalf("backend outputs should have matching receipt and tx delta counts: serial=%#v stm=%#v", serialResult, stmResult)
	}
	serialRemote := remoteWritebackItemsForTest(block, annotateStateDeltaTxIDs(stateKVsFromExecutionDelta(serialResult.StateDelta), serialResult.TxDeltas, block.TxList), serialResult.TxDeltas)
	stmRemote := remoteWritebackItemsForTest(block, annotateStateDeltaTxIDs(stateKVsFromExecutionDelta(stmResult.StateDelta), stmResult.TxDeltas, block.TxList), stmResult.TxDeltas)
	if fmt.Sprint(serialRemote) != fmt.Sprint(stmRemote) {
		t.Fatalf("MetaTrack remote writeback should be backend-equivalent:\nserial=%#v\nstm=%#v", serialRemote, stmRemote)
	}
	serialApplied, err := applyStateDeltaToSnapshot(map[string]string{"s0::balance:alice": "100", "s0::nonce:alice": "0"}, serialRemote, "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	stmApplied, err := applyStateDeltaToSnapshot(map[string]string{"s0::balance:alice": "100", "s0::nonce:alice": "0"}, stmRemote, "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	if state.RootOfSnapshot(serialApplied) != state.RootOfSnapshot(stmApplied) {
		t.Fatalf("MetaTrack backends produced different remote logical state roots: serial=%#v stm=%#v", serialApplied, stmApplied)
	}
	if got := serialApplied["s0::balance:alice"]; got != "99" {
		t.Fatalf("sender balance mismatch after backend-equivalent remote writeback: %q", got)
	}
}

func TestMetaTrackRemoteNonCommutativeSetUsesBaseDigestCAS(t *testing.T) {
	updates := []state.StateKV{
		{Key: "object:parcel-1", Value: "new-owner", BaseValue: "old-owner", BaseValueDigest: stateValueDigest("old-owner")},
		{Key: "object:parcel-1", Value: "stale-owner", BaseValue: "old-owner", BaseValueDigest: stateValueDigest("old-owner")},
	}
	applied, err := applyStateDeltaToSnapshot(map[string]string{"s0::object:parcel-1": "old-owner"}, updates[:1], "s0", 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = applyStateDeltaToSnapshot(applied, updates[1:], "s0", 1)
	if err == nil {
		t.Fatal("stale non-commutative set should return CAS mismatch instead of silently skipping")
	}
	if got := applied["s0::object:parcel-1"]; got != "new-owner" {
		t.Fatalf("stale non-commutative set overwrote newer value, got %q snapshot=%#v", got, applied)
	}

	db := state.NewDB(t.TempDir(), "s0")
	db.Set("object:parcel-1", "old-owner")
	if err := db.ApplyDeterministicBatch(updates[:1]); err != nil {
		t.Fatal(err)
	}
	if err := db.ApplyDeterministicBatch(updates[1:]); err == nil {
		t.Fatal("DB batch apply should return CAS mismatch for stale set")
	}
	if got := db.Get("object:parcel-1"); got != "new-owner" {
		t.Fatalf("DB batch apply allowed stale non-commutative set overwrite, got %q", got)
	}
}

func TestSystemDeltaDrainBlockCommitsPendingMarkerThroughConsensusPath(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	request := StateDeltaApplyRequest{RequestID: "drain-marker", TxID: "tx-relay", TxIDs: []string{"tx-relay"}, BlockHash: "target-block", Key: "relay_commit:tx-relay", Value: "1", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::relay_commit:tx-relay", SourceHeight: 1}
	if ack := home.handleStateDeltaApplyRequest(request); !ack.Success {
		t.Fatalf("system delta request rejected: %#v", ack)
	}
	ready := home.readyRemoteStateDeltasForConsensus(1)
	if len(ready) != 1 {
		t.Fatalf("expected one ready system delta, got %#v", ready)
	}
	block := home.buildSystemDeltaDrainBlock(nowForTest(), ready)
	if len(block.TxList) != 0 || len(block.SystemStateDeltas) != 1 {
		t.Fatalf("drain block should contain only system deltas: %#v", block)
	}
	if _, err := home.commitOnce(context.Background(), block, CommitOriginConsensus); err != nil {
		t.Fatal(err)
	}
	if got := home.db.Get("relay_commit:tx-relay"); got != "1" {
		t.Fatalf("drain block did not commit marker: %q", got)
	}
	if len(home.pendingStateDeltas) != 0 || len(home.pendingStateDeltaKeys) != 0 {
		t.Fatalf("drain block did not clear pending deltas: pending=%#v keys=%#v", home.pendingStateDeltas, home.pendingStateDeltaKeys)
	}
	if len(home.lifecycle) != 0 {
		t.Fatalf("system-only drain block should not create logical tx lifecycle rows: %#v", home.lifecycle)
	}
	if len(home.blockExecutionSummaries) == 0 || intFromAny(home.blockExecutionSummaries[len(home.blockExecutionSummaries)-1]["system_delta_drain_block_count"]) != 1 {
		t.Fatalf("drain block summary missing audit count: %#v", home.blockExecutionSummaries)
	}
}

func TestSystemDeltaDrainDoesNotProduceWhenNoPendingDelta(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	ready := home.readyRemoteStateDeltasForConsensus(1)
	if len(ready) != 0 {
		t.Fatalf("unexpected ready deltas: %#v", ready)
	}
	input := BlockProductionInput{Pool: home.pool, Proposer: home.proposer, Limit: home.blockSize(), Now: nowForTest(), SystemDeltaReady: len(ready) > 0}
	if home.plugins.BlockProducer.ShouldProduce(input) {
		t.Fatal("empty mempool with no system delta should not produce a drain block")
	}
}

func TestRuntimeScheduleBlockUsesSchedulerPluginAndRehashesProposal(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["block_executor"] = PluginConfig{PluginID: "metatrack_block_executor", Config: map[string]any{"worker_count": 2}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	conservative := tx.SignedTransaction{TxID: "conservative"}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{conservative.TxID, fast.TxID}, TxList: []tx.SignedTransaction{conservative, fast}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	originalHash := block.BlockHash

	scheduled := runtime.scheduleBlock(block)

	if scheduled.TxIDs[0] != "conservative" || scheduled.TxIDs[1] != "fast" {
		t.Fatalf("metatrack proposal should preserve consensus order and defer execution ordering to block executor: %#v", scheduled.TxIDs)
	}
	if scheduled.ExecutionPlan == nil || scheduled.ExecutionPlan.PayloadDigest == "" {
		t.Fatalf("metatrack proposal did not bind an execution plan: %#v", scheduled.ExecutionPlan)
	}
	if scheduled.BlockHash == originalHash {
		t.Fatal("metatrack execution plan must be bound into the block hash")
	}
	if len(runtime.schedulerRows) == 0 {
		t.Fatal("runtime did not record scheduler trace rows")
	}
	if !schedulerRowsSaw(runtime.schedulerRows, "fast", "planned_ready", false, false) {
		t.Fatalf("runtime scheduler trace missing planned fast-ready evidence: %#v", runtime.schedulerRows)
	}
	if !schedulerRowsCarryQueueDepths(runtime.schedulerRows) {
		t.Fatalf("runtime scheduler trace missing queue depth columns: %#v", runtime.schedulerRows)
	}
}

func TestRuntimeScheduleBlockBindsMetaTrackPlanForBlockSTMBackend(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off"}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	item := tx.SignedTransaction{TxID: "metatrack-stm", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)

	scheduled := runtime.scheduleBlock(block)

	if scheduled.ExecutionPlan == nil || scheduled.ExecutionPlan.PayloadDigest == "" {
		t.Fatalf("metatrack block-stm proposal did not bind an execution plan: %#v", scheduled.ExecutionPlan)
	}
	if err := runtime.verifyExecutionPlanEnvelope(scheduled); err != nil {
		t.Fatalf("validator should accept independently recomputed metatrack block-stm plan: %v", err)
	}
}

func TestRuntimeValidatorRejectsSemanticallyTamperedMetaTrackPlan(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off"}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	item := tx.SignedTransaction{TxID: "metatrack-tamper", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	scheduled := runtime.scheduleBlock(block)

	var payload map[string]any
	if err := json.Unmarshal(scheduled.ExecutionPlan.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	payload["remote_access_estimate"] = float64(999)
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	scheduled.ExecutionPlan.Payload = raw
	scheduled.ExecutionPlan.PayloadDigest = stableTextDigest(string(raw))
	scheduled.ExecutionPlan.PlanDigest = scheduled.ExecutionPlan.PayloadDigest
	realblock.AssignHash(&scheduled)

	if err := runtime.verifyExecutionPlanEnvelope(scheduled); err == nil || !strings.Contains(err.Error(), "semantic recompute mismatch") {
		t.Fatalf("validator should reject digest-self-consistent semantic plan tamper, got %v", err)
	}
}

func TestRuntimeRejectsMetaTrackPlanExecutionShardMismatch(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 4, "execution_mode": "performance", "oracle_mode": "off"}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	item := tx.SignedTransaction{TxID: "metatrack-plan-drive", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	scheduled := runtime.scheduleBlock(block)
	if err := runtime.validateMetaTrackPlanDrivesExecution(scheduled); err != nil {
		t.Fatalf("expected local plan execution shard to pass: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal(scheduled.ExecutionPlan.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	placements := payload["transaction_placements"].([]any)
	first := placements[0].(map[string]any)
	first["ExecutionShard"] = "s1"
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	scheduled.ExecutionPlan.Payload = raw

	if err := runtime.validateMetaTrackPlanDrivesExecution(scheduled); err == nil || !strings.Contains(err.Error(), "plan drives execution mismatch") {
		t.Fatalf("expected execution shard mismatch, got %v", err)
	}
}

func TestRuntimeScheduleTraceSeparatesRemoteHomeAccessFromStolenWork(t *testing.T) {
	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0"}, PluginProfile: profile},
			{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n-s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
		},
	}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	item := tx.SignedTransaction{TxID: "remote-home", AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	block := realblock.Block{ShardID: "s1", Height: 1, PreviousHash: "genesis", ProposerID: "n-s1", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)

	runtime.scheduleBlock(block)

	if schedulerRowsSawStolen(runtime.schedulerRows, "remote-home") {
		t.Fatalf("remote-home execution must not be reported as stolen work: %#v", runtime.schedulerRows)
	}
	if !schedulerRowsSawReason(runtime.schedulerRows, "remote-home", "remote_home_access") {
		t.Fatalf("runtime scheduler trace should record remote-home access separately: %#v", runtime.schedulerRows)
	}
}

func TestMetaTrackRuntimeRecordsRemoteStateDeltaEvidenceWithoutBypassingConsensus(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	homeAddr := freeLocalAddr(t)
	homeValidatorAddr := freeLocalAddr(t)
	execAddr := freeLocalAddr(t)
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: homeAddr, DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
			{NodeID: "n-s0v", ShardID: "s0", Role: "validator", Leader: false, ListenAddr: homeValidatorAddr, DataDir: filepath.Join(root, "n-s0v"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
			{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: execAddr, DataDir: filepath.Join(root, "n-s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
		},
	}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	homeValidator, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	exec, err := newNodeRuntime(plan, plan.NodeConfigs[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer home.Stop()
	if err := homeValidator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer homeValidator.Stop()
	if err := exec.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer exec.Stop()
	home.committedHeight = 2
	homeValidator.committedHeight = 2
	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	block := realblock.Block{BlockHash: "block-apply", Height: 1, ShardID: "s1"}

	localDelta, err := exec.applyMetaTrackRemoteDeltas(ctx, block, []state.StateKV{{Key: "s1::" + remoteKey, Value: "99", TxIDs: []string{"tx-remote-apply"}}})
	if err != nil {
		t.Fatal(err)
	}

	if len(localDelta) != 0 {
		t.Fatalf("remote home delta must not remain in execution-shard consensus delta, got %#v", localDelta)
	}
	if got := home.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard should not apply remote delta outside consensus, got %q", got)
	}
	if got := homeValidator.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard validator should not apply remote delta outside consensus, got %q", got)
	}
	home.applyQueuedStateDeltas()
	homeValidator.applyQueuedStateDeltas()
	if got := home.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard applied remote delta outside consensus, got %q", got)
	}
	if got := homeValidator.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard validator applied remote delta outside consensus, got %q", got)
	}
	if len(exec.remoteStateRows) != 2 || exec.remoteStateRows[0][10] != "write_apply" || exec.remoteStateRows[1][10] != "write_apply" {
		t.Fatalf("expected write_apply remote state rows for every home shard node, got %#v", exec.remoteStateRows)
	}
	if exec.remoteStateRows[0][5] != "tx-remote-apply" {
		t.Fatalf("remote state apply row lost logical tx provenance: %#v", exec.remoteStateRows[0])
	}
	localDelta, err = exec.applyMetaTrackRemoteDeltas(ctx, block, []state.StateKV{{Key: "s1::" + remoteKey, Value: "102", TxIDs: []string{"tx-remote-delta"}, UpdateSemantics: "commutative_delta", Delta: 3}})
	if err != nil {
		t.Fatal(err)
	}
	if len(localDelta) != 0 {
		t.Fatalf("remote commutative delta must not remain execution-shard local: %#v", localDelta)
	}
	home.applyQueuedStateDeltas()
	homeValidator.applyQueuedStateDeltas()
	if got := home.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard applied commutative remote delta outside consensus, got %q", got)
	}
	if got := homeValidator.db.Get(remoteKey); got != "" {
		t.Fatalf("home shard validator applied commutative remote delta outside consensus, got %q", got)
	}
	if exec.remoteStateRows[2][10] != "write_apply:commutative_delta" || exec.remoteStateRows[3][10] != "write_apply:commutative_delta" {
		t.Fatalf("expected commutative remote apply evidence, got %#v", exec.remoteStateRows)
	}
	if !transportSaw(home.transport.Log.Entries(), "receive", stateDeltaApplyMessage) {
		t.Fatal("home shard did not receive state delta apply over p2p")
	}
	if !transportSaw(exec.transport.Log.Entries(), "receive", stateDeltaApplyAckMessage) {
		t.Fatal("execution shard did not receive state delta apply ack over p2p")
	}
}

func TestSummarizeRemoteStateRowsClassifiesWriteApplyPrefixes(t *testing.T) {
	row := func(kind, success, latency string) []string {
		return []string{"1", "n0", "s1", "1", "b1", "tx", "k", "s0::k", "s0", "s1", kind, latency, "w", "root", success, ""}
	}
	summary := summarizeRemoteStateRows([][]string{
		row("read", "true", "1"),
		row("read_write", "true", "2"),
		row("commutative_delta", "true", "3"),
		row("write_apply", "true", "4"),
		row("write_apply:commutative_delta", "true", "5"),
		row("future_kind", "true", "6"),
		row("read", "false", "7"),
	})

	if summary.total != 6 || summary.reads != 3 || summary.writes != 2 || summary.unknown != 1 || summary.failed != 1 {
		t.Fatalf("unexpected remote state summary: %#v", summary)
	}
	if summary.avgLatency != 3.5 {
		t.Fatalf("unexpected average latency: %v", summary.avgLatency)
	}
}

func TestSummarizeMethodRowsUsesUnifiedAggregationMetrics(t *testing.T) {
	executionRows := [][]string{
		{"1", "n0", "s0", "tx-a", "1", "dual_track_execution", "fast", "independent"},
		{"2", "n0", "s0", "tx-b", "1", "dual_track_execution", "conservative", "conflict"},
		{"3", "n0", "s0", "tx-b", "1", "dual_track_execution", "conservative", "validator_replay"},
	}
	commitRows := [][]string{
		{"1", "n0", "s0", "1", "normal_commit", "", "2", "5", "false", "5", "5", "0", "0"},
		{"2", "n0", "s0", "2", "commutative_hot_update_aggregation", "s0:2", "4", "2", "true", "4", "2", "1", "3"},
	}

	summary := summarizeMethodRows(executionRows, commitRows)

	if summary.fastTrackCount != 1 || summary.conservativeTrackCount != 2 {
		t.Fatalf("unexpected track counts: %#v", summary)
	}
	if summary.executedLogicalTransactionCount != 2 || summary.executedTransactionInstanceCount != 3 {
		t.Fatalf("unexpected execution counts: %#v", summary)
	}
	if summary.preAggregationPhysicalOps != 9 || summary.postAggregationPhysicalOps != 7 {
		t.Fatalf("pre/post aggregation metrics must use state-op units: %#v", summary)
	}
	if summary.aggregatedKeyCount != 1 || summary.aggregatedLogicalDeltaCount != 3 {
		t.Fatalf("aggregation evidence was not summarized: %#v", summary)
	}
	if summary.physicalOpsSavedCount() != 2 || summary.aggregationReductionRatio() != float64(2)/float64(9) {
		t.Fatalf("unexpected reduction metrics: saved=%d ratio=%f", summary.physicalOpsSavedCount(), summary.aggregationReductionRatio())
	}
}

func TestSummarizeBlockProductionRowsUsesCommittedChainEvidence(t *testing.T) {
	rows := [][]string{
		{"n0", "s0", "1", "0", "b1", "genesis", "100", "txroot", "before", "after", "receipt", "1000", "1010"},
		{"n0", "s0", "2", "0", "b2", "b1", "80", "txroot", "before", "after", "receipt", "1070", "1085"},
		{"n0", "s0", "3", "0", "b3", "b2", "20", "txroot", "before", "after", "receipt", "1170", "1210"},
	}

	summary := summarizeBlockProductionRows(rows)

	if summary.count != 3 || summary.averageTxPerBlock != float64(200)/3 || summary.minTxPerBlock != 20 || summary.maxTxPerBlock != 100 {
		t.Fatalf("unexpected block tx statistics: %#v", summary)
	}
	if summary.intervalMeanMS != 100 || summary.intervalP95MS != 75 {
		t.Fatalf("unexpected block interval statistics: %#v", summary)
	}
}

func TestMetaTrackRemoteStateDeltaSideEffectsAreLeaderOnly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	homeAddr := freeLocalAddr(t)
	validatorAddr := freeLocalAddr(t)
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: homeAddr, DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0"}, PluginProfile: profile},
			{NodeID: "n-s1v", ShardID: "s1", Role: "validator", Leader: false, ListenAddr: validatorAddr, DataDir: filepath.Join(root, "n-s1v"), Validators: []string{"n-s1", "n-s1v"}, PluginProfile: profile},
		},
	}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	validator, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer home.Stop()
	if err := validator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer validator.Stop()
	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	block := realblock.Block{BlockHash: "block-validator-remote-apply", Height: 1, ShardID: "s1"}

	localDelta, err := validator.applyMetaTrackRemoteDeltas(ctx, block, []state.StateKV{{Key: "s1::" + remoteKey, Value: "99", TxIDs: []string{"tx-validator-remote-apply"}}})
	if err != nil {
		t.Fatal(err)
	}

	if len(localDelta) != 0 {
		t.Fatalf("validator must not retain remote-home delta as local authority: %#v", localDelta)
	}
	if got := home.db.Get(remoteKey); got != "" {
		t.Fatalf("validator emitted remote state side effect, home value %q", got)
	}
	if len(validator.remoteStateRows) != 0 {
		t.Fatalf("validator should not record outbound remote write_apply rows: %#v", validator.remoteStateRows)
	}
}

func TestMetaTrackRemoteStateDeltaQueuesUntilHomeHeightBarrier(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	request := StateDeltaApplyRequest{RequestID: "apply-queued", TxID: "tx-queued", TxIDs: []string{"tx-queued"}, BlockHash: "remote-block", Key: "balance:alice", Value: "99", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::balance:alice", SourceHeight: 2}

	ack := home.handleStateDeltaApplyRequest(request)
	if !ack.Success {
		t.Fatalf("queued delta should be acknowledged as reliably received: %#v", ack)
	}
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("remote delta crossed the height barrier early, got %q", got)
	}
	if len(home.pendingStateDeltas) != 1 {
		t.Fatalf("remote delta should queue for the next home-shard consensus block, got %d", len(home.pendingStateDeltas))
	}

	home.committedHeight = 1
	home.applyQueuedStateDeltas()
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("remote delta applied before source height was reached, got %q", got)
	}

	home.committedHeight = 2
	home.applyQueuedStateDeltas()
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("remote delta applied before deterministic lag barrier, got %q", got)
	}

	home.applyQueuedStateDeltas()
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("remote delta bypassed consensus state after height barrier, got %q", got)
	}
	if len(home.pendingStateDeltas) != 1 {
		t.Fatalf("queued remote delta must remain pending until a home-shard block commits it: %#v", home.pendingStateDeltas)
	}
}

func TestMetaTrackRemoteStateDeltaCommitsOnHomeShardConsensusPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n-s0"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
		{NodeID: "n-s0v", ShardID: "s0", Role: "validator", Leader: false, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n-s0v"), Validators: []string{"n-s0", "n-s0v"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n-s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	homeLeader, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	homeValidator, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	execShard, err := newNodeRuntime(plan, plan.NodeConfigs[2])
	if err != nil {
		t.Fatal(err)
	}
	if err := homeLeader.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer homeLeader.Stop()
	if err := homeValidator.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer homeValidator.Stop()
	if err := execShard.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer execShard.Stop()
	remoteKey := keyWithHomeShardPrefix(t, "s0", []string{"s0", "s1"}, "balance:")
	account := strings.TrimPrefix(remoteKey, "balance:")
	sourceBlock := realblock.Block{BlockHash: "remote-source-block", Height: 1, ShardID: "s1"}
	localDelta, err := execShard.applyMetaTrackRemoteDeltas(ctx, sourceBlock, []state.StateKV{{Key: "s1::" + remoteKey, Value: "99", TxIDs: []string{"remote-credit"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(localDelta) != 0 {
		t.Fatalf("execution shard retained remote home-state authority: %#v", localDelta)
	}
	if got := execShard.db.Get(remoteKey); got != "" {
		t.Fatalf("execution shard kept an authoritative remote key copy: %q", got)
	}

	requestA := StateDeltaApplyRequest{RequestID: "remote-credit-a", TxID: "remote-credit", TxIDs: []string{"remote-credit"}, BlockHash: sourceBlock.BlockHash, Key: remoteKey, Value: "99", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + remoteKey, SourceHeight: 1}
	requestDuplicate := requestA
	requestDuplicate.RequestID = "remote-credit-duplicate"
	requestB := StateDeltaApplyRequest{RequestID: "remote-bonus", TxID: "remote-bonus", TxIDs: []string{"remote-bonus"}, BlockHash: sourceBlock.BlockHash, Key: remoteKey, Value: "104", UpdateSemantics: "commutative_delta", Delta: 5, HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + remoteKey, SourceHeight: 1}
	for _, request := range []StateDeltaApplyRequest{requestA, requestDuplicate, requestB} {
		if ack := homeLeader.handleStateDeltaApplyRequest(request); !ack.Success {
			t.Fatalf("leader rejected remote delta: %#v", ack)
		}
	}
	for _, request := range []StateDeltaApplyRequest{requestB, requestDuplicate, requestA} {
		if ack := homeValidator.handleStateDeltaApplyRequest(request); !ack.Success {
			t.Fatalf("validator rejected remote delta: %#v", ack)
		}
	}
	if got := homeLeader.db.Get(remoteKey); got != "" {
		t.Fatalf("leader applied remote delta before consensus: %q", got)
	}
	if got := homeValidator.db.Get(remoteKey); got != "" {
		t.Fatalf("validator applied remote delta before consensus: %q", got)
	}
	readyLeader := homeLeader.readyRemoteStateDeltasForConsensus(1)
	readyValidator := homeValidator.readyRemoteStateDeltasForConsensus(1)
	if len(readyLeader) != 2 || len(readyValidator) != 2 {
		t.Fatalf("expected set and commutative remote deltas ready on both validators, leader=%#v validator=%#v", readyLeader, readyValidator)
	}
	if readyLeader[0].UpdateSemantics != "" || readyLeader[1].UpdateSemantics != "commutative_delta" {
		t.Fatalf("remote deltas must apply set before commutative delta, got %#v", readyLeader)
	}

	spend := tx.SignedTransaction{TxID: "spend-remote-credit", Sender: account, Receiver: "receiver-after-remote", Nonce: 0, Value: 1}
	homeBlock := realblock.Block{Height: 1, ShardID: "s0", PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 10, TxIDs: []string{spend.TxID}, TxList: []tx.SignedTransaction{spend}, SystemStateDeltas: readyLeader}
	realblock.AssignHash(&homeBlock)
	withoutRemote := homeBlock
	withoutRemote.SystemStateDeltas = nil
	realblock.AssignHash(&withoutRemote)
	if homeBlock.BlockHash == withoutRemote.BlockHash {
		t.Fatal("remote system deltas must be bound into the proposed block hash")
	}
	leaderResult, err := homeLeader.commitOnce(context.Background(), homeBlock, CommitOriginConsensus)
	if err != nil {
		t.Fatal(err)
	}
	validatorResult, err := homeValidator.commitOnce(context.Background(), homeBlock, CommitOriginConsensus)
	if err != nil {
		t.Fatal(err)
	}
	if leaderResult.Disposition != CommitApplied || validatorResult.Disposition != CommitApplied {
		t.Fatalf("home shard did not commit remote delta on all validators: leader=%s validator=%s", leaderResult.Disposition, validatorResult.Disposition)
	}
	if got := homeLeader.db.Get(remoteKey); got != "103" {
		t.Fatalf("leader did not apply deterministic remote delta before spend, got %q", got)
	}
	if got := homeValidator.db.Get(remoteKey); got != "103" {
		t.Fatalf("validator did not apply deterministic remote delta before spend, got %q", got)
	}
	if homeLeader.db.Root() != homeValidator.db.Root() {
		t.Fatalf("home validators diverged roots: %s != %s", homeLeader.db.Root(), homeValidator.db.Root())
	}
	if len(homeLeader.pendingStateDeltas) != 0 || len(homeValidator.pendingStateDeltas) != 0 {
		t.Fatalf("remote deltas not cleared after consensus commit: leader=%#v validator=%#v", homeLeader.pendingStateDeltas, homeValidator.pendingStateDeltas)
	}
	if got := execShard.db.Get(remoteKey); got != "" {
		t.Fatalf("execution shard retained remote home-state after home commit: %q", got)
	}
	reopened, err := state.Open(plan.NodeConfigs[0].DataDir, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.Get(remoteKey); got != "103" {
		t.Fatalf("recovered home state did not preserve remote delta, got %q", got)
	}
}

func TestCommitDoesNotApplyRemoteStateDeltasOutsideConsensus(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	request := StateDeltaApplyRequest{RequestID: "apply-before-next", TxID: "tx-remote-credit", TxIDs: []string{"tx-remote-credit"}, BlockHash: "remote-block", Key: "balance:alice", Value: "99", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::balance:alice", SourceHeight: 1}

	ack := home.handleStateDeltaApplyRequest(request)
	if !ack.Success {
		t.Fatalf("queued delta should be acknowledged as reliably received: %#v", ack)
	}
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("queued delta must not apply before the height barrier, got %q", got)
	}

	home.committedHeight = 2
	home.committedHash = "h2"
	item := tx.SignedTransaction{TxID: "tx-spend-after-remote-credit", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1}
	block := realblock.Block{Height: 3, ShardID: "s0", PreviousHash: "h2", ProposerID: "n-s0", Timestamp: 3, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}}
	block.SystemStateDeltas = home.readyRemoteStateDeltasForConsensus(block.Height)
	realblock.AssignHash(&block)

	result, err := home.commitOnce(context.Background(), block, CommitOriginConsensus)
	if err != nil {
		t.Fatal(err)
	}
	if result.Disposition != CommitApplied {
		t.Fatalf("expected next block to commit after applying queued deltas, got %s", result.Disposition)
	}
	if got := home.db.Get("balance:alice"); got != "98" {
		t.Fatalf("next block should read the consensus-bound remote credit before spending, alice balance %q", got)
	}
	if got := home.db.Get("balance:bob"); got != "1" {
		t.Fatalf("receiver balance missing from next block execution, got %q", got)
	}
	if len(home.pendingStateDeltas) != 0 {
		t.Fatalf("remote delta should be cleared after home-shard consensus commit: %#v", home.pendingStateDeltas)
	}
}

func TestWriteArtifactsFlushesReadyRemoteStateDeltas(t *testing.T) {
	profile := testMetaTrackProfile()
	dataDir := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: dataDir, Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	request := StateDeltaApplyRequest{RequestID: "apply-before-artifacts", TxID: "tx-artifact-credit", TxIDs: []string{"tx-artifact-credit"}, BlockHash: "remote-block", Key: "balance:alice", Value: "7", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::balance:alice", SourceHeight: 1}
	home.committedHeight = 1
	ack := home.handleStateDeltaApplyRequest(request)
	if !ack.Success {
		t.Fatalf("queued delta should be acknowledged: %#v", ack)
	}
	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("queued delta should wait for a deterministic flush boundary, got %q", got)
	}

	if err := home.WriteArtifacts(); err != nil {
		t.Fatal(err)
	}

	if got := home.db.Get("balance:alice"); got != "" {
		t.Fatalf("artifact flush must not apply remote delta outside consensus, got %q", got)
	}
	if len(home.pendingStateDeltas) != 1 {
		t.Fatalf("artifact flush must leave remote delta pending for consensus commit: %#v", home.pendingStateDeltas)
	}
	summary := readJSONMap(t, filepath.Join(dataDir, "node_summary.json"))
	if summary["state_root"] != home.db.Root() {
		t.Fatalf("node summary was written before ready remote delta flush: %#v", summary["state_root"])
	}
}

func TestAnnotateStateDeltaTxIDsUsesTxDeltaWriteSetsDeterministically(t *testing.T) {
	physical := []state.StateKV{
		{Key: "s1::coaccess:hot", Value: "6"},
		{Key: "s1::balance:alice", Value: "999"},
		{Key: "s1::balance:bob", Value: "2"},
		{Key: "s1::balance:carol", Value: "0"},
	}
	txDeltas := []execution.TxDelta{
		{TxID: "tx-a", OriginalIndex: 0, WriteSet: map[string]string{"coaccess:hot": "1"}, Success: true},
		{TxID: "tx-b", OriginalIndex: 1, WriteSet: map[string]string{"balance:alice": "999"}, Success: true},
		{TxID: "tx-c", OriginalIndex: 2, WriteSet: map[string]string{"coaccess:hot": "6"}, Success: true},
		{TxID: "tx-d", OriginalIndex: 3, WriteSet: map[string]string{"balance:bob": "1"}, Success: true},
		{TxID: "tx-e", OriginalIndex: 4, WriteSet: map[string]string{"balance:bob": "2"}, Success: true},
		{TxID: "tx-f", OriginalIndex: 5, WriteSet: map[string]string{"balance:carol": "0"}, Success: false},
	}

	transactions := []tx.SignedTransaction{
		{TxID: "tx-a", AccessList: []tx.AccessItem{{Key: "coaccess:hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}},
		{TxID: "tx-b", AccessList: []tx.AccessItem{{Key: "balance:alice", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}},
		{TxID: "tx-c", AccessList: []tx.AccessItem{{Key: "coaccess:hot", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 5}}},
		{TxID: "tx-d", Sender: "sender-d", Receiver: "bob", Value: 1},
		{TxID: "tx-e", Sender: "sender-e", Receiver: "bob", Value: 1},
		{TxID: "tx-f", Sender: "sender-f", Receiver: "carol", Value: 1},
	}

	got := annotateStateDeltaTxIDs(physical, txDeltas, transactions)

	if fmt.Sprint(got[0].TxIDs) != "[tx-a tx-c]" {
		t.Fatalf("commutative physical update should retain contributing logical tx ids in order: %#v", got[0].TxIDs)
	}
	if got[0].UpdateSemantics != "commutative_delta" || got[0].Delta != 6 {
		t.Fatalf("commutative physical update should carry delta semantics, got %#v", got[0])
	}
	if fmt.Sprint(got[1].TxIDs) != "[tx-b]" {
		t.Fatalf("ordinary physical update should retain its logical tx id: %#v", got[1].TxIDs)
	}
	if got[1].UpdateSemantics != "" || got[1].Delta != 0 {
		t.Fatalf("ordinary physical update should remain set semantics, got %#v", got[1])
	}
	if fmt.Sprint(got[2].TxIDs) != "[tx-d tx-e]" {
		t.Fatalf("receiver balance update should retain contributing tx ids: %#v", got[2].TxIDs)
	}
	if got[2].UpdateSemantics != "commutative_delta" || got[2].Delta != 2 {
		t.Fatalf("receiver balance remote update should carry additive semantics, got %#v", got[2])
	}
	if got[3].UpdateSemantics != "" || got[3].Delta != 0 {
		t.Fatalf("failed transfer receiver update must not become additive, got %#v", got[3])
	}
	if len(physical[0].TxIDs) != 0 {
		t.Fatalf("annotateStateDeltaTxIDs must not mutate input delta: %#v", physical[0].TxIDs)
	}
}

func TestLogicalPhysicalUpdateMappingRecordsPhysicalWriteProvenance(t *testing.T) {
	dataDir := t.TempDir()
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: dataDir, Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{BlockHash: "block-map", Height: 1, ShardID: "s0", TxList: []tx.SignedTransaction{{TxID: "tx-a"}, {TxID: "tx-b"}}}
	decision := CommitDecision{LogicalUpdates: 2, PhysicalUpdates: 1, AggregationGroupID: "s0:1", Applied: true}
	physical := []state.StateKV{{Key: "s0::coaccess:hot", Value: "3", TxIDs: []string{"tx-a", "tx-b"}}}

	runtime.recordExecutionAndCommitDecisions(block, decision, physical)
	if err := runtime.writeMetaTrackNodeArtifacts(runtime.executionRows, runtime.commitRows, runtime.logicalPhysicalRows); err != nil {
		t.Fatal(err)
	}

	rows := readCSVRows(t, filepath.Join(dataDir, "logical_physical_update_mapping.csv"))
	if len(rows) != 2 {
		t.Fatalf("expected header and one mapping row, got %#v", rows)
	}
	header := strings.Join(rows[0], "|")
	if !strings.Contains(header, "state_key") || !strings.Contains(header, "logical_tx_ids") || !strings.Contains(header, "value_digest") {
		t.Fatalf("mapping header lacks physical write provenance columns: %#v", rows[0])
	}
	if rows[1][6] != "s0::coaccess:hot" || rows[1][8] != "tx-a|tx-b" || rows[1][11] != "1" {
		t.Fatalf("mapping row lost logical-to-physical aggregation evidence: %#v", rows[1])
	}
}

func TestBlockSTMArtifactsPreserveKernelSerialEquivalenceEvidence(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["block_executor"] = PluginConfig{PluginID: "block_stm_block_executor", Config: map[string]any{"worker_count": 4}}
	dataDir := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: dataDir, Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.writeBlockSTMArtifacts([]map[string]any{{
		"block_hash":            "block-equivalence",
		"height":                uint64(1),
		"block_executor_id":     execution.BlockSTMExecutorID,
		"state_root_before":     "before",
		"state_root_after":      "after",
		"receipt_root":          "receipts",
		"execution_plan_digest": "plan",
		"serial_equivalent":     false,
		"block_stm_metrics": execution.BlockSTMMetrics{
			WorkerCount:                     4,
			MaximumParallelWidth:            2,
			ExecutionTaskCount:              1,
			ValidationTaskCount:             1,
			EstimateMarkCount:               3,
			EstimateReadCount:               2,
			ValidatedSpeculativeResultCount: 1,
			MaximumConcurrentExecutions:     2,
			SchedulerQueuePeak:              5,
			StaleTaskCount:                  1,
			IncarnationHistogram:            map[int]int{0: 1},
		},
	}})
	if err != nil {
		t.Fatal(err)
	}

	summary := readJSONMap(t, filepath.Join(dataDir, "block_stm_summary.json"))
	if summary["serial_equivalent"] != false {
		t.Fatalf("block_stm_summary.json must preserve kernel equivalence evidence, got %#v", summary["serial_equivalent"])
	}
	metrics := summary["block_stm_metrics"].(map[string]any)
	for key, want := range map[string]float64{
		"estimate_mark_count":                3,
		"estimate_read_count":                2,
		"validated_speculative_result_count": 1,
		"maximum_concurrent_executions":      2,
		"scheduler_queue_peak":               5,
		"stale_task_count":                   1,
	} {
		if metrics[key] != want {
			t.Fatalf("block_stm_summary.json did not aggregate %s: got %#v want %.0f", key, metrics[key], want)
		}
	}
	equivalence := readJSONMap(t, filepath.Join(dataDir, "serial_equivalence.json"))
	if equivalence["serial_equivalent"] != false {
		t.Fatalf("serial_equivalence.json must not hard-code success, got %#v", equivalence["serial_equivalent"])
	}
}

func testMetaTrackProfile() map[string]PluginConfig {
	return map[string]PluginConfig{
		"workload":              {PluginID: "deterministic_signed_synthetic", Config: map[string]any{}},
		"transaction_admission": {PluginID: "signature_nonce_admission", Config: map[string]any{}},
		"txpool":                {PluginID: "fifo_per_node_mempool", Config: map[string]any{"capacity": 100}},
		"sharding":              {PluginID: "deterministic_state_key_sharding", Config: map[string]any{}},
		"routing":               {PluginID: "metatrack_coaccess_routing", Config: map[string]any{}},
		"block_producer":        {PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 100, "interval_ms": 75}},
		"consensus":             {PluginID: "pbft_style_consensus", Config: map[string]any{}},
		"network":               {PluginID: "localhost_tcp_typed_network", Config: map[string]any{}},
		"execution":             {PluginID: "dual_track_execution", Config: map[string]any{}},
		"scheduler":             {PluginID: "fast_first_scheduler", Config: map[string]any{}},
		"block_executor":        {PluginID: "serial_block_executor", Config: map[string]any{"worker_count": 1}},
		"state_access":          {PluginID: "direct_state_access", Config: map[string]any{}},
		"state_storage":         {PluginID: "persistent_local_state_store", Config: map[string]any{}},
		"cross_shard":           {PluginID: "relay_certificate_protocol", Config: map[string]any{}},
		"commit":                {PluginID: "commutative_hot_update_aggregation", Config: map[string]any{}},
		"fault_injection":       {PluginID: "faults_disabled", Config: map[string]any{}},
		"metrics":               {PluginID: "runtime_core_metrics", Config: map[string]any{}},
		"observability":         {PluginID: "node_network_consensus_observer", Config: map[string]any{}},
	}
}

func readCSVRows(t *testing.T, path string) [][]string {
	t.Helper()
	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	rows, err := csv.NewReader(handle).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func readJSONMap(t *testing.T, path string) map[string]any {
	t.Helper()
	payload, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]any{}
	if err := json.Unmarshal(payload, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func keyWithHomeShard(t *testing.T, shard string, shards []string) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("metatrack-remote-test-%d", i)
		if shards[stableKey([]string{key})%len(shards)] == shard {
			return key
		}
	}
	t.Fatalf("could not find key for %s", shard)
	return ""
}

func keyWithHomeShardPrefix(t *testing.T, shard string, shards []string, prefix string) string {
	t.Helper()
	for i := 0; i < 4096; i++ {
		key := fmt.Sprintf("%smetatrack-account-%d", prefix, i)
		if shards[stableKey([]string{key})%len(shards)] == shard {
			return key
		}
	}
	t.Fatalf("could not find key with prefix %q for %s", prefix, shard)
	return ""
}

func freeLocalAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func transportSaw(entries []p2p.NetworkLogEntry, direction, messageType string) bool {
	for _, entry := range entries {
		if entry.Direction == direction && entry.MessageType == messageType {
			return true
		}
	}
	return false
}

func remoteWritebackItemsForTest(block realblock.Block, annotated []state.StateKV, txDeltas []execution.TxDelta) []state.StateKV {
	out := make([]state.StateKV, 0, len(annotated))
	for _, item := range annotated {
		unqualified, ok := unqualifiedLocalKey(item.Key, block.ShardID)
		if !ok {
			continue
		}
		next := metaTrackRemoteWritebackItem(item, unqualified, block.TxList, txDeltas)
		next.Key = unqualified
		out = append(out, next)
	}
	return out
}

func mustParseStateInt(t *testing.T, value string) int64 {
	t.Helper()
	var out int64
	if _, err := fmt.Sscanf(value, "%d", &out); err != nil {
		t.Fatalf("invalid integer state value %q: %v", value, err)
	}
	return out
}

func schedulerRowsSaw(rows [][]string, txID, queue string, blocked, wakeup bool) bool {
	for _, row := range rows {
		if len(row) < 13 {
			continue
		}
		if row[5] == txID && row[7] == queue && row[11] == fmt.Sprint(blocked) && row[12] == fmt.Sprint(wakeup) {
			return true
		}
	}
	return false
}

func schedulerRowsSawStolen(rows [][]string, txID string) bool {
	for _, row := range rows {
		if len(row) < 13 {
			continue
		}
		if row[5] == txID && row[9] == "false" && row[10] == "true" {
			return true
		}
	}
	return false
}

func schedulerRowsSawReason(rows [][]string, txID, reason string) bool {
	for _, row := range rows {
		if len(row) < 9 {
			continue
		}
		if row[5] == txID && strings.Contains(row[8], reason) {
			return true
		}
	}
	return false
}

func schedulerRowsCarryQueueDepths(rows [][]string) bool {
	for _, row := range rows {
		if len(row) >= 18 && row[13] != "" && row[14] != "" && row[15] != "" && row[16] != "" && row[17] != "" {
			return true
		}
	}
	return false
}
func TestRemoteStateDeltaDrainStateKeepsPendingWorkLiveBeforeReady(t *testing.T) {
	request := StateDeltaApplyRequest{
		RequestID:      "request-1",
		TxID:           "tx-1",
		BlockHash:      "source-block-100",
		Key:            "balance:alice",
		Value:          "10",
		HomeShard:      "s1",
		ExecutionShard: "s0",
		SourceHeight:   100,
	}

	runtime := &NodeRuntime{
		pendingStateDeltas: []StateDeltaApplyRequest{request},
	}

	ready, pending := runtime.remoteStateDeltaDrainState(1)
	if !pending {
		t.Fatal("pending remote state delta must keep system drain work active")
	}
	if len(ready) != 0 {
		t.Fatalf("delta became ready before source-height boundary: %d", len(ready))
	}

	readyHeight := request.SourceHeight + remoteStateDeltaApplyLagBlocks
	ready, pending = runtime.remoteStateDeltaDrainState(readyHeight)
	if !pending {
		t.Fatal("delta remains pending until its consensus block commits")
	}
	if len(ready) != 1 {
		t.Fatalf("expected one ready remote state delta, got %d", len(ready))
	}
	if ready[0].SourceHeight != request.SourceHeight {
		t.Fatalf(
			"source height mismatch: got %d want %d",
			ready[0].SourceHeight,
			request.SourceHeight,
		)
	}
}