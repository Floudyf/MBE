package v5

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
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
	if !transportSaw(exec.transport.Log.Entries(), "state_access_send", stateFetchRequestMessage) {
		t.Fatal("state fetch request did not use the dedicated state-access lane")
	}
	if !transportSaw(home.transport.Log.Entries(), "state_access_send", stateFetchResponseMessage) {
		t.Fatal("state fetch response did not use the dedicated state-access lane")
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
	conservative := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "conservative"}, "s0")
	fast := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}, "s0")
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{conservative.TxID, fast.TxID}, TxList: []tx.SignedTransaction{conservative, fast}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	originalHash := block.BlockHash

	scheduled, err := runtime.scheduleBlock(block)
	if err != nil {
		t.Fatal(err)
	}

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

func TestMetaTrackProposalFailsClosedWithoutSignedRoutingMetadata(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	item := tx.SignedTransaction{
		TxID:       "metatrack-missing-route",
		Sender:     "sender",
		Receiver:   "receiver",
		StateKeys:  []string{"state:key"},
		AccessList: []tx.AccessItem{{Key: "state:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
	}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	if _, err := runtime.scheduleBlock(block); err == nil || !strings.Contains(err.Error(), "missing signed execution routing metadata") {
		t.Fatalf("expected fail-closed MetaTrack route validation, got %v", err)
	}
}

func TestRuntimeScheduleBlockLabelsStatelessHashPlanIndependently(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
	profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	item := tx.SignedTransaction{TxID: "stateless-hash", StateKeys: []string{"market:0x1"}, AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)

	scheduled, err := runtime.scheduleBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if scheduled.ExecutionPlan == nil {
		t.Fatal("stateless hash proposal did not bind a batch execution plan")
	}
	if scheduled.ExecutionPlan.AlgorithmID != "stateless_hash_batch_execution_plan_v1" {
		t.Fatalf("stateless hash plan was mislabeled: %#v", scheduled.ExecutionPlan)
	}
	var payload map[string]any
	if err := json.Unmarshal(scheduled.ExecutionPlan.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["algorithm_id"] != "stateless_hash_batch_execution_plan_v1" {
		t.Fatalf("stateless hash payload was mislabeled: %#v", payload)
	}
	if err := runtime.verifyExecutionPlanEnvelope(scheduled); err != nil {
		t.Fatalf("validator rejected stateless hash plan: %v", err)
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
	item := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "metatrack-stm", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}, "s0")
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)

	scheduled, err := runtime.scheduleBlock(block)
	if err != nil {
		t.Fatal(err)
	}

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
	item := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "metatrack-tamper", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}, "s0")
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	scheduled, err := runtime.scheduleBlock(block)
	if err != nil {
		t.Fatal(err)
	}

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
	item := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "metatrack-plan-drive", AccessList: []tx.AccessItem{{Key: "market:0x1", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}, "s0")
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	scheduled, err := runtime.scheduleBlock(block)
	if err != nil {
		t.Fatal(err)
	}
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

	if err := runtime.validateMetaTrackPlanDrivesExecution(scheduled); err == nil || (!strings.Contains(err.Error(), "plan drives execution mismatch") && !strings.Contains(err.Error(), "signed route mismatch")) {
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
	item := attachTestMetaTrackRouting(t, tx.SignedTransaction{TxID: "remote-home", AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}, "s1")
	block := realblock.Block{ShardID: "s1", Height: 1, PreviousHash: "genesis", ProposerID: "n-s1", Timestamp: 1, TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)

	if _, err := runtime.scheduleBlock(block); err != nil {
		t.Fatal(err)
	}

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
	if !transportSaw(exec.transport.Log.Entries(), "state_access_send", stateDeltaApplyMessage) {
		t.Fatal("state delta apply did not use the dedicated state-access lane")
	}
	if !transportSaw(home.transport.Log.Entries(), "state_access_send", stateDeltaApplyAckMessage) {
		t.Fatal("state delta apply ack did not use the dedicated state-access lane")
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

func attachTestMetaTrackRouting(t *testing.T, item tx.SignedTransaction, shard string) tx.SignedTransaction {
	t.Helper()
	if item.Sender == "" {
		item.Sender = "test-sender-" + item.TxID
	}
	if item.Receiver == "" {
		item.Receiver = "test-receiver-" + item.TxID
	}
	routing := tx.ExecutionRoutingMetadata{
		SenderID:              item.Sender,
		ReceiverID:            item.Receiver,
		RoutingEpoch:          0,
		RoutingOrdinal:        1,
		ExecutionShard:        shard,
		RoutingReason:         "test_sender_group_epoch_affinity",
		RoutePlanDigest:       "test-route-plan-" + shard,
		PredictedRemoteReads:  0,
		PredictedRemoteWrites: 0,
	}
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	return item
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

func TestValidatePrePrepareRequiresLocalNextHeightAndParent(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
	}}
	runtime := &NodeRuntime{
		node:            plan.NodeConfigs[1],
		plan:            plan,
		committedHeight: 1,
		committedHash:   "height-1",
	}
	makeBlock := func(height uint64, previous string) realblock.Block {
		block := realblock.Block{ShardID: "s0", Height: height, PreviousHash: previous, ProposerID: "n0", Timestamp: 1}
		realblock.AssignHash(&block)
		return block
	}
	accepted, catchup, err := runtime.validatePrePrepare("n0", makeBlock(2, "height-1"))
	if err != nil || !accepted || catchup {
		t.Fatalf("valid next proposal rejected: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	accepted, catchup, err = runtime.validatePrePrepare("n0", makeBlock(3, "height-2"))
	if err != nil || accepted || !catchup {
		t.Fatalf("future proposal must be held for catch-up: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	accepted, catchup, err = runtime.validatePrePrepare("n0", makeBlock(2, "wrong-parent"))
	if err != nil || accepted || !catchup {
		t.Fatalf("parent mismatch must be held for catch-up: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	accepted, catchup, err = runtime.validatePrePrepare("n0", makeBlock(1, "genesis"))
	if err != nil || accepted || catchup {
		t.Fatalf("stale proposal must be ignored: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
}

func TestFuturePrePrepareConflictingSameViewDigestKeepsFirstProposal(t *testing.T) {
	runtime := &NodeRuntime{
		node:                NodePlan{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		committedHeight:     0,
		committedHash:       "genesis",
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
	}
	first := realblock.Block{ShardID: "s0", Height: 2, PreviousHash: "height-1", ProposerID: "n0", Timestamp: 1}
	realblock.AssignHash(&first)
	replacement := first
	replacement.Timestamp = 2
	realblock.AssignHash(&replacement)
	runtime.deferPrePrepare("n0", first)
	runtime.deferPrePrepare("n0", replacement)
	pending := runtime.deferredPrePrepares[2]
	if pending.Block.BlockHash != first.BlockHash {
		t.Fatalf("same-view conflicting future proposal replaced the first digest: got=%s want=%s", pending.Block.BlockHash, first.BlockHash)
	}
	if runtime.lastProposalError == "" {
		t.Fatal("conflicting future PRE-PREPARE was not recorded")
	}
}

func TestFuturePrePrepareIsReplayedImmediatelyAfterParentCommit(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
	}}
	future := realblock.Block{ShardID: "s0", Height: 2, PreviousHash: "height-1", ProposerID: "n0", Timestamp: 1}
	realblock.AssignHash(&future)
	sent := map[string]p2p.MessageEnvelope{}
	runtime := &NodeRuntime{
		node:                plan.NodeConfigs[1],
		plan:                plan,
		committedHeight:     0,
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		runtimeMetricCounts: map[string]int64{},
		sendToNodeHook: func(_ context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
			sent[nodeID] = envelope
			return nil
		},
	}
	accepted, catchup, err := runtime.validatePrePrepare("n0", future)
	if err != nil || accepted || !catchup {
		t.Fatalf("future proposal disposition mismatch: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	runtime.deferPrePrepare("n0", future)
	if pending := runtime.deferredPrePrepares[future.Height]; pending.Block.BlockHash != future.BlockHash {
		t.Fatalf("future proposal was not deferred: %+v", pending)
	}
	runtime.committedHeight = 1
	runtime.committedHash = "height-1"
	runtime.replayDeferredPrePrepare(context.Background())
	if _, ok := runtime.deferredPrePrepares[future.Height]; ok {
		t.Fatal("replayed proposal remained deferred")
	}
	if runtime.proposals[future.BlockHash].BlockHash != future.BlockHash {
		t.Fatal("replayed proposal was not remembered")
	}
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		envelope, ok := sent[nodeID]
		if !ok || envelope.MessageType != p2p.MessagePBFTPrepare {
			t.Fatalf("replayed proposal did not broadcast PREPARE to %s: %+v", nodeID, envelope)
		}
		vote, err := p2p.DecodePayload[pbft.Prepare](envelope)
		if err != nil {
			t.Fatal(err)
		}
		if vote.BlockHash != future.BlockHash || vote.Height != future.Height || vote.NodeID != "n1" {
			t.Fatalf("unexpected replayed prepare vote: %+v", vote)
		}
	}
}

func TestValidateConsensusCommitRequiresRememberedNextProposal(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
	}}
	runtime := &NodeRuntime{
		node:            plan.NodeConfigs[1],
		plan:            plan,
		committedHeight: 1,
		committedHash:   "height-1",
		proposals:       map[string]realblock.Block{},
	}
	makeBlock := func(height uint64, previous string) realblock.Block {
		block := realblock.Block{ShardID: "s0", Height: height, PreviousHash: previous, ProposerID: "n0", Timestamp: 1}
		realblock.AssignHash(&block)
		return block
	}
	next := makeBlock(2, "height-1")
	runtime.proposals[next.BlockHash] = next
	accepted, catchup, err := runtime.validateConsensusCommit("n0", next)
	if err != nil || !accepted || catchup {
		t.Fatalf("valid remembered commit rejected: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	unknown := makeBlock(2, "height-1")
	unknown.Timestamp = 2
	realblock.AssignHash(&unknown)
	accepted, catchup, err = runtime.validateConsensusCommit("n0", unknown)
	if err != nil || accepted || !catchup {
		t.Fatalf("unremembered commit must request catch-up: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	future := makeBlock(3, "height-2")
	accepted, catchup, err = runtime.validateConsensusCommit("n0", future)
	if err != nil || accepted || !catchup {
		t.Fatalf("future commit must request catch-up: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}
	if _, _, err := runtime.validateConsensusCommit("n2", next); err == nil {
		t.Fatal("non-leader commit sender was accepted")
	}
}

func TestRejectDeterministicExecutionFreezesRuntime(t *testing.T) {
	runtime := &NodeRuntime{}
	block := realblock.Block{Height: 7, BlockHash: "block-7"}
	result, err := runtime.rejectDeterministicExecution(block, "invalid_execution_receipts", fmt.Errorf("missing execution receipt"))
	if err == nil || result.Disposition != CommitRejected {
		t.Fatalf("deterministic failure was not rejected: result=%+v err=%v", result, err)
	}
	if runtime.fatalExecutionError != "missing execution receipt" {
		t.Fatalf("runtime did not freeze on deterministic failure: %q", runtime.fatalExecutionError)
	}
	if runtime.lastCommitFailure.Phase != "invalid_execution_receipts" || runtime.lastCommitFailure.Height != block.Height {
		t.Fatalf("deterministic failure evidence is incomplete: %+v", runtime.lastCommitFailure)
	}
}

func TestMetaTrackRemoteWritebackPreservesRoutingOrdinalForNonCommutativeWriters(t *testing.T) {
	transactions := []tx.SignedTransaction{
		{TxID: "tx-late", ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 22}},
		{TxID: "tx-early", ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 7}},
	}
	deltas := []execution.TxDelta{
		{TxID: "tx-late", Success: true, WriteSet: map[string]string{"asset:hot": "late"}},
		{TxID: "tx-early", Success: true, WriteSet: map[string]string{"asset:hot": "early"}},
	}
	physical := state.StateKV{Key: "s1::asset:hot", Value: "late", TxIDs: []string{"tx-late", "tx-early"}, UpdateSemantics: "set"}
	items := metaTrackRemoteWritebackItems(physical, "asset:hot", transactions, deltas)
	if len(items) != 2 {
		t.Fatalf("expected one remote writeback per non-commutative writer, got %#v", items)
	}
	if items[0].TxIDs[0] != "tx-early" || items[0].RoutingOrdinal != 7 || items[0].Value != "early" {
		t.Fatalf("earlier routing ordinal must be first: %#v", items[0])
	}
	if items[1].TxIDs[0] != "tx-late" || items[1].RoutingOrdinal != 22 || items[1].Value != "late" {
		t.Fatalf("later routing ordinal must be second: %#v", items[1])
	}
}

func TestRemoteDeltaConsensusOrderUsesRoutingOrdinalBeforeSourceBlockShape(t *testing.T) {
	early := StateDeltaApplyRequest{RoutingOrdinal: 3, Key: "asset:hot", SourceHeight: 99, BlockHash: "zzzz", TxID: "early", ExecutionShard: "s1"}
	late := StateDeltaApplyRequest{RoutingOrdinal: 10, Key: "asset:hot", SourceHeight: 1, BlockHash: "aaaa", TxID: "late", ExecutionShard: "s1"}
	if remoteDeltaConsensusOrder(early) >= remoteDeltaConsensusOrder(late) {
		t.Fatalf("routing ordinal must dominate source block height/hash: early=%q late=%q", remoteDeltaConsensusOrder(early), remoteDeltaConsensusOrder(late))
	}
}

func TestRemoteWritebackSequenceIsIndependentOfSourceBlockGrouping(t *testing.T) {
	txs := []tx.SignedTransaction{
		{TxID: "t1", ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 1}},
		{TxID: "t2", ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 2}},
		{TxID: "t3", ExecutionRouting: &tx.ExecutionRoutingMetadata{RoutingOrdinal: 3}},
	}
	deltas := []execution.TxDelta{
		{TxID: "t1", Success: true, OriginalIndex: 0, WriteSet: map[string]string{"object:hot": "v1"}},
		{TxID: "t2", Success: true, OriginalIndex: 1, WriteSet: map[string]string{"object:hot": "v2"}},
		{TxID: "t3", Success: true, OriginalIndex: 2, WriteSet: map[string]string{"object:hot": "v3"}},
	}
	collect := func(groups [][]int) []state.StateKV {
		out := []state.StateKV{}
		for _, group := range groups {
			groupTxs := make([]tx.SignedTransaction, 0, len(group))
			groupDeltas := make([]execution.TxDelta, 0, len(group))
			ids := make([]string, 0, len(group))
			for _, index := range group {
				groupTxs = append(groupTxs, txs[index])
				groupDeltas = append(groupDeltas, deltas[index])
				ids = append(ids, txs[index].TxID)
			}
			physical := state.StateKV{Key: "s1::object:hot", Value: groupDeltas[len(groupDeltas)-1].WriteSet["object:hot"], TxIDs: ids, UpdateSemantics: "set"}
			out = append(out, metaTrackRemoteWritebackItems(physical, "object:hot", groupTxs, groupDeltas)...)
		}
		sort.SliceStable(out, func(i, j int) bool { return out[i].RoutingOrdinal < out[j].RoutingOrdinal })
		return out
	}
	left := collect([][]int{{0, 1}, {2}})
	right := collect([][]int{{0}, {1, 2}})
	if !reflect.DeepEqual(left, right) {
		t.Fatalf("remote writeback sequence changed with source block grouping:\nleft=%#v\nright=%#v", left, right)
	}
}

func TestVersionedRemoteHomePublishesHistoricalVersionsAndNeverRegressesMaterializedState(t *testing.T) {
	profile := testMetaTrackProfile()
	profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
	profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
		{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
	}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	home.db.Set(key, "v0")
	home.stateVersionInitial[key] = "v0"

	late := StateDeltaApplyRequest{RequestID: "v116", TxID: "tx116", BlockHash: "b116", Key: key, Value: "v116", UpdateSemantics: "set", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + key, RoutingOrdinal: 116, PreviousVersion: 80, ProducedVersion: 116}
	early := StateDeltaApplyRequest{RequestID: "v80", TxID: "tx80", BlockHash: "b80", Key: key, Value: "v80", UpdateSemantics: "set", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + key, RoutingOrdinal: 80, PreviousVersion: 31, ProducedVersion: 80}
	for _, request := range []StateDeltaApplyRequest{late, early} {
		if ack := home.handleStateDeltaApplyRequest(request); !ack.Success {
			t.Fatalf("version publication failed: %+v", ack)
		}
	}
	if value, ok := home.stateVersionValue(key, 80); !ok || value != "v80" {
		t.Fatalf("historical v80 missing after out-of-order arrival: value=%q ok=%t", value, ok)
	}
	if value, ok := home.stateVersionValue(key, 116); !ok || value != "v116" {
		t.Fatalf("historical v116 missing after out-of-order arrival: value=%q ok=%t", value, ok)
	}
	ready := home.readyRemoteStateDeltasForConsensus(1)
	if len(ready) != 2 || ready[0].ProducedVersion != 80 || ready[1].ProducedVersion != 116 {
		t.Fatalf("versioned consensus order must ignore network arrival order: %#v", ready)
	}
	block := realblock.Block{Height: 1, ShardID: "s0", SystemStateDeltas: ready}
	updates := home.materializableRemoteStateDeltas(block, "s0")
	if len(updates) != 2 || updates[0].ProducedVersion != 80 || updates[1].ProducedVersion != 116 {
		t.Fatalf("materialization did not preserve ascending per-key versions: %#v", updates)
	}
	if err := home.db.ApplyDeterministicBatch(updates); err != nil {
		t.Fatal(err)
	}
	home.markRemoteStateDeltasApplied(ready)
	if got := home.db.Get(key); got != "v116" {
		t.Fatalf("latest logical version was not materialized: got=%q", got)
	}

	// A late older version remains queryable as history but must not regress the
	// durable latest-value materialization.
	older := StateDeltaApplyRequest{RequestID: "v70", TxID: "tx70", BlockHash: "b70", Key: key, Value: "v70", UpdateSemantics: "set", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + key, RoutingOrdinal: 70, PreviousVersion: 31, ProducedVersion: 70}
	if ack := home.handleStateDeltaApplyRequest(older); !ack.Success {
		t.Fatalf("late historical publication failed: %+v", ack)
	}
	if value, ok := home.stateVersionValue(key, 70); !ok || value != "v70" {
		t.Fatalf("late historical version must remain fetchable: value=%q ok=%t", value, ok)
	}
	lateBlock := realblock.Block{Height: 2, ShardID: "s0", SystemStateDeltas: home.readyRemoteStateDeltasForConsensus(2)}
	if updates := home.materializableRemoteStateDeltas(lateBlock, "s0"); len(updates) != 0 {
		t.Fatalf("older version must not regress materialized DB: %#v", updates)
	}
	if got := home.db.Get(key); got != "v116" {
		t.Fatalf("late old version regressed DB: got=%q", got)
	}
}

func TestVersionedRemoteHomeNoopAdvancesVersionWithoutMutatingBusinessValue(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	key := "asset:noop"
	home.db.Set(key, "stable")
	home.stateVersionInitial[key] = "stable"
	request := StateDeltaApplyRequest{RequestID: "noop-9", TxID: "tx9", BlockHash: "b9", Key: key, Value: "stable", UpdateSemantics: "set", HomeShard: "s0", ExecutionShard: "s1", SourceKey: "s1::" + key, RoutingOrdinal: 9, PreviousVersion: 4, ProducedVersion: 9, OrderingNoop: true}
	if ack := home.handleStateDeltaApplyRequest(request); !ack.Success {
		t.Fatalf("noop version publication failed: %+v", ack)
	}
	ready := home.readyRemoteStateDeltasForConsensus(1)
	if len(ready) != 1 || !ready[0].OrderingNoop {
		t.Fatalf("noop version did not enter consensus evidence: %#v", ready)
	}
	block := realblock.Block{Height: 1, ShardID: "s0", SystemStateDeltas: ready}
	if updates := home.materializableRemoteStateDeltas(block, "s0"); len(updates) != 0 {
		t.Fatalf("noop version must not create a physical DB write: %#v", updates)
	}
	home.markRemoteStateDeltasApplied(ready)
	if got := home.db.Get(key); got != "stable" {
		t.Fatalf("noop version mutated business value: %q", got)
	}
	if got := home.stateVersionMaterialized[key]; got != 9 {
		t.Fatalf("noop version did not advance materialized frontier: got=%d", got)
	}
	if value, ok := home.stateVersionValue(key, 9); !ok || value != "stable" {
		t.Fatalf("noop logical version is not available to successors: value=%q ok=%t", value, ok)
	}
}

func TestVersionedRemoteWaveAdvancesInBlockDependencyFrontierForSerialAndBlockSTM(t *testing.T) {
	for _, blockExecutor := range []string{"serial_block_executor", "block_stm_block_executor"} {
		t.Run(blockExecutor, func(t *testing.T) {
			profile := testMetaTrackProfile()
			profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
			profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
			profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
			profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
			config := map[string]any{"worker_count": 1}
			if blockExecutor == "block_stm_block_executor" {
				config = map[string]any{"worker_count": 2, "execution_mode": "performance", "oracle_mode": "off"}
			}
			profile["block_executor"] = PluginConfig{PluginID: blockExecutor, Config: config}
			plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
				{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile},
				{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s1"}, PluginProfile: profile},
			}}
			runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
			if err != nil {
				t.Fatal(err)
			}
			key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
			runtime.db.Set(key, "seed")
			runtime.stateVersionInitial[key] = "seed"
			makeItem := func(id string, ordinal, required uint64) tx.SignedTransaction {
				item := tx.SignedTransaction{TxID: id, Sender: "sender-" + id, Receiver: "receiver-" + id, AccessListSchema: "mbe_workload_record_v3", AccessListSource: "test_versioned_frontier", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}, StateKeys: []string{key}}
				item.ExecutionRouting = &tx.ExecutionRoutingMetadata{RoutingOrdinal: ordinal, ExecutionShard: "s0", StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: required, ProducedVersion: ordinal}}}
				return item
			}
			first := makeItem("tx1", 1, 0)
			second := makeItem("tx2", 2, 1)
			block := realblock.Block{BlockHash: "version-frontier-" + blockExecutor, Height: 1, ShardID: "s0", TxIDs: []string{first.TxID, second.TxID}, TxList: []tx.SignedTransaction{first, second}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			result, err := runtime.executeVersionedRemoteBlock(ctx, block, runtime.db.Snapshot())
			if err != nil {
				t.Fatalf("version frontier execution failed: %v", err)
			}
			if len(result.ExecutionResult.TxDeltas) != 2 || !result.ExecutionResult.TxDeltas[0].Success || !result.ExecutionResult.TxDeltas[1].Success {
				t.Fatalf("version frontier did not complete both transactions: %#v", result.ExecutionResult.TxDeltas)
			}
			if got := intValue(result.ActualMetrics["versioned_state_ready_wave_count"]); got < 2 {
				t.Fatalf("dependent writers must advance in at least two ready waves, got %d (%#v)", got, result.ActualMetrics)
			}
			if _, ok := runtime.stateVersionValue(key, 1); !ok {
				t.Fatal("first produced version was not published")
			}
			if _, ok := runtime.stateVersionValue(key, 2); !ok {
				t.Fatal("second produced version was not published")
			}
		})
	}
}

func TestVersionedRemoteFrontierMakesCrossShardProgressWithoutWholeBlockBarrier(t *testing.T) {
	for _, blockExecutor := range []string{"serial_block_executor", "block_stm_block_executor"} {
		t.Run(blockExecutor, func(t *testing.T) {
			profile := testMetaTrackProfile()
			profile["routing"] = PluginConfig{PluginID: "stateless_hash_routing", Config: map[string]any{}}
			profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
			profile["scheduler"] = PluginConfig{PluginID: "fifo_serial_scheduler", Config: map[string]any{}}
			profile["commit"] = PluginConfig{PluginID: "normal_commit", Config: map[string]any{}}
			config := map[string]any{"worker_count": 1}
			if blockExecutor == "block_stm_block_executor" {
				config = map[string]any{"worker_count": 2, "execution_mode": "performance", "oracle_mode": "off"}
			}
			profile["block_executor"] = PluginConfig{PluginID: blockExecutor, Config: config}
			root := t.TempDir()
			plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
				{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "s0"), Validators: []string{"n-s0"}, PluginProfile: profile},
				{NodeID: "n-s1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "s1"), Validators: []string{"n-s1"}, PluginProfile: profile},
			}}
			s0, err := newNodeRuntime(plan, plan.NodeConfigs[0])
			if err != nil {
				t.Fatal(err)
			}
			s1, err := newNodeRuntime(plan, plan.NodeConfigs[1])
			if err != nil {
				t.Fatal(err)
			}
			runtimes := map[string]*NodeRuntime{"n-s0": s0, "n-s1": s1}
			wire := func(from *NodeRuntime) func(context.Context, string, p2p.MessageEnvelope) error {
				return func(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
					target := runtimes[nodeID]
					if target == nil {
						return fmt.Errorf("unknown test node %s", nodeID)
					}
					switch envelope.MessageType {
					case stateFetchRequestMessage:
						request, err := p2p.DecodePayload[StateFetchRequest](envelope)
						if err != nil {
							return err
						}
						value, ready := target.stateVersionValue(request.Key, request.RequiredVersion)
						response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: request.HomeShard + "::" + request.Key, Value: value, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: target.plugins.StateStorage.Root(target.db), StateVersion: request.RequiredVersion, Versioned: request.Versioned, Success: ready}
						if !ready {
							response.Error = "state_version_not_ready"
						}
						response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
						from.handleStateFetchResponse(response)
						return nil
					case stateDeltaApplyMessage:
						request, err := p2p.DecodePayload[StateDeltaApplyRequest](envelope)
						if err != nil {
							return err
						}
						ack := target.handleStateDeltaApplyRequest(request)
						from.handleStateDeltaApplyAck(ack)
						return nil
					default:
						return fmt.Errorf("unexpected test state-access message %s", envelope.MessageType)
					}
				}
			}
			s0.sendToNodeHook = wire(s0)
			s1.sendToNodeHook = wire(s1)

			key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
			s0.db.Set(key, "seed")
			s0.stateVersionInitial[key] = "seed"
			makeItem := func(id, execShard string, ordinal, required uint64) tx.SignedTransaction {
				item := tx.SignedTransaction{TxID: id, Sender: "sender-" + id, Receiver: "receiver-" + id, AccessListSchema: "mbe_workload_record_v3", AccessListSource: "cross_shard_version_frontier_test", AccessList: []tx.AccessItem{{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}, StateKeys: []string{key}}
				item.ExecutionRouting = &tx.ExecutionRoutingMetadata{RoutingOrdinal: ordinal, ExecutionShard: execShard, StateVersions: []tx.StateVersionDependency{{Key: key, RequiredVersion: required, ProducedVersion: ordinal}}}
				return item
			}
			producer := makeItem("tx1", "s1", 1, 0)
			consumer := makeItem("tx2", "s0", 2, 1)
			producerBlock := realblock.Block{BlockHash: "producer-" + blockExecutor, Height: 1, ShardID: "s1", TxIDs: []string{producer.TxID}, TxList: []tx.SignedTransaction{producer}}
			consumerBlock := realblock.Block{BlockHash: "consumer-" + blockExecutor, Height: 1, ShardID: "s0", TxIDs: []string{consumer.TxID}, TxList: []tx.SignedTransaction{consumer}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			type outcome struct {
				result BlockExecutionResult
				err    error
			}
			consumerDone := make(chan outcome, 1)
			go func() {
				result, err := s0.executeVersionedRemoteBlock(ctx, consumerBlock, s0.db.Snapshot())
				consumerDone <- outcome{result: result, err: err}
			}()
			time.Sleep(50 * time.Millisecond)
			producerResult, err := s1.executeVersionedRemoteBlock(ctx, producerBlock, s1.db.Snapshot())
			if err != nil {
				t.Fatalf("producer block failed: %v", err)
			}
			if len(producerResult.ExecutionResult.TxDeltas) != 1 || !producerResult.ExecutionResult.TxDeltas[0].Success {
				t.Fatalf("producer transaction failed: %#v", producerResult.ExecutionResult.TxDeltas)
			}
			select {
			case got := <-consumerDone:
				if got.err != nil {
					t.Fatalf("consumer block did not wake after exact version publication: %v", got.err)
				}
				if len(got.result.ExecutionResult.TxDeltas) != 1 || !got.result.ExecutionResult.TxDeltas[0].Success {
					t.Fatalf("consumer transaction failed: %#v", got.result.ExecutionResult.TxDeltas)
				}
			case <-ctx.Done():
				t.Fatal("cross-shard version frontier deadlocked")
			}
			if _, ok := s0.stateVersionValue(key, 1); !ok {
				t.Fatal("remote producer version did not reach home shard")
			}
			if _, ok := s0.stateVersionValue(key, 2); !ok {
				t.Fatal("consumer version was not published after wakeup")
			}
		})
	}
}

func TestCanonicalBusinessStateDigestExcludesProtocolOnlyKeys(t *testing.T) {
	base := map[string]string{
		"s0::asset:1":               "v1",
		"s0::balance:alice":         "7",
		"s0::relay_commit:tx-cross": "1",
		"s0::protocol:marker":       "internal",
	}
	changedProtocol := copyStringMap(base)
	changedProtocol["s0::relay_commit:tx-cross"] = "2"
	changedProtocol["s0::protocol:marker"] = "different"
	if canonicalBusinessStateDigest(base) != canonicalBusinessStateDigest(changedProtocol) {
		t.Fatal("protocol-only keys changed the business-state diagnostic digest")
	}
	changedBusiness := copyStringMap(base)
	changedBusiness["s0::asset:1"] = "v2"
	if canonicalBusinessStateDigest(base) == canonicalBusinessStateDigest(changedBusiness) {
		t.Fatal("business-state diagnostic failed to detect a logical state change")
	}
}

func TestSummarizeStateReadyEvidenceCarriesNativeAndVersionedFrontiers(t *testing.T) {
	summary := summarizeStateReadyEvidence([]map[string]any{
		{"state_wait_blocked_count": 2, "state_ready_wakeup_count": 1, "remote_state_fetch_latency_ms": 7, "remote_state_fetch_count": 3, "remote_state_fetch_completed_count": 2, "state_ready_scheduler_mode": "transaction_level_suspend_resume", "versioned_state_ready_wave_count": 4, "versioned_state_ready_wait_observation_count": 5, "versioned_state_ready_resolved_token_count": 6, "versioned_state_probe_count": 7, "versioned_state_probe_latency_ms": 8, "versioned_state_ready_max_wave_width": 3, "versioned_state_ready_scheduler_mode": "per_transaction_per_key_version_frontier"},
		{"state_wait_blocked_count": 1, "state_ready_wakeup_count": 1, "remote_state_fetch_latency_ms": 11, "remote_state_fetch_count": 2, "remote_state_fetch_completed_count": 2, "state_ready_scheduler_mode": "transaction_level_suspend_resume", "versioned_state_ready_wave_count": 2, "versioned_state_ready_wait_observation_count": 1, "versioned_state_ready_resolved_token_count": 2, "versioned_state_probe_count": 3, "versioned_state_probe_latency_ms": 4, "versioned_state_ready_max_wave_width": 5, "versioned_state_ready_scheduler_mode": "per_transaction_per_key_version_frontier"},
	})
	if summary.waitCount != 3 || summary.resumeCount != 2 || summary.waitMS != 18 || summary.fetchCount != 5 || summary.fetchCompletedCount != 4 || summary.mode != "transaction_level_suspend_resume" {
		t.Fatalf("native StateReady evidence aggregation mismatch: %+v", summary)
	}
	if summary.versionedWaveCount != 6 || summary.versionedWaitCount != 6 || summary.versionedResolvedCount != 8 || summary.versionedProbeCount != 10 || summary.versionedProbeLatencyMS != 12 || summary.versionedMaxWaveWidth != 5 || summary.versionedMode != "per_transaction_per_key_version_frontier" {
		t.Fatalf("versioned frontier evidence aggregation mismatch: %+v", summary)
	}
}

func TestStateFetchWitnessDigestBindsExactLogicalVersion(t *testing.T) {
	base := StateFetchResponse{
		BlockHash:      "block-version-witness",
		QualifiedKey:   "s0::asset:k",
		Value:          "same-value",
		StateRoot:      "root",
		HomeShard:      "s0",
		ExecutionShard: "s1",
		StateVersion:   3,
		Versioned:      true,
	}
	later := base
	later.StateVersion = 7
	firstDigest := stateFetchWitnessDigest(base, string(tx.AccessReadWrite))
	laterDigest := stateFetchWitnessDigest(later, string(tx.AccessReadWrite))
	if firstDigest == laterDigest {
		t.Fatal("exact logical state version must be bound into the remote witness digest")
	}
}
