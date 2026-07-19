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

func TestRuntimeScheduleBlockUsesSchedulerPluginAndRehashesProposal(t *testing.T) {
	profile := testMetaTrackProfile()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{{NodeID: "n-s0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: t.TempDir(), Validators: []string{"n-s0"}, PluginProfile: profile}}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	conservative := tx.SignedTransaction{TxID: "conservative", AccessList: []tx.AccessItem{{Key: "state:rw", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}}
	fast := tx.SignedTransaction{TxID: "fast", AccessList: []tx.AccessItem{{Key: "state:delta", Mode: tx.AccessCommutativeDelta, UpdateSemantics: "add", Delta: 1}}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n-s0", Timestamp: 1, TxIDs: []string{conservative.TxID, fast.TxID}, TxList: []tx.SignedTransaction{conservative, fast}, StateRootBefore: "empty", StateRootAfter: "pending_not_executed", ReceiptRoot: "pending_not_executed"}
	realblock.AssignHash(&block)
	originalHash := block.BlockHash

	scheduled := runtime.scheduleBlock(block)

	if scheduled.TxIDs[0] != "fast" || scheduled.TxIDs[1] != "conservative" {
		t.Fatalf("runtime proposal was not scheduler ordered: %#v", scheduled.TxIDs)
	}
	if scheduled.BlockHash == originalHash {
		t.Fatal("scheduled proposal hash did not change after order change")
	}
	if scheduled.TxRoot == block.TxRoot {
		t.Fatal("scheduled proposal tx root did not change after order change")
	}
	if len(runtime.schedulerRows) == 0 {
		t.Fatal("runtime did not record scheduler trace rows")
	}
	if !schedulerRowsSaw(runtime.schedulerRows, "fast", "fast_queue", false, false) {
		t.Fatalf("runtime scheduler trace missing fast queue dispatch evidence: %#v", runtime.schedulerRows)
	}
	if !schedulerRowsCarryQueueDepths(runtime.schedulerRows) {
		t.Fatalf("runtime scheduler trace missing queue depth columns: %#v", runtime.schedulerRows)
	}
}

func TestRuntimeScheduleTraceMarksRemoteHomeWorkAsStolen(t *testing.T) {
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

	if !schedulerRowsSawStolen(runtime.schedulerRows, "remote-home") {
		t.Fatalf("runtime scheduler trace should mark remote-home execution as stolen work: %#v", runtime.schedulerRows)
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
		"block_stm_metrics":     execution.BlockSTMMetrics{WorkerCount: 4, MaximumParallelWidth: 2, ExecutionTaskCount: 1, ValidationTaskCount: 1, IncarnationHistogram: map[int]int{0: 1}},
	}})
	if err != nil {
		t.Fatal(err)
	}

	summary := readJSONMap(t, filepath.Join(dataDir, "block_stm_summary.json"))
	if summary["serial_equivalent"] != false {
		t.Fatalf("block_stm_summary.json must preserve kernel equivalence evidence, got %#v", summary["serial_equivalent"])
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
		"block_producer":        {PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 10, "interval_ms": 150}},
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

func schedulerRowsCarryQueueDepths(rows [][]string) bool {
	for _, row := range rows {
		if len(row) >= 18 && row[13] != "" && row[14] != "" && row[15] != "" && row[16] != "" && row[17] != "" {
			return true
		}
	}
	return false
}
