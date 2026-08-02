package v5

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestMetaTrackCommitEnvelopeDoesNotBlockStateFetchService(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{
		ExecutionBackend: "real_cluster",
		NoFallback:       true,
		NodeConfigs: []NodePlan{
			{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n0"), Validators: []string{"n0"}, PluginProfile: profile},
			{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n1"), Validators: []string{"n1"}, PluginProfile: profile},
		},
	}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Stop()

	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	item := tx.SignedTransaction{
		TxID:       "metatrack-state-fetch-dispatch",
		Sender:     "alice",
		Receiver:   "bob",
		StateKeys:  []string{remoteKey},
		AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}},
	}
	block := realblock.Block{
		ShardID:         "s1",
		Height:          1,
		PreviousHash:    "genesis",
		ProposerID:      "n1",
		Timestamp:       nowForTest().UnixMilli(),
		TxIDs:           []string{item.TxID},
		TxList:          []tx.SignedTransaction{item},
		StateRootBefore: runtime.db.Root(),
		StateRootAfter:  "pending_not_executed",
		ReceiptRoot:     "pending_not_executed",
	}
	realblock.AssignHash(&block)
	responseSent := make(chan struct{}, 1)
	runtime.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		// The commit worker's response is deliberately withheld. A separate
		// inbound request must still be serviced while that worker is blocked.
		if message.MessageType == stateFetchResponseMessage {
			responseSent <- struct{}{}
		}
		return nil
	}
	envelope, err := p2p.NewEnvelope(p2p.MessagePBFTCommit, "n0", "n1", "s1", block.Height, 0, block.Height, Proposal{Block: block})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- runtime.handle(ctx, envelope) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("commit envelope dispatch returned error: %v", err)
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("PBFT commit envelope remained blocked behind MetaTrack state fetch")
	}
	deadline := time.Now().Add(time.Second)
	for {
		runtime.mu.Lock()
		phase := runtime.commitPhase
		runtime.mu.Unlock()
		if phase == "remote_state_prefetch" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("commit worker did not begin the remote state fetch")
		}
		time.Sleep(time.Millisecond)
	}
	requestEnvelope, err := p2p.NewEnvelope(stateFetchRequestMessage, "n0", "n1", "s0", 1, 0, 1, StateFetchRequest{
		RequestID:      "independent-request",
		TxID:           "independent-tx",
		BlockHash:      "independent-block",
		Key:            remoteKey,
		HomeShard:      "s1",
		ExecutionShard: "s0",
		AccessKind:     string(tx.AccessRead),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handle(ctx, requestEnvelope); err != nil {
		t.Fatalf("state fetch request handler failed while commit worker was blocked: %v", err)
	}
	select {
	case <-responseSent:
	case <-time.After(150 * time.Millisecond):
		t.Fatal("state fetch request was not serviced while commit worker awaited a remote response")
	}
}

func TestMetaTrackStateFetchCancellationCleansWaiterAndIgnoresLateResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n0"), Validators: []string{"n0"}, PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n1"), Validators: []string{"n1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Stop()

	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	item := tx.SignedTransaction{TxID: "cancelled-fetch", Sender: "alice", Receiver: "bob", StateKeys: []string{remoteKey}, AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	block := realblock.Block{ShardID: "s1", Height: 1, PreviousHash: "genesis", ProposerID: "n1", Timestamp: nowForTest().UnixMilli(), TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}}
	realblock.AssignHash(&block)
	requestSent := make(chan struct{}, 1)
	runtime.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		if message.MessageType == stateFetchRequestMessage {
			requestSent <- struct{}{}
		}
		return nil
	}
	fetchCtx, fetchCancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		_, _, err := runtime.fetchRemoteState(fetchCtx, block, item, item.AccessList[0], "s0")
		done <- err
	}()
	select {
	case <-requestSent:
	case <-time.After(time.Second):
		t.Fatal("test setup did not issue the remote state request")
	}
	fetchCancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled remote state fetch unexpectedly succeeded")
		}
	case <-time.After(time.Second):
		t.Fatal("cancelled remote state fetch did not return")
	}

	requestID := stableTextDigest("n1|cancelled-fetch|" + block.BlockHash + "|" + remoteKey + "|s0|s1")
	runtime.mu.Lock()
	_, waiting := runtime.stateFetchWaiters[requestID]
	runtime.mu.Unlock()
	if waiting {
		t.Fatal("cancelled remote state fetch left its waiter registered")
	}
	runtime.handleStateFetchResponse(StateFetchResponse{RequestID: requestID, Success: true})
	runtime.mu.Lock()
	remaining := len(runtime.stateFetchWaiters)
	runtime.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("late response recreated or retained waiters: %d", remaining)
	}
}

func TestMetaTrackStateFetchWorkersDoNotBlockResponseDispatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n0"), Validators: []string{"n0"}, PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n1"), Validators: []string{"n1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Stop()

	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	workersEntered := make(chan struct{}, stateFetchWorkerCount)
	releaseWorkers := make(chan struct{})
	runtime.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		if message.MessageType == stateFetchResponseMessage {
			workersEntered <- struct{}{}
			<-releaseWorkers
		}
		return nil
	}
	for i := range stateFetchWorkerCount {
		envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, "n0", "n1", "s0", 1, 0, 1, StateFetchRequest{RequestID: "blocked-worker-" + string(rune('a'+i)), TxID: "tx", BlockHash: "block", Key: remoteKey, HomeShard: "s1", ExecutionShard: "s0", AccessKind: string(tx.AccessRead)})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.handle(ctx, envelope); err != nil {
			t.Fatalf("enqueue state fetch request %d: %v", i, err)
		}
	}
	for range stateFetchWorkerCount {
		select {
		case <-workersEntered:
		case <-time.After(time.Second):
			t.Fatal("state fetch workers did not begin their blocking response sends")
		}
	}

	requestID := "response-must-not-wait"
	waiter := make(chan StateFetchResponse, 1)
	runtime.mu.Lock()
	runtime.stateFetchWaiters[requestID] = waiter
	runtime.mu.Unlock()
	responseEnvelope, err := p2p.NewEnvelope(stateFetchResponseMessage, "n0", "n1", "s0", 1, 0, 1, StateFetchResponse{RequestID: requestID, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := runtime.handle(ctx, responseEnvelope); err != nil {
		t.Fatalf("dispatch state fetch response: %v", err)
	}
	if elapsed := time.Since(started); elapsed > 150*time.Millisecond {
		t.Fatalf("response dispatch waited behind state fetch workers: %s", elapsed)
	}
	select {
	case response := <-waiter:
		if !response.Success {
			t.Fatal("response was not delivered to its waiter")
		}
	case <-time.After(150 * time.Millisecond):
		t.Fatal("response waiter was not signaled")
	}
	close(releaseWorkers)
}

func TestMetaTrackBlockedResponseSendsDoNotStarveStateFetchPreparation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n0"), Validators: []string{"n0"}, PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n1"), Validators: []string{"n1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer runtime.Stop()

	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	entered := make(chan struct{}, stateFetchWorkerCount)
	release := make(chan struct{})
	defer close(release)
	runtime.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		if message.MessageType == stateFetchResponseMessage {
			entered <- struct{}{}
			<-release
		}
		return nil
	}
	for i := range stateFetchWorkerCount {
		envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, "n0", "n1", "s0", 1, 0, 1, StateFetchRequest{RequestID: "blocked-preparation-" + string(rune('a'+i)), TxID: "tx", BlockHash: "blocked-block", Key: remoteKey, HomeShard: "s1", ExecutionShard: "s0", AccessKind: string(tx.AccessRead)})
		if err != nil {
			t.Fatal(err)
		}
		if err := runtime.handle(ctx, envelope); err != nil {
			t.Fatal(err)
		}
	}
	for range stateFetchWorkerCount {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("test setup did not block every state fetch response sender")
		}
	}

	request := StateFetchRequest{RequestID: "must-prepare-after-blocked-response", TxID: "next", BlockHash: "next-block", Key: remoteKey, HomeShard: "s1", ExecutionShard: "s0", AccessKind: string(tx.AccessRead)}
	envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, "n0", "n1", "s0", 1, 0, 1, request)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.handle(ctx, envelope); err != nil {
		t.Fatal(err)
	}
	cacheKey := stateFetchWitnessKey(request)
	deadline := time.Now().Add(150 * time.Millisecond)
	for {
		runtime.mu.Lock()
		_, prepared := runtime.stateFetchWitnesses[cacheKey]
		runtime.mu.Unlock()
		if prepared {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("blocked response sends starved subsequent state fetch preparation")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestMetaTrackPrefetchFailureRollsBackAndReleasesLeaderProposal(t *testing.T) {
	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n0"), Validators: []string{"n0"}, PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, "n1"), Validators: []string{"n1"}, PluginProfile: profile},
	}}
	runtime, err := newNodeRuntime(plan, plan.NodeConfigs[1])
	if err != nil {
		t.Fatal(err)
	}
	remoteKey := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	item := tx.SignedTransaction{TxID: "prefetch-failure", Sender: "alice", Receiver: "bob", StateKeys: []string{remoteKey}, AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
	block := realblock.Block{ShardID: "s1", Height: 1, PreviousHash: "genesis", ProposerID: "n1", Timestamp: nowForTest().UnixMilli(), TxIDs: []string{item.TxID}, TxList: []tx.SignedTransaction{item}}
	realblock.AssignHash(&block)
	runtime.mu.Lock()
	runtime.proposals[block.BlockHash] = block
	runtime.proposalInFlight = true
	runtime.proposalInFlightHash = block.BlockHash
	runtime.proposalStartedAt = time.Now()
	runtime.mu.Unlock()
	runtime.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		if message.MessageType == stateFetchRequestMessage {
			return errors.New("injected state fetch transport failure")
		}
		return nil
	}
	rootBefore := runtime.db.Root()
	if err := runtime.finalize(context.Background(), block); err == nil {
		t.Fatal("prefetch failure unexpectedly finalized the block")
	}
	if rootAfter := runtime.db.Root(); rootAfter != rootBefore {
		t.Fatalf("prefetch failure changed state root: before=%s after=%s", rootBefore, rootAfter)
	}
	runtime.mu.Lock()
	inFlight := runtime.proposalInFlight
	_, proposalRetained := runtime.proposals[block.BlockHash]
	committing := runtime.committing[block.BlockHash]
	runtime.mu.Unlock()
	if inFlight || proposalRetained || committing {
		t.Fatalf("prefetch failure left proposal state stuck: in_flight=%v retained=%v committing=%v", inFlight, proposalRetained, committing)
	}
	if err := os.MkdirAll(runtime.node.DataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := runtime.writeRuntimeStatus(); err != nil {
		t.Fatal(err)
	}
	statusBytes, err := os.ReadFile(filepath.Join(runtime.node.DataDir, "node_runtime_status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		CommitPhase             string                 `json:"commit_phase"`
		LastProposalError       string                 `json:"last_proposal_error"`
		LastCommitFailure       CommitFailure          `json:"last_commit_failure"`
		StateFetchQueueSize     int                    `json:"state_fetch_request_queue_depth"`
		PendingStateFetchCount  int                    `json:"pending_state_fetch_count"`
		StateFetchFailures      []StateFetchDiagnostic `json:"state_fetch_failures"`
		LastStateFetch          StateFetchDiagnostic   `json:"last_state_fetch"`
		LastStateFetchService   StateFetchDiagnostic   `json:"last_state_fetch_service"`
		StateFetchServiceErrors []StateFetchDiagnostic `json:"state_fetch_service_errors"`
	}
	if err := json.Unmarshal(statusBytes, &status); err != nil {
		t.Fatal(err)
	}
	if status.CommitPhase != "state_access_error" {
		t.Fatalf("commit phase=%q, want state_access_error", status.CommitPhase)
	}
	if !strings.Contains(status.LastProposalError, "injected state fetch transport failure") {
		t.Fatalf("last proposal error missing injected cause: %q", status.LastProposalError)
	}
	if status.LastCommitFailure.Phase != "state_access_error" || status.LastCommitFailure.Height != block.Height || status.LastCommitFailure.BlockHash != block.BlockHash || !strings.Contains(status.LastCommitFailure.Error, "injected state fetch transport failure") || status.LastCommitFailure.Timestamp == 0 {
		t.Fatalf("commit failure diagnostic mismatch: %#v", status.LastCommitFailure)
	}
	if status.StateFetchQueueSize != 0 {
		t.Fatalf("unexpected state fetch queue depth after direct send failure: %d", status.StateFetchQueueSize)
	}
	if status.PendingStateFetchCount != 0 {
		t.Fatalf("failed prefetch left pending RPCs: %d", status.PendingStateFetchCount)
	}
	if status.LastStateFetch.RequestID == "" || status.LastStateFetch.TxID != item.TxID || status.LastStateFetch.Key != remoteKey || status.LastStateFetch.HomeShard != "s0" || status.LastStateFetch.ExecutionShard != "s1" || status.LastStateFetch.Stage != "send_error" || !strings.Contains(status.LastStateFetch.Error, "injected state fetch transport failure") {
		t.Fatalf("last fetch diagnostic missing request identity or send failure: %#v", status.LastStateFetch)
	}
	if len(status.StateFetchFailures) != 1 || status.StateFetchFailures[0].RequestID != status.LastStateFetch.RequestID {
		t.Fatalf("fetch failure history mismatch: %#v", status.StateFetchFailures)
	}
	if status.LastStateFetchService.Stage != "" || len(status.StateFetchServiceErrors) != 0 {
		t.Fatalf("request-side send failure should not fabricate home service activity: last=%#v errors=%#v", status.LastStateFetchService, status.StateFetchServiceErrors)
	}
}

func TestMetaTrackBidirectionalStateFetchStaysLiveUnderValidatorConcurrency(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true}
	for shardIndex, shardID := range []string{"s0", "s1"} {
		for validatorIndex := range 4 {
			nodeID := fmt.Sprintf("n%d", shardIndex*4+validatorIndex)
			validators := []string{"n0", "n1", "n2", "n3"}
			if shardID == "s1" {
				validators = []string{"n4", "n5", "n6", "n7"}
			}
			plan.NodeConfigs = append(plan.NodeConfigs, NodePlan{NodeID: nodeID, ShardID: shardID, Role: "validator", Leader: validatorIndex == 0, ListenAddr: freeLocalAddr(t), DataDir: filepath.Join(root, nodeID), Validators: validators, PluginProfile: profile})
		}
	}
	runtimes := make(map[string]*NodeRuntime, len(plan.NodeConfigs))
	for _, node := range plan.NodeConfigs {
		runtime, err := newNodeRuntime(plan, node)
		if err != nil {
			t.Fatal(err)
		}
		runtimes[node.NodeID] = runtime
		if err := runtime.Start(ctx); err != nil {
			t.Fatal(err)
		}
		defer runtime.Stop()
	}
	keyS0 := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	keyS1 := keyWithHomeShard(t, "s1", []string{"s0", "s1"})
	runtimes["n0"].db.Set(keyS0, "41")
	runtimes["n4"].db.Set(keyS1, "59")

	start := make(chan struct{})
	errs := make(chan error, 8)
	var workers sync.WaitGroup
	for _, nodeID := range []string{"n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7"} {
		runtime := runtimes[nodeID]
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			homeShard, remoteKey := "s0", keyS0
			if runtime.node.ShardID == "s0" {
				homeShard, remoteKey = "s1", keyS1
			}
			for sequence := 0; sequence < 32; sequence++ {
				block := realblock.Block{BlockHash: fmt.Sprintf("bidirectional-%s-%d", runtime.node.NodeID, sequence), Height: 1, ShardID: runtime.node.ShardID}
				item := tx.SignedTransaction{TxID: fmt.Sprintf("bidirectional-tx-%s-%d", runtime.node.NodeID, sequence), AccessList: []tx.AccessItem{{Key: remoteKey, Mode: tx.AccessRead, UpdateSemantics: "validate"}}}
				response, _, err := runtime.fetchRemoteState(ctx, block, item, item.AccessList[0], homeShard)
				if err != nil {
					errs <- fmt.Errorf("%s sequence %d: %w", runtime.node.NodeID, sequence, err)
					return
				}
				want := "41"
				if homeShard == "s1" {
					want = "59"
				}
				if response.Value != want {
					errs <- fmt.Errorf("%s sequence %d: remote value=%q want %q", runtime.node.NodeID, sequence, response.Value, want)
					return
				}
			}
		}()
	}
	close(start)
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(done)
	}()
	select {
	case err := <-errs:
		t.Fatal(err)
	case <-done:
	case <-ctx.Done():
		t.Fatalf("bidirectional state fetch did not complete: %v", ctx.Err())
	}
	for nodeID, runtime := range runtimes {
		runtime.mu.Lock()
		pending := len(runtime.pendingStateFetches)
		failures := append([]StateFetchDiagnostic(nil), runtime.stateFetchFailures...)
		runtime.mu.Unlock()
		if pending != 0 || len(failures) != 0 {
			t.Fatalf("%s retained fetch state after bidirectional traffic: pending=%d failures=%#v", nodeID, pending, failures)
		}
	}
}
