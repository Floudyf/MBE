package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
)

func TestPorygonV19BlockSizeUsesCommonBlockSizeBeforeLegacyTransactionCap(t *testing.T) {
	producer := porygonBlockProducer{makeBasic("block_producer", porygonBlockProducerID, map[string]any{
		"block_size":             1000,
		"transaction_block_size": 100,
	})}
	if got := producer.BlockSize(); got != 1000 {
		t.Fatalf("Porygon hidden transaction block cap still overrides common block_size: got=%d want=1000", got)
	}
}

func TestPorygonV19ESCResultDigestIgnoresLocalTiming(t *testing.T) {
	base := PorygonESCWaveResult{BlockHash: "b", Height: 1, Wave: 0, ExecutionShardID: "s0", SenderNodeID: "n0", BusinessExecutionUS: 10}
	left := sealPorygonESCWaveResult(base)
	base.BusinessExecutionUS = 999999
	right := sealPorygonESCWaveResult(base)
	if left.ResultDigest != right.ResultDigest {
		t.Fatalf("local timing contaminated Porygon semantic result digest: %s != %s", left.ResultDigest, right.ResultDigest)
	}
}

func TestPorygonV19ESCExecutionResultThresholdCertificate(t *testing.T) {
	nodes := []NodePlan{}
	for i := 0; i < 8; i++ {
		nodes = append(nodes, NodePlan{
			NodeID: fmt.Sprintf("n%d", i), ShardID: "porygon-global", ConsensusDomainID: "porygon-global",
			ExecutionShardID: fmt.Sprintf("s%d", i/4), Leader: i == 0,
		})
	}
	runtime := &NodeRuntime{plan: Plan{NodeConfigs: nodes}, node: nodes[0]}
	for _, tc := range []struct {
		shard string
		voter int
	}{{"s0", 0}, {"s0", 1}, {"s0", 2}, {"s1", 4}, {"s1", 5}, {"s1", 6}} {
		result := sealPorygonESCWaveResult(PorygonESCWaveResult{
			BlockHash: "block-1", Height: 1, Wave: 0, ExecutionShardID: tc.shard, SenderNodeID: fmt.Sprintf("n%d", tc.voter),
		})
		if err := runtime.acceptPorygonESCWaveResult(result); err != nil {
			t.Fatal(err)
		}
	}
	cert, ready, err := runtime.tryBuildPorygonESCWaveCertificate("block-1", 1, 0, []string{"s0", "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if !ready || len(cert.Entries) != 2 {
		t.Fatalf("Porygon Te execution-result certificate not built: ready=%v cert=%+v", ready, cert)
	}
	if err := runtime.validatePorygonCertificateAgainstPlan(cert, []string{"s0", "s1"}); err != nil {
		t.Fatal(err)
	}
	for _, entry := range cert.Entries {
		if len(entry.Voters) != 3 {
			t.Fatalf("4-member ESC must require >1/2 consistent execution results (3), entry=%+v", entry)
		}
	}
}

func TestPorygonV19DistributedESCOwnershipExecutesOnlyLocalTransactions(t *testing.T) {
	left, right := findDisjointCrossESCPair(t)
	block := porygonFixtureBlock(t, left, right)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	var plan porygonExecutionPlan
	if planned.Block.ExecutionPlan == nil {
		t.Fatal("missing Porygon execution plan")
	}
	if err := json.Unmarshal(planned.Block.ExecutionPlan.Payload, &plan); err != nil {
		t.Fatal(err)
	}
	if len(plan.Waves) != 1 || len(plan.Assignments) != 2 {
		t.Fatalf("fixture must be one disjoint wave: %+v", plan)
	}
	localShard := fmt.Sprintf("s%d", plan.Assignments[0].ExecutionShard)
	localExpected := 0
	for _, assignment := range plan.Assignments {
		if fmt.Sprintf("s%d", assignment.ExecutionShard) == localShard {
			localExpected++
		}
	}
	byID := map[string]struct{ index int }{}
	for index, item := range planned.Block.TxList {
		byID[item.TxID] = struct{ index int }{index: index}
	}

	exchange := func(ctx context.Context, local PorygonESCWaveResult, required []string) (PorygonESCWaveCertificate, error) {
		local.SenderNodeID = "n-local"
		local = sealPorygonESCWaveResult(local)
		entries := []PorygonESCWaveCertificateEntry{}
		serial := execution.NewSerialExecutor()
		for _, shardID := range required {
			if shardID == local.ExecutionShardID {
				entries = append(entries, PorygonESCWaveCertificateEntry{ExecutionShardID: shardID, ResultDigest: local.ResultDigest, Voters: []string{"n0", "n1", "n2"}, Result: local})
				continue
			}
			remote := PorygonESCWaveResult{BlockHash: local.BlockHash, Height: local.Height, Wave: local.Wave, ExecutionShardID: shardID, SenderNodeID: "n-remote"}
			for _, assignment := range plan.Assignments {
				if assignment.Wave != local.Wave || fmt.Sprintf("s%d", assignment.ExecutionShard) != shardID {
					continue
				}
				item := planned.Block.TxList[assignment.OriginalIndex]
				snapshot := porygonTransactionSnapshot(map[string]string{}, planned.Block.ShardID, item)
				receipt, delta := serial.ExecuteTransaction(planned.Block, item, snapshot, byID[item.TxID].index)
				remote.Results = append(remote.Results, PorygonWaveTxResult{TxID: item.TxID, Receipt: receipt, Delta: delta})
			}
			remote = sealPorygonESCWaveResult(remote)
			entries = append(entries, PorygonESCWaveCertificateEntry{ExecutionShardID: shardID, ResultDigest: remote.ResultDigest, Voters: []string{"n4", "n5", "n6"}, Result: remote})
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].ExecutionShardID < entries[j].ExecutionShardID })
		return sealPorygonESCWaveCertificate(PorygonESCWaveCertificate{BlockHash: local.BlockHash, Height: local.Height, Wave: local.Wave, LeaderNodeID: "n0", Entries: entries}), nil
	}

	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	distributed, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4,
		ExecutionShardID: localShard, PorygonWaveExchange: exchange,
	})
	if err != nil {
		t.Fatal(err)
	}
	legacy, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if got := len(distributed.BusinessAttempts); got != localExpected {
		t.Fatalf("local ESC business attempts=%d want=%d: %+v", got, localExpected, distributed.BusinessAttempts)
	}
	if len(distributed.ExecutionResult.Receipts) != len(planned.Block.TxList) {
		t.Fatalf("global materialization lost certified remote results: receipts=%d want=%d", len(distributed.ExecutionResult.Receipts), len(planned.Block.TxList))
	}
	if distributed.ExecutionResult.StateRootAfter != legacy.ExecutionResult.StateRootAfter {
		t.Fatalf("distributed ESC materialization changed state root: distributed=%s legacy=%s", distributed.ExecutionResult.StateRootAfter, legacy.ExecutionResult.StateRootAfter)
	}
	if distributed.ActualMetrics["porygon_esc_execution_ownership_mode"] != "distributed_esc_quorum_result_exchange_v1" {
		t.Fatalf("ownership truth metric missing: %#v", distributed.ActualMetrics)
	}
	if got := intMetric(t, distributed.ActualMetrics, "porygon_local_business_execution_count"); got != localExpected {
		t.Fatalf("local business count=%d want=%d", got, localExpected)
	}
	if got := intMetric(t, distributed.ActualMetrics, "porygon_certified_remote_business_result_count"); got != len(planned.Block.TxList)-localExpected {
		t.Fatalf("remote certified count=%d want=%d", got, len(planned.Block.TxList)-localExpected)
	}
}

func TestPorygonV19WaveExchangeConvergesAcrossTwoESCs(t *testing.T) {
	validatorIDs := make([]string, 8)
	nodes := make([]NodePlan, 8)
	for i := 0; i < 8; i++ {
		validatorIDs[i] = fmt.Sprintf("n%d", i)
	}
	for i := 0; i < 8; i++ {
		nodes[i] = NodePlan{
			NodeID: fmt.Sprintf("n%d", i), ShardID: "porygon-global", ConsensusDomainID: "porygon-global",
			ExecutionShardID: fmt.Sprintf("s%d", i/4), Leader: i == 0, Validators: append([]string(nil), validatorIDs...),
		}
	}
	plan := Plan{NodeConfigs: nodes}
	runtimes := map[string]*NodeRuntime{}
	for _, node := range nodes {
		runtime := &NodeRuntime{
			plan: plan, node: node,
			plugins: RuntimePlugins{BlockExecutor: porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}},
		}
		runtimes[node.NodeID] = runtime
		defer porygonESCExchangeStates.Delete(runtime)
	}
	for _, runtime := range runtimes {
		runtime := runtime
		runtime.sendToNodeHook = func(ctx context.Context, nodeID string, msg p2p.MessageEnvelope) error {
			target := runtimes[nodeID]
			if target == nil {
				return fmt.Errorf("unknown target %s", nodeID)
			}
			switch msg.MessageType {
			case porygonESCWaveResultMessage:
				return target.handlePorygonESCWaveResult(ctx, msg)
			case porygonESCWaveCertificateMessage:
				return target.handlePorygonESCWaveCertificate(ctx, msg)
			default:
				return fmt.Errorf("unexpected message type %s", msg.MessageType)
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	errCh := make(chan error, len(runtimes))
	digestCh := make(chan string, len(runtimes))
	for _, runtime := range runtimes {
		runtime := runtime
		wg.Add(1)
		go func() {
			defer wg.Done()
			cert, err := runtime.porygonWaveExchange(ctx, PorygonESCWaveResult{BlockHash: "block-exchange", Height: 1, Wave: 0}, []string{"s0", "s1"})
			if err != nil {
				errCh <- err
				return
			}
			if len(cert.Entries) != 2 {
				errCh <- fmt.Errorf("certificate entries=%d want=2", len(cert.Entries))
				return
			}
			digestCh <- cert.CertificateDigest
		}()
	}
	wg.Wait()
	close(errCh)
	close(digestCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	var digest string
	count := 0
	for got := range digestCh {
		count++
		if got == "" {
			t.Fatal("empty Porygon ESC certificate digest")
		}
		if digest == "" {
			digest = got
		} else if got != digest {
			t.Fatalf("replicas observed different ESC certificates: %s != %s", got, digest)
		}
	}
	if count != 8 {
		t.Fatalf("certificate completion count=%d want=8", count)
	}
}
