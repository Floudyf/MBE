package v5

import (
	"context"
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func batchSIV5TestTx(id string, reads, writes []string) tx.SignedTransaction {
	accesses := make([]tx.AccessItem, 0, len(reads)+len(writes))
	for _, key := range reads {
		accesses = append(accesses, tx.AccessItem{Key: key, Mode: tx.AccessRead, UpdateSemantics: "read"})
	}
	for _, key := range writes {
		accesses = append(accesses, tx.AccessItem{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	}
	return tx.SignedTransaction{
		TxID:             id,
		Sender:           "batch-si-sender-" + id,
		Receiver:         "batch-si-receiver-" + id,
		Nonce:            0,
		Value:            1,
		StateKeys:        append(append([]string(nil), reads...), writes...),
		AccessList:       accesses,
		AccessListSchema: "batch_si_test_access_v1",
		AccessListSource: "batch_si_plugin_test",
		Payload:          "batch_si_plugin_test",
	}
}

func batchSIV5TestBlock(items ...tx.SignedTransaction) realblock.Block {
	b := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: append([]tx.SignedTransaction(nil), items...)}
	for _, item := range items {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	return b
}

func batchSITestPluginConfig(workerCount int) map[string]any {
	return map[string]any{
		"worker_count":   workerCount,
		"partition_mode": execution.BatchSIPartitionWRBP,
		"ordering_mode":  execution.BatchSIOrderingOFAS,
		"priority_mode":  execution.BatchSIPriorityPaper,
		"execution_mode": execution.BatchSIExecutionSnapshotParallel,
	}
}

func batchSITestProfile() map[string]PluginConfig {
	plannerConfig := batchSITestPluginConfig(4)
	return map[string]PluginConfig{
		"workload":              {PluginID: "deterministic_signed_synthetic", Config: map[string]any{}},
		"transaction_admission": {PluginID: "signature_nonce_admission", Config: map[string]any{}},
		"txpool":                {PluginID: "fifo_per_node_mempool", Config: map[string]any{}},
		"sharding":              {PluginID: "deterministic_state_key_sharding", Config: map[string]any{}},
		"routing":               {PluginID: "hash_routing_baseline", Config: map[string]any{}},
		"block_producer":        {PluginID: "time_or_count_block_producer", Config: map[string]any{}},
		"consensus":             {PluginID: "pbft_style_consensus", Config: map[string]any{}},
		"network":               {PluginID: "localhost_tcp_typed_network", Config: map[string]any{}},
		"execution":             {PluginID: batchSIExecutionID, Config: map[string]any{}},
		"scheduler":             {PluginID: batchSISchedulerID, Config: map[string]any{"partition_mode": plannerConfig["partition_mode"], "ordering_mode": plannerConfig["ordering_mode"], "priority_mode": plannerConfig["priority_mode"]}},
		"block_executor":        {PluginID: execution.BatchSIBlockExecutorID, Config: plannerConfig},
		"state_access":          {PluginID: "direct_state_access", Config: map[string]any{}},
		"state_storage":         {PluginID: "persistent_local_state_store", Config: map[string]any{}},
		"cross_shard":           {PluginID: "relay_certificate_protocol", Config: map[string]any{}},
		"commit":                {PluginID: "normal_commit", Config: map[string]any{}},
		"fault_injection":       {PluginID: "faults_disabled", Config: map[string]any{}},
		"metrics":               {PluginID: "runtime_core_metrics", Config: map[string]any{}},
		"observability":         {PluginID: "node_network_consensus_observer", Config: map[string]any{}},
	}
}

func TestBatchSIPluginsRegisterAsIndependentExecutionPath(t *testing.T) {
	registry := BuiltinRegistry()
	executionPlugin, err := registry.Create("execution", batchSIExecutionID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executionPlugin.(ExecutionPlugin); !ok {
		t.Fatalf("batch-si execution classifier does not satisfy ExecutionPlugin: %T", executionPlugin)
	}
	schedulerPlugin, err := registry.Create("scheduler", batchSISchedulerID, batchSITestPluginConfig(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := schedulerPlugin.(ConsensusExecutionPlanner); !ok {
		t.Fatalf("batch-si scheduler does not satisfy ConsensusExecutionPlanner: %T", schedulerPlugin)
	}
	executorPlugin, err := registry.Create("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executorPlugin.(BlockExecutorPlugin); !ok {
		t.Fatalf("batch-si executor does not satisfy BlockExecutorPlugin: %T", executorPlugin)
	}
}

func TestBatchSISchedulerUsesPrivateExecutionModeDefault(t *testing.T) {
	registry := BuiltinRegistry()
	scheduler, err := registry.Create("scheduler", batchSISchedulerID, map[string]any{
		"partition_mode": execution.BatchSIPartitionWRBP,
		"ordering_mode":  execution.BatchSIOrderingOFAS,
		"priority_mode":  execution.BatchSIPriorityPaper,
	})
	if err != nil {
		t.Fatalf("scheduler-only Batch-SI config should use its own execution-mode default: %v", err)
	}
	planner, ok := scheduler.(ConsensusExecutionPlanner)
	if !ok {
		t.Fatalf("Batch-SI scheduler lost ConsensusExecutionPlanner: %T", scheduler)
	}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(batchSIV5TestTx("T1", nil, []string{"k1"})))
	if err != nil {
		t.Fatalf("scheduler-only Batch-SI plan failed: %v", err)
	}
	if planned.Block.ExecutionPlan == nil {
		t.Fatal("scheduler-only Batch-SI config did not create a consensus-bound plan")
	}
}

func TestBatchSIPluginPairingAndPlannerConfigAreEnforced(t *testing.T) {
	profile := batchSITestProfile()
	profile["execution"] = PluginConfig{PluginID: "serial_execution_baseline", Config: map[string]any{}}
	if _, err := InstantiatePlugins(profile); err == nil || !strings.Contains(err.Error(), "must be selected together") {
		t.Fatalf("expected private Batch-SI plugin pairing rejection, got %v", err)
	}

	profile = batchSITestProfile()
	profile["scheduler"] = PluginConfig{PluginID: batchSISchedulerID, Config: map[string]any{
		"partition_mode": execution.BatchSIPartitionSequential,
		"ordering_mode":  execution.BatchSIOrderingOFAS,
		"priority_mode":  execution.BatchSIPriorityPaper,
	}}
	if _, err := InstantiatePlugins(profile); err == nil || !strings.Contains(err.Error(), "planning configuration mismatch") {
		t.Fatalf("expected scheduler/executor config mismatch rejection, got %v", err)
	}
}

func TestBatchSIPluginConfigValidationRejectsInvalidWorkersAndModes(t *testing.T) {
	registry := BuiltinRegistry()
	invalidWorkers := batchSITestPluginConfig(9)
	if _, err := registry.Create("block_executor", execution.BatchSIBlockExecutorID, invalidWorkers); err == nil || !strings.Contains(err.Error(), "worker_count") {
		t.Fatalf("expected invalid worker_count rejection, got %v", err)
	}
	invalidMode := batchSITestPluginConfig(4)
	invalidMode["ordering_mode"] = "other_scheme"
	if _, err := registry.Create("scheduler", batchSISchedulerID, invalidMode); err == nil || !strings.Contains(err.Error(), "ordering_mode") {
		t.Fatalf("expected invalid Batch-SI ordering mode rejection, got %v", err)
	}
}

func TestBatchSIConsensusPlannerDefersCycleAndProducesClosedPlan(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("T1", []string{"k1"}, []string{"k3"}),
		batchSIV5TestTx("T2", []string{"k2"}, []string{"k1"}),
		batchSIV5TestTx("T3", []string{"k3"}, []string{"k2"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(4))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Deferred) == 0 {
		t.Fatal("cyclic Batch-SI candidate must defer at least one transaction before PBFT")
	}
	if len(planned.Block.TxList)+len(planned.Deferred) != len(items) {
		t.Fatalf("planner lost transactions: accepted=%d deferred=%d", len(planned.Block.TxList), len(planned.Deferred))
	}
	if planned.Block.ExecutionPlan == nil || planned.Block.ExecutionPlan.AlgorithmID != execution.BatchSIPlanAlgorithmID {
		t.Fatalf("missing consensus-bound Batch-SI plan: %#v", planned.Block.ExecutionPlan)
	}
	if err := planner.VerifyBlockPlan(planned.Block); err != nil {
		t.Fatalf("accepted-set plan should be closed and verifiable: %v", err)
	}
	plan, err := execution.ParseBatchSIPlan(planned.Block.ExecutionPlan.Payload)
	if err != nil {
		t.Fatal(err)
	}
	if plan.Metrics.OFASAbortedTransactionCount != 0 {
		t.Fatalf("consensus-bound accepted-set plan must not embed candidate-only deferrals: %#v", plan.Metrics)
	}
}

func TestBatchSIBlockExecutorReturnsCompleteGenericResult(t *testing.T) {
	items := []tx.SignedTransaction{
		batchSIV5TestTx("T1", nil, []string{"a"}),
		batchSIV5TestTx("T2", nil, []string{"b"}),
		batchSIV5TestTx("T3", []string{"a"}, []string{"c"}),
	}
	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(4))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(items...))
	if err != nil {
		t.Fatal(err)
	}
	if len(planned.Deferred) != 0 {
		t.Fatalf("independent test transactions should not be deferred: %#v", planned.Deferred)
	}
	realblock.AssignHash(&planned.Block)
	plugin := batchSIBlockExecutor{makeBasic("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(4))}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             planned.Block,
		BaseStateSnapshot: map[string]string{},
		NodeID:            "n0",
		ShardID:           "s0",
		WorkerCount:       4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExecutionResult.BlockExecutorID != execution.BatchSIBlockExecutorID {
		t.Fatalf("wrong executor id: %#v", result.ExecutionResult)
	}
	if len(result.ExecutionResult.Receipts) != len(planned.Block.TxList) || len(result.ExecutionResult.TxDeltas) != len(planned.Block.TxList) {
		t.Fatalf("Batch-SI must return exactly one receipt and delta per accepted block transaction: %#v", result.ExecutionResult)
	}
	if result.PlanDigest == "" || result.PlanDigest != planned.Block.ExecutionPlan.PlanDigest || len(result.StateDelta) == 0 || result.WorkerCount != 4 {
		t.Fatalf("incomplete Batch-SI generic result: %#v", result)
	}
	if reused, ok := result.ActualMetrics["batch_si_cross_scheme_algorithm_reuse"].(bool); !ok || reused {
		t.Fatalf("Batch-SI must report private algorithm execution: %#v", result.ActualMetrics)
	}
	if verified, ok := result.ActualMetrics["batch_si_execution_plan_digest_verified"].(bool); !ok || !verified {
		t.Fatalf("Batch-SI plan digest verification evidence missing: %#v", result.ActualMetrics)
	}
}

func TestBatchSIBlockExecutorAcceptsSignedSyntheticLogicalAccessAliases(t *testing.T) {
	logicalSender := "client_s0_6"
	receiver := "receiver_s0"
	generated, _, _, err := tx.Generate(tx.GenerateOptions{
		Count:      1,
		Sender:     logicalSender,
		Receiver:   receiver,
		StartNonce: 0,
		Value:      1,
		StateKeys:  []string{"shard:s0:account", "asset:6"},
		AccessList: tx.DefaultTransferAccessList(logicalSender, receiver),
		Seed:       "batch-si-v5-logical-account-alias",
	})
	if err != nil {
		t.Fatal(err)
	}
	item := generated[0]
	item.Payload = "v5_safe"
	if item.Sender == logicalSender {
		t.Fatal("fixture did not reproduce the signed-address/logical-alias split")
	}

	planner := batchSIScheduler{makeBasic("scheduler", batchSISchedulerID, batchSITestPluginConfig(2))}
	planned, err := planner.PlanBlock(batchSIV5TestBlock(item))
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	plugin := batchSIBlockExecutor{makeBasic("block_executor", execution.BatchSIBlockExecutorID, batchSITestPluginConfig(2))}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             planned.Block,
		BaseStateSnapshot: map[string]string{},
		NodeID:            "n0",
		ShardID:           "s0",
		WorkerCount:       2,
	})
	if err != nil {
		t.Fatalf("signed synthetic Batch-SI execution failed: %v", err)
	}
	if len(result.ExecutionResult.TxDeltas) != 1 {
		t.Fatalf("expected one transaction delta, got %d", len(result.ExecutionResult.TxDeltas))
	}
	writes := result.ExecutionResult.TxDeltas[0].WriteSet
	for _, key := range []string{
		"balance:" + logicalSender,
		"nonce:" + logicalSender,
		"balance:" + receiver,
	} {
		if _, ok := writes[key]; !ok {
			t.Fatalf("missing logical access-list write %s: %#v", key, writes)
		}
	}
	if _, ok := writes["balance:"+item.Sender]; ok {
		t.Fatalf("physical signing address leaked into Batch-SI writes: %#v", writes)
	}
	if _, ok := writes["nonce:"+receiver]; ok {
		t.Fatalf("read-only receiver nonce was written: %#v", writes)
	}
}
