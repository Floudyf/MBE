package v5

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func TestGroundhogPluginsRegisterThroughBuiltinRegistry(t *testing.T) {
	registry := BuiltinRegistry()
	producer, err := registry.Create("block_producer", groundhogBlockProducerID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := producer.(BlockProducerPlugin); !ok {
		t.Fatalf("groundhog producer does not satisfy BlockProducerPlugin: %T", producer)
	}
	executor, err := registry.Create("block_executor", groundhogBlockExecutorID, map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := executor.(BlockExecutorPlugin); !ok {
		t.Fatalf("groundhog executor does not satisfy BlockExecutorPlugin: %T", executor)
	}
}

func TestGroundhogPluginPairingIsEnforced(t *testing.T) {
	profile := groundhogTestProfile()
	profile["block_executor"] = PluginConfig{PluginID: "serial_block_executor", Config: map[string]any{"worker_count": 1}}
	_, err := InstantiatePlugins(profile)
	if err == nil || !strings.Contains(err.Error(), "must be selected together") {
		t.Fatalf("expected producer/executor pairing rejection, got %v", err)
	}
}

func TestGroundhogPluginDefaultsUseReferenceOrderedSetInitialLimit(t *testing.T) {
	profile := groundhogTestProfile()
	plugins, err := InstantiatePlugins(profile)
	if err != nil {
		t.Fatal(err)
	}
	producer := plugins.BlockProducer.(groundhogBlockProducer)
	executor := plugins.BlockExecutor.(groundhogBlockExecutor)
	if got := intValue(producer.config["ordered_set_limit"]); got != 0 {
		t.Fatalf("empty profile should leave producer override unset, got %d", got)
	}
	if got := intValue(executor.config["ordered_set_limit"]); got != 0 {
		t.Fatalf("empty profile should leave executor override unset, got %d", got)
	}
	producerLimit := intValue(producer.config["ordered_set_limit"])
	if producerLimit == 0 {
		producerLimit = execution.GroundhogOrderedSetInitialLimit
	}
	executorLimit := intValue(executor.config["ordered_set_limit"])
	if executorLimit == 0 {
		executorLimit = execution.GroundhogOrderedSetInitialLimit
	}
	if producerLimit != 64 || executorLimit != 64 {
		t.Fatalf("Groundhog default ordered-set limit must be 64: producer=%d executor=%d", producerLimit, executorLimit)
	}
}

func TestGroundhogPluginRejectsOrderedSetLimitAboveReferenceMaximum(t *testing.T) {
	profile := groundhogTestProfile()
	profile["block_producer"] = PluginConfig{PluginID: groundhogBlockProducerID, Config: map[string]any{"ordered_set_limit": 65_536}}
	profile["block_executor"] = PluginConfig{PluginID: groundhogBlockExecutorID, Config: map[string]any{"ordered_set_limit": 65_536}}
	_, err := InstantiatePlugins(profile)
	if err == nil || !strings.Contains(err.Error(), "must be between 1 and") {
		t.Fatalf("expected ordered-set maximum rejection, got %v", err)
	}
}

func TestGroundhogPluginSetLimitsMustMatch(t *testing.T) {
	profile := groundhogTestProfile()
	profile["block_producer"] = PluginConfig{PluginID: groundhogBlockProducerID, Config: map[string]any{"ordered_set_limit": 32}}
	profile["block_executor"] = PluginConfig{PluginID: groundhogBlockExecutorID, Config: map[string]any{"ordered_set_limit": 64}}
	_, err := InstantiatePlugins(profile)
	if err == nil || !strings.Contains(err.Error(), "ordered_set_limit mismatch") {
		t.Fatalf("expected Groundhog set-limit mismatch rejection, got %v", err)
	}
}

func TestGroundhogProducerDefersAggregateConflictAndKeepsSelectedReserved(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count:       2,
		Sender:      "groundhog-sender",
		Receiver:    "groundhog-receiver",
		StartNonce:  0,
		Value:       600_000,
		Seed:        "groundhog-producer-test",
		StartTimeMS: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := mempool.New("n0", "s0", mempool.DefaultPolicy(), nil)
	for _, item := range items {
		if admitted := pool.AdmitAt(item, time.UnixMilli(item.Timestamp)); !admitted.Accepted {
			t.Fatalf("failed to admit %s: %#v", item.TxID, admitted)
		}
	}
	plugin := groundhogBlockProducer{makeBasic("block_producer", groundhogBlockProducerID, map[string]any{
		"block_size":                2,
		"candidate_scan_multiplier": 1,
		"ordered_set_limit":         32,
	})}
	candidate, err := plugin.BuildCandidate(BlockProductionInput{
		Pool:              pool,
		Proposer:          realblock.NewProposer("n0", "s0"),
		Limit:             2,
		Now:               time.UnixMilli(10),
		Context:           context.Background(),
		BaseStateSnapshot: map[string]string{},
		WorkerCount:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(candidate.TxList) != 1 {
		t.Fatalf("expected one selected transaction after aggregate overspend, got %d", len(candidate.TxList))
	}
	if pool.Len() != 2 {
		t.Fatalf("producer must not remove transactions before commit, pool size=%d", pool.Len())
	}
	if pool.ReservedCount() != 1 {
		t.Fatalf("selected transaction must remain reserved and deferred transaction released, reserved=%d", pool.ReservedCount())
	}
	if candidate.ProposalEvidence == nil || candidate.ProposalEvidence.AlgorithmID != groundhogCandidateSelectionEvidenceID {
		t.Fatalf("missing Groundhog proposal selection evidence: %#v", candidate.ProposalEvidence)
	}
	if stableTextDigest(string(candidate.ProposalEvidence.Payload)) != candidate.ProposalEvidence.PayloadDigest {
		t.Fatal("Groundhog proposal evidence digest mismatch")
	}
	var evidence groundhogCandidateSelectionEvidence
	if err := json.Unmarshal(candidate.ProposalEvidence.Payload, &evidence); err != nil {
		t.Fatal(err)
	}
	if evidence.CandidateCount != 2 || len(evidence.SelectedTxIDs) != 1 || len(evidence.DeferredTxIDs) != 1 || len(evidence.DeferredReasons) != 1 {
		t.Fatalf("incomplete Groundhog selection evidence: %#v", evidence)
	}
	reservedAgain := pool.ReserveReady(2)
	if len(reservedAgain) != 1 || reservedAgain[0].TxID == candidate.TxList[0].TxID {
		t.Fatalf("expected only deferred transaction to be reservable again: %#v", reservedAgain)
	}
	pool.ReleaseReserved(reservedAgain)
	pool.ReleaseReserved(candidate.TxList)
}

func TestGroundhogBlockExecutorReturnsCompleteGenericResult(t *testing.T) {
	txs := independentTransferTxs(8)
	b := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: txs}
	for _, item := range txs {
		b.TxIDs = append(b.TxIDs, item.TxID)
	}
	realblock.AssignHash(&b)
	plugin := groundhogBlockExecutor{makeBasic("block_executor", groundhogBlockExecutorID, map[string]any{
		"worker_count":      4,
		"ordered_set_limit": 64,
	})}
	result, err := plugin.ExecuteBlock(context.Background(), BlockExecutionInput{
		Block:             b,
		BaseStateSnapshot: map[string]string{},
		NodeID:            "n0",
		ShardID:           "s0",
		WorkerCount:       4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.ExecutionResult.BlockExecutorID != execution.GroundhogBlockExecutorID {
		t.Fatalf("wrong executor id: %#v", result.ExecutionResult)
	}
	if len(result.ExecutionResult.Receipts) != len(txs) || len(result.ExecutionResult.TxDeltas) != len(txs) {
		t.Fatalf("groundhog must return exactly one receipt and delta per fixed-block transaction: %#v", result.ExecutionResult)
	}
	if result.PlanDigest == "" || len(result.StateDelta) == 0 || result.WorkerCount != 4 {
		t.Fatalf("incomplete generic result: %#v", result)
	}
	if result.ActualMetrics["groundhog_fallback_mode"] != "disabled" {
		t.Fatalf("groundhog must not use serial fallback: %#v", result.ActualMetrics)
	}
	if intMetric(t, result.ActualMetrics, "reexecution_count") != 0 {
		t.Fatalf("groundhog fixed-block execution must not reexecute: %#v", result.ActualMetrics)
	}
	if len(result.ScheduleEvents) != len(txs) {
		t.Fatalf("expected one schedule event per transaction: %#v", result.ScheduleEvents)
	}
}

func groundhogTestProfile() map[string]PluginConfig {
	return map[string]PluginConfig{
		"workload":              {PluginID: "deterministic_signed_synthetic", Config: map[string]any{}},
		"transaction_admission": {PluginID: "signature_nonce_admission", Config: map[string]any{}},
		"txpool":                {PluginID: "fifo_per_node_mempool", Config: map[string]any{}},
		"sharding":              {PluginID: "deterministic_state_key_sharding", Config: map[string]any{}},
		"routing":               {PluginID: "hash_routing_baseline", Config: map[string]any{}},
		"block_producer":        {PluginID: groundhogBlockProducerID, Config: map[string]any{}},
		"consensus":             {PluginID: "pbft_style_consensus", Config: map[string]any{}},
		"network":               {PluginID: "localhost_tcp_typed_network", Config: map[string]any{}},
		"execution":             {PluginID: "serial_execution_baseline", Config: map[string]any{}},
		"scheduler":             {PluginID: "fifo_serial_scheduler", Config: map[string]any{}},
		"block_executor":        {PluginID: groundhogBlockExecutorID, Config: map[string]any{}},
		"state_access":          {PluginID: "direct_state_access", Config: map[string]any{}},
		"state_storage":         {PluginID: "persistent_local_state_store", Config: map[string]any{}},
		"cross_shard":           {PluginID: "relay_certificate_protocol", Config: map[string]any{}},
		"commit":                {PluginID: "normal_commit", Config: map[string]any{}},
		"fault_injection":       {PluginID: "faults_disabled", Config: map[string]any{}},
		"metrics":               {PluginID: "runtime_core_metrics", Config: map[string]any{}},
		"observability":         {PluginID: "node_network_consensus_observer", Config: map[string]any{}},
	}
}
