package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"metaverse-chainlab/executor/v5"
)

func TestProgressSnapshotChangesOnlyOnRealProgress(t *testing.T) {
	initial := progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}
	if progressChanged(initial, initial) {
		t.Fatal("identical runtime snapshot was reported as progress")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 3, MinHeight: 4, MaxHeight: 5, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("terminal progress was not detected")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 6, Mempool: 8, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("height progress was not detected")
	}
	if !progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 7, Reserved: 1, Pending: 3, ProposalInFlight: true}) {
		t.Fatal("mempool progress was not detected")
	}
	if progressChanged(initial, progressSnapshot{Terminal: 2, MinHeight: 4, MaxHeight: 5, Mempool: 9, Reserved: 2, Pending: 4, ProposalInFlight: false}) {
		t.Fatal("queue growth or proposal jitter was reported as progress")
	}
	raw, err := json.Marshal(initial)
	if err != nil || string(raw) == "{}" {
		t.Fatalf("progress snapshot did not serialize: %s", raw)
	}
}

func TestDrainBudgetScalesWithWorkloadAndBlockProducer(t *testing.T) {
	base := drainBudgetTestPlan(1000, 100, 75)
	larger := base
	larger.WorkloadPlan.TxCount = 10000
	if !(drainBudget(larger).HardTimeout > drainBudget(base).HardTimeout) {
		t.Fatal("drain budget did not grow with tx_count")
	}
	smallerBlocks := drainBudgetTestPlan(1000, 10, 75)
	smallerBlocks.NodeConfigs[0].PluginProfile["block_producer"] = blockProducerConfig(10, 75)
	if !(drainBudget(smallerBlocks).HardTimeout > drainBudget(base).HardTimeout) {
		t.Fatal("drain budget did not account for smaller block_size")
	}
	slowerInterval := drainBudgetTestPlan(1000, 100, 300)
	slowerInterval.NodeConfigs[0].PluginProfile["block_producer"] = blockProducerConfig(100, 300)
	if !(drainBudget(slowerInterval).HardTimeout > drainBudget(base).HardTimeout) {
		t.Fatal("drain budget did not account for slower block interval")
	}
	crossShard := drainBudgetTestPlan(1000, 100, 75)
	crossShard.WorkloadPlan.ExpectedCrossShardCount = 250
	if !(drainBudget(crossShard).HardTimeout > drainBudget(base).HardTimeout) {
		t.Fatal("drain budget did not account for cross-shard lifecycle work")
	}
	budget := drainBudget(larger)
	if budget.NoProgressTimeout <= 0 || budget.NoProgressTimeout >= budget.HardTimeout {
		t.Fatalf("invalid no-progress watchdog budget: %#v", budget)
	}
}

func TestDrainBudgetKeepsAbsoluteHardCap(t *testing.T) {
	plan := drainBudgetTestPlan(1_000_000, 1, 1000)
	plan.DurationMS = int((2 * time.Hour).Milliseconds())
	if got := drainBudget(plan).HardTimeout; got != 90*time.Minute {
		t.Fatalf("hard cap changed: %s", got)
	}
}

func TestShutdownBudgetScalesForLargeArtifactFlush(t *testing.T) {
	plan := drainBudgetTestPlan(10_000, 100, 75)
	for len(plan.NodeConfigs) < 8 {
		plan.NodeConfigs = append(plan.NodeConfigs, plan.NodeConfigs[0])
	}
	got := shutdownBudget(plan)
	if got <= 30*time.Second {
		t.Fatalf("shutdown budget did not grow for 10K run: %s", got)
	}
	if got > 10*time.Minute {
		t.Fatalf("shutdown budget exceeded hard cap: %s", got)
	}
	tiny := drainBudgetTestPlan(1, 100, 75)
	if got := shutdownBudget(tiny); got < 30*time.Second {
		t.Fatalf("tiny shutdown budget fell below minimum: %s", got)
	}
}

func TestRuntimePlanForNodesExtendsShortExperimentDuration(t *testing.T) {
	plan := drainBudgetTestPlan(10000, 100, 75)
	plan.DurationMS = 180000
	runtimePlan := runtimePlanForNodes(plan)
	if runtimePlan.DurationMS <= plan.DurationMS {
		t.Fatalf("node runtime duration was not extended: plan=%d runtime=%d", plan.DurationMS, runtimePlan.DurationMS)
	}
	if runtimePlan.WorkloadPlan.TxCount != plan.WorkloadPlan.TxCount {
		t.Fatal("node runtime plan changed workload semantics")
	}
}

func TestRemoteStateAggregateDeduplicatesByStableOperationKeys(t *testing.T) {
	root := t.TempDir()
	nodeA := filepath.Join(root, "nodes", "n0")
	nodeB := filepath.Join(root, "nodes", "n1")
	if err := os.MkdirAll(nodeA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeB, 0o755); err != nil {
		t.Fatal(err)
	}
	header := []string{"timestamp", "node_id", "execution_shard", "height", "block_hash", "tx_id", "state_key", "qualified_home_key", "home_shard", "response_execution_shard", "access_kind", "latency_ms", "witness_digest", "home_state_root", "success", "error", "delta_id", "source_height", "source_block_hash", "update_semantics"}
	rows := [][]string{
		{"1", "n0", "s1", "7", "fetch-block", "tx1", "balance:a", "s0::balance:a", "s0", "s1", "read", "3", "w1", "r", "true", "", "", "", "", ""},
		{"2", "n0", "s1", "7", "fetch-block", "tx1", "balance:a", "s0::balance:a", "s0", "s1", "read_write", "4", "w2", "r", "true", "", "", "", "", ""},
		{"3", "n0", "s1", "8", "apply-block", "tx2", "balance:b", "s0::balance:b", "s0", "s1", "write_apply:commutative_delta", "5", "w3", "r", "true", "", "delta-1", "8", "source-block", "commutative_delta"},
		{"4", "n0", "s1", "8", "apply-block", "tx3", "balance:c", "s0::balance:c", "s0", "s1", "future_kind", "6", "w4", "r", "true", "", "", "", "", ""},
		{"5", "n0", "s1", "9", "failed-block", "tx4", "balance:d", "s0::balance:d", "s0", "s1", "read", "7", "w5", "r", "false", "timeout", "", "", "", ""},
	}
	writeRows(filepath.Join(nodeA, "remote_state_access.csv"), header, rows)
	writeRows(filepath.Join(nodeB, "remote_state_access.csv"), header, rows)

	summary, err := writeRemoteStateAggregate(root, []v5.NodePlan{{NodeID: "n0", DataDir: nodeA}, {NodeID: "n1", DataDir: nodeB}}, 2)
	if err != nil {
		t.Fatal(err)
	}

	if summary["physical_remote_operation_count"] != 8 || summary["physical_remote_fetch_count"] != 4 || summary["physical_remote_writeback_count"] != 2 || summary["remote_operation_unknown_kind_count"] != 2 || summary["physical_remote_failed_count"] != 2 {
		t.Fatalf("unexpected physical summary: %#v", summary)
	}
	if summary["replica_deduplicated_remote_operation_count"] != 3 || summary["replica_deduplicated_remote_fetch_count"] != 1 || summary["replica_deduplicated_remote_writeback_count"] != 1 {
		t.Fatalf("unexpected deduplicated summary: %#v", summary)
	}
	if summary["replica_amplification_factor"] != float64(8)/float64(3) || summary["remote_fetches_per_logical_tx"] != 0.5 || summary["remote_writebacks_per_logical_tx"] != 0.5 {
		t.Fatalf("unexpected normalized summary: %#v", summary)
	}
	if _, err := os.Stat(filepath.Join(root, "aggregate", "remote_state_metrics_summary.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "physical_remote_state_operations.csv")); err != nil {
		t.Fatal(err)
	}
	if summary["physical_remote_operation_artifact"] != "physical_remote_state_operations.csv" {
		t.Fatalf("missing physical artifact pointer: %#v", summary)
	}
	if _, err := os.Stat(filepath.Join(root, "aggregate", "replica_deduplicated_remote_operations.csv")); err != nil {
		t.Fatal(err)
	}
}

func TestMechanismAggregatesWriteRootSummaries(t *testing.T) {
	root := t.TempDir()
	nodeA := filepath.Join(root, "nodes", "n0")
	nodeB := filepath.Join(root, "nodes", "n1")
	if err := os.MkdirAll(nodeA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeB, 0o755); err != nil {
		t.Fatal(err)
	}
	writeJSON(t, filepath.Join(nodeA, "block_stm_summary.json"), map[string]any{
		"serial_equivalent": true,
		"block_stm_metrics": map[string]any{
			"worker_count":                        4,
			"maximum_parallel_width":              3,
			"abort_count":                         2,
			"dependency_abort_count":              1,
			"validation_abort_count":              1,
			"reexecution_count":                   1,
			"validation_failure_count":            1,
			"dependency_wait_count":               5,
			"dependency_resume_count":             4,
			"estimate_count":                      7,
			"estimate_mark_count":                 7,
			"estimate_read_count":                 5,
			"business_execution_invocation_count": 11,
			"committed_transaction_count":         10,
		},
	})
	writeJSON(t, filepath.Join(nodeB, "block_stm_summary.json"), map[string]any{
		"serial_equivalent": true,
		"block_stm_metrics": map[string]any{
			"worker_count":                        4,
			"maximum_parallel_width":              2,
			"abort_count":                         3,
			"dependency_abort_count":              2,
			"validation_abort_count":              1,
			"reexecution_count":                   4,
			"validation_failure_count":            0,
			"dependency_wait_count":               6,
			"dependency_resume_count":             5,
			"estimate_count":                      8,
			"estimate_mark_count":                 8,
			"estimate_read_count":                 6,
			"business_execution_invocation_count": 14,
			"committed_transaction_count":         10,
		},
	})
	remoteState := map[string]any{
		"physical_remote_fetch_count":                 12,
		"physical_remote_writeback_count":             8,
		"replica_deduplicated_remote_fetch_count":     3,
		"replica_deduplicated_remote_writeback_count": 2,
	}

	summary, err := writeMechanismAggregates(root, []v5NodeSummary{
		{NodeID: "n0", ShardID: "s0", FastTrackCount: 6, ConservativeTrackCount: 4, AggregationGroupCount: 2, SchedulerEventCount: 10, SchedulerBlockedCount: 1, SchedulerWakeupCount: 1, PreAggregationPhysicalOps: 9, PostAggregationPhysicalOps: 7, AggregatedKeyCount: 1, AggregatedLogicalDeltaCount: 3, PhysicalOpsSavedCount: 2, RemoteStateAccessCount: 5},
		{NodeID: "n1", ShardID: "s0", FastTrackCount: 6, ConservativeTrackCount: 4, AggregationGroupCount: 2, SchedulerEventCount: 10, SchedulerBlockedCount: 1, SchedulerWakeupCount: 1, PreAggregationPhysicalOps: 9, PostAggregationPhysicalOps: 7, AggregatedKeyCount: 1, AggregatedLogicalDeltaCount: 3, PhysicalOpsSavedCount: 2, RemoteStateAccessCount: 5},
	}, []v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: nodeA}, {NodeID: "n1", ShardID: "s0", DataDir: nodeB}}, remoteState)
	if err != nil {
		t.Fatal(err)
	}

	metatrack := summary["metatrack"].(map[string]any)
	if metatrack["status"] != "available" || metatrack["fast_track_logical_tx_count"] != 6 || metatrack["conservative_track_logical_tx_count"] != 4 || metatrack["physical_replica_fast_track_instance_count"] != 12 || metatrack["physical_replica_conservative_track_instance_count"] != 8 || metatrack["physical_ops_saved_count"] != 4 {
		t.Fatalf("unexpected metatrack aggregate: %#v", metatrack)
	}
	blockSTM := summary["block_stm"].(map[string]any)
	if blockSTM["status"] != "available" || blockSTM["worker_count"] != 4 || blockSTM["worker_count_replica_consistent"] != true || blockSTM["maximum_parallel_width"] != 3 || blockSTM["abort_count"] != 5 || blockSTM["dependency_abort_count"] != 3 || blockSTM["validation_abort_count"] != 2 || blockSTM["abort_decomposition_consistent"] != true || blockSTM["reexecution_count"] != 5 || blockSTM["estimate_publish_count"] != 15 || blockSTM["committed_transaction_count_physical_replica_total"] != 20 || blockSTM["replica_deduplicated_committed_transaction_count"] != 10 {
		t.Fatalf("unexpected block-stm aggregate: %#v", blockSTM)
	}
	for _, name := range []string{"metatrack_aggregate_summary.json", "block_stm_aggregate_summary.json", "mechanism_metrics_summary.json"} {
		if _, err := os.Stat(filepath.Join(root, "aggregate", name)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestBlockProductionAggregateUsesCommittedChainEvidence(t *testing.T) {
	root := t.TempDir()
	nodeA := filepath.Join(root, "nodes", "n0")
	nodeB := filepath.Join(root, "nodes", "n1")
	if err := os.MkdirAll(nodeA, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(nodeB, 0o755); err != nil {
		t.Fatal(err)
	}
	header := []string{"node_id", "shard_id", "height", "view", "block_hash", "parent_hash", "tx_count", "tx_digest", "state_root_before", "state_root_after", "receipt_root", "commit_started_at", "commit_finished_at"}
	rows := [][]string{
		{"n0", "s0", "1", "0", "block-a", "genesis", "100", "txs-a", "r0", "r1", "rc1", "900", "1000"},
		{"n0", "s0", "2", "0", "block-b", "block-a", "25", "txs-b", "r1", "r2", "rc2", "1050", "1080"},
	}
	writeRows(filepath.Join(nodeA, "committed_chain.csv"), header, rows)
	writeRows(filepath.Join(nodeB, "committed_chain.csv"), header, rows)

	summary, err := writeBlockProductionAggregate(root, []v5.NodePlan{
		{NodeID: "n0", DataDir: nodeA, PluginProfile: map[string]v5.PluginConfig{"block_producer": blockProducerConfig(100, 75)}},
		{NodeID: "n1", DataDir: nodeB, PluginProfile: map[string]v5.PluginConfig{"block_producer": blockProducerConfig(100, 75)}},
	})
	if err != nil {
		t.Fatal(err)
	}

	if summary["configured_block_size"] != 100 || summary["configured_block_interval_ms"] != 75 {
		t.Fatalf("configured block producer values were not propagated: %#v", summary)
	}
	if summary["actual_committed_block_count"] != 2 || summary["actual_average_tx_per_block"] != 62.5 || summary["actual_min_tx_per_block"] != 25 || summary["actual_max_tx_per_block"] != 100 {
		t.Fatalf("unexpected block size evidence: %#v", summary)
	}
	if intFromAny(summary["actual_block_interval_mean_ms"]) != 80 || intFromAny(summary["actual_block_interval_p95_ms"]) != 80 {
		t.Fatalf("unexpected block interval evidence: %#v", summary)
	}
	if _, err := os.Stat(filepath.Join(root, "aggregate", "block_production_summary.json")); err != nil {
		t.Fatal(err)
	}
}

func drainBudgetTestPlan(txCount, blockSize, intervalMS int) v5.Plan {
	return v5.Plan{
		DurationMS: 0,
		WorkloadPlan: v5.WorkloadPlan{
			TxCount: txCount,
		},
		NodeConfigs: []v5.NodePlan{{
			PluginProfile: map[string]v5.PluginConfig{
				"block_producer": blockProducerConfig(blockSize, intervalMS),
			},
		}},
	}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	raw, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func blockProducerConfig(blockSize, intervalMS int) v5.PluginConfig {
	return v5.PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": blockSize, "interval_ms": intervalMS}}
}

func writeDrainStatus(t *testing.T, root string, finishedAt int64) {
	t.Helper()
	raw, err := json.Marshal(map[string]any{"completion_reason": "drain_quiescent", "drain_finished_at": finishedAt})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "drain_status.json"), append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

func writeRows(path string, header []string, rows [][]string) {
	file, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	writer := csv.NewWriter(file)
	_ = writer.Write(header)
	_ = writer.WriteAll(rows)
	writer.Flush()
	if err := writer.Error(); err != nil {
		panic(err)
	}
	if err := file.Close(); err != nil {
		panic(err)
	}
}

func TestFinalityDoesNotDrainBeforeSourceFinalize(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeRows := func(path string, header []string, rows [][]string) {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		_ = writer.Write(header)
		_ = writer.WriteAll(rows)
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	submissionRows := [][]string{}
	lifecycleRows := [][]string{}
	for i := 0; i < 1000; i++ {
		id := "tx-" + strconv.Itoa(i)
		cross := i >= 750
		crossText := "false"
		if cross {
			crossText = "true"
		}
		submissionRows = append(submissionRows, []string{strconv.Itoa(i + 1), id, "sender", "n0", "s0", "", crossText, "s0", "s1", "true", "1", ""})
		lifecycleRows = append(lifecycleRows, []string{strconv.Itoa(i + 1), id, id, "submitted", "n0", "s0", "", "", "1", "true"})
		stage := "durable_committed"
		if cross {
			lifecycleRows = append(lifecycleRows,
				[]string{strconv.Itoa(2000 + i), id, id, "sourcelock", "n0", "s0", "s0", "s1", "2", "true"},
				[]string{strconv.Itoa(3000 + i), id, id, "targetcommit", "n1", "s1", "s0", "s1", "3", "true"})
		} else {
			lifecycleRows = append(lifecycleRows, []string{strconv.Itoa(2000 + i), id, id, stage, "n0", "s0", "", "", "2", "true"})
		}
	}
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, submissionRows)
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, lifecycleRows)
	writeDrainStatus(t, root, 6000)

	assertSummary := func(wantTerminal, wantIncomplete int) {
		summary, err := deriveFinalityArtifacts(root, nil, false)
		if err != nil {
			t.Fatal(err)
		}
		if summary["terminal_unique_tx_count"] != wantTerminal || summary["incomplete_unique_tx_count"] != wantIncomplete {
			t.Fatalf("unexpected finality summary: %#v", summary)
		}
	}
	assertSummary(750, 250)

	file, err := os.OpenFile(filepath.Join(root, "client", "client_lifecycle.csv"), os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	for i := 750; i < 1000; i++ {
		id := "tx-" + strconv.Itoa(i)
		_ = writer.Write([]string{strconv.Itoa(4000 + i), id, id, "sourcefinalize", "n0", "s0", "s0", "s1", "4", "true"})
	}
	writer.Flush()
	_ = file.Close()
	assertSummary(1000, 0)
}

func TestDeriveLiveTerminalRequiresShardAgreementForFailure(t *testing.T) {
	classification := map[string]bool{"tx-1": false}
	statuses := []map[string]any{
		{"shard_id": "s0", "execution_failed_logical_tx_ids": []any{}, "admission_rejected_logical_tx_ids": []any{"tx-1"}},
		{"shard_id": "s0", "execution_failed_logical_tx_ids": []any{}, "admission_rejected_logical_tx_ids": []any{}},
	}
	terminal, counts, err := deriveLiveTerminalWithExpected(classification, statuses, false, map[string]int{"s0": 2})
	if err != nil {
		t.Fatal(err)
	}
	if terminal["tx-1"] || counts["terminal"] != 0 || counts["incomplete"] != 1 {
		t.Fatalf("single-replica failure became globally terminal: terminal=%v counts=%v", terminal, counts)
	}
	statuses[1]["admission_rejected_logical_tx_ids"] = []any{"tx-1"}
	terminal, counts, err = deriveLiveTerminalWithExpected(classification, statuses, false, map[string]int{"s0": 2})
	if err != nil {
		t.Fatal(err)
	}
	if !terminal["tx-1"] || counts["terminal"] != 1 || counts["incomplete"] != 0 {
		t.Fatalf("replica-agreed deterministic failure did not become terminal: terminal=%v counts=%v", terminal, counts)
	}
}

func TestDeriveLiveTerminalDurableCommitOverridesReplicaLocalFailure(t *testing.T) {
	classification := map[string]bool{"tx-1": false}
	statuses := []map[string]any{
		{"shard_id": "s0", "execution_failed_logical_tx_ids": []any{}, "admission_rejected_logical_tx_ids": []any{"tx-1"}},
		{"shard_id": "s0", "durable_committed_logical_tx_ids": []any{"tx-1"}, "execution_failed_logical_tx_ids": []any{}, "admission_rejected_logical_tx_ids": []any{}},
	}
	terminal, counts, err := deriveLiveTerminalWithExpected(classification, statuses, false, map[string]int{"s0": 2})
	if err != nil {
		t.Fatal(err)
	}
	if !terminal["tx-1"] || counts["terminal"] != 1 || counts["incomplete"] != 0 {
		t.Fatalf("durable commit did not authoritatively close transaction: terminal=%v counts=%v", terminal, counts)
	}
}

func TestDeriveLiveTerminalExecutionFailureIsAuthoritativeForIntraShard(t *testing.T) {
	classification := map[string]bool{"tx-1": false}
	statuses := []map[string]any{{
		"shard_id":                          "s0",
		"execution_failed_logical_tx_ids":   []any{"tx-1"},
		"admission_rejected_logical_tx_ids": []any{},
	}}
	terminal, counts, err := deriveLiveTerminalWithExpected(classification, statuses, false, map[string]int{"s0": 8})
	if err != nil {
		t.Fatal(err)
	}
	if !terminal["tx-1"] || counts["terminal"] != 1 || counts["incomplete"] != 0 {
		t.Fatalf("committed-block execution failure did not close intra-shard transaction: terminal=%v counts=%v", terminal, counts)
	}
}

func TestDeriveLiveTerminalUsesSubmittedClassification(t *testing.T) {
	classification := map[string]bool{"intra": false, "cross": true}
	statuses := []map[string]any{
		{"durable_committed_logical_tx_ids": []any{"intra", "cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"intra", "cross"}},
		{"durable_committed_logical_tx_ids": []any{"cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"cross"}},
	}
	terminal, counts, err := deriveLiveTerminal(classification, statuses, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 1 || !terminal["intra"] || counts["incomplete"] != 1 {
		t.Fatalf("cross-shard durable commit was incorrectly terminal: terminal=%v counts=%v", terminal, counts)
	}
	statuses[0]["source_finalized_logical_tx_ids"] = []any{"cross"}
	statuses[1]["source_finalized_logical_tx_ids"] = []any{"cross"}
	terminal, counts, err = deriveLiveTerminal(classification, statuses, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 2 || counts["incomplete"] != 0 {
		t.Fatalf("source finalize did not close cross-shard transaction: terminal=%v counts=%v", terminal, counts)
	}
}

func TestDeriveLiveTerminalHandlesTargetCommitAndDuplicates(t *testing.T) {
	classification := map[string]bool{}
	for i := 0; i < 750; i++ {
		classification[fmt.Sprintf("intra-%d", i)] = false
	}
	for i := 0; i < 250; i++ {
		classification[fmt.Sprintf("cross-%d", i)] = true
	}
	statuses := []map[string]any{{
		"durable_committed_logical_tx_ids": []any{},
		"source_finalized_logical_tx_ids":  []any{},
		"refunded_logical_tx_ids":          []any{},
		"failed_logical_tx_ids":            []any{},
		"target_commit_logical_tx_ids":     []any{},
	}}
	for i := 0; i < 750; i++ {
		statuses[0]["durable_committed_logical_tx_ids"] = append(statuses[0]["durable_committed_logical_tx_ids"].([]any), fmt.Sprintf("intra-%d", i))
	}
	for i := 0; i < 250; i++ {
		statuses[0]["durable_committed_logical_tx_ids"] = append(statuses[0]["durable_committed_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i), fmt.Sprintf("cross-%d", i))
		if i < 248 {
			statuses[0]["source_finalized_logical_tx_ids"] = append(statuses[0]["source_finalized_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i))
		}
	}
	terminal, counts, err := deriveLiveTerminal(classification, statuses, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 998 || counts["incomplete"] != 2 {
		t.Fatalf("expected 998 terminal and 2 incomplete: terminal=%d counts=%v", len(terminal), counts)
	}
	for i := 248; i < 250; i++ {
		statuses[0]["source_finalized_logical_tx_ids"] = append(statuses[0]["source_finalized_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i))
	}
	terminal, counts, err = deriveLiveTerminal(classification, statuses, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 1000 || counts["incomplete"] != 0 {
		t.Fatalf("expected all transactions terminal after finalize: terminal=%d counts=%v", len(terminal), counts)
	}
}

func TestSubmissionClassificationMustMatchExpectedCount(t *testing.T) {
	if err := validateSubmissionClassification(map[string]bool{"only": false}, 2); err == nil {
		t.Fatal("missing submitted classification was accepted")
	}
}

func TestHasNonTerminalMempoolIgnoresTerminalResidue(t *testing.T) {
	terminal := map[string]bool{"done": true}
	statuses := []map[string]any{
		{"mempool_depth": float64(1), "mempool_logical_tx_ids": []any{"done"}},
	}
	if hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("terminal mempool residue should not block drain")
	}
	statuses = []map[string]any{
		{"mempool_depth": float64(1), "mempool_logical_tx_ids": []any{"waiting"}},
	}
	if !hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("non-terminal mempool item should block drain")
	}
	statuses = []map[string]any{
		{"mempool_depth": float64(1)},
	}
	if !hasNonTerminalMempool(statuses, terminal) {
		t.Fatal("legacy status without mempool IDs should remain conservative")
	}
}

func TestHasPendingProposalWorkIgnoresTerminalOnlyResidue(t *testing.T) {
	terminal := map[string]bool{"done": true}
	statuses := []map[string]any{{
		"proposal_in_flight":                true,
		"proposal_work_details_available":   true,
		"proposal_logical_tx_ids":           []any{"done"},
		"proposal_system_state_delta_count": float64(0),
	}}
	if hasPendingProposalWork(statuses, terminal) {
		t.Fatal("terminal-only proposal residue should not extend drain timing")
	}

	statuses[0]["proposal_logical_tx_ids"] = []any{"waiting"}
	if !hasPendingProposalWork(statuses, terminal) {
		t.Fatal("proposal containing a non-terminal transaction must block drain")
	}

	statuses[0]["proposal_logical_tx_ids"] = []any{"done"}
	statuses[0]["proposal_system_state_delta_count"] = float64(1)
	if !hasPendingProposalWork(statuses, terminal) {
		t.Fatal("proposal containing system state deltas must block drain")
	}

	legacy := []map[string]any{{"proposal_in_flight": true}}
	if !hasPendingProposalWork(legacy, terminal) {
		t.Fatal("opaque legacy proposal status must remain conservative")
	}
}

func TestQuiescentCompletionTimeUsesRuntimeProgressInsteadOfObserverDelay(t *testing.T) {
	started := time.UnixMilli(1000)
	observed := time.UnixMilli(7000)
	statuses := []map[string]any{{"last_progress_at": float64(5000)}, {"last_progress_at": float64(4500)}}
	if got := quiescentCompletionTime(started, observed, statuses); got.UnixMilli() != 5000 {
		t.Fatalf("observer delay leaked into completion time: %d", got.UnixMilli())
	}

	statuses = []map[string]any{{"last_progress_at": float64(500)}}
	if got := quiescentCompletionTime(started, observed, statuses); got.UnixMilli() != started.UnixMilli() {
		t.Fatalf("completion time escaped drain start bound: %d", got.UnixMilli())
	}

	statuses = []map[string]any{{"last_progress_at": float64(9000)}}
	if got := quiescentCompletionTime(started, observed, statuses); got.UnixMilli() != observed.UnixMilli() {
		t.Fatalf("completion time escaped observation bound: %d", got.UnixMilli())
	}

	if got := quiescentCompletionTime(started, observed, nil); got.UnixMilli() != observed.UnixMilli() {
		t.Fatalf("missing progress timestamp should fall back to observation: %d", got.UnixMilli())
	}
}

func TestProgressSnapshotCountsPendingSystemDeltas(t *testing.T) {
	statuses := []map[string]any{
		{"committed_height": float64(1), "mempool_depth": float64(0), "reserved_tx_count": float64(0), "pending_commit_count": float64(0), "pending_future_block_count": float64(0), "pending_cross_shard_count": float64(0), "pending_state_delta_count": float64(1), "pending_state_delta_key_count": float64(1), "ready_state_delta_count": float64(1), "proposal_in_flight": false},
	}
	snapshot := makeProgressSnapshot(1, statuses, map[string]map[string]bool{"s0": map[string]bool{"1": true}})
	if snapshot.Pending != 3 {
		t.Fatalf("pending system deltas should block drain, pending=%d", snapshot.Pending)
	}
}

func TestFinalityCountsDurableCommitIndependentlyOfTerminalStageOrder(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	write := func(path string, header []string, rows [][]string) {
		file, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		writer := csv.NewWriter(file)
		_ = writer.Write(header)
		_ = writer.WriteAll(rows)
		writer.Flush()
		if err := writer.Error(); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	id := "intra-same-timestamp"
	write(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}
	write(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"100", id, id, "submitted", "n0", "s0", "", "", "1", "true"},
		{"200", id, id, "refund", "n0", "s0", "", "", "2", "true"},
		{"200", id, id, "durable_committed", "n0", "s0", "", "", "2", "true"},
	})
	writeDrainStatus(t, root, 200)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["intra_shard_committed_unique_count"] != 1 || summary["intra_shard_terminal_unique_count"] != 1 || summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 1 {
		t.Fatalf("same-timestamp stages caused metric drift: %#v", summary)
	}
}

func TestFinalityDurableCommitOverridesEarlierReplicaFailure(t *testing.T) {
	root := t.TempDir()
	client := filepath.Join(root, "client")
	n0 := filepath.Join(root, "nodes", "n0")
	n1 := filepath.Join(root, "nodes", "n1")
	for _, dir := range []string{client, n0, n1} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	id := "bsx-future-nonce-regression"
	writeRows(filepath.Join(client, "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	writeRows(filepath.Join(client, "client_lifecycle.csv"), header, [][]string{{"100", id, id, "submitted", "mbe-client", "s0", "", "", "0", "true", ""}})
	writeRows(filepath.Join(n0, "transaction_lifecycle.csv"), header, [][]string{{"500", id, id, "durable_committed", "n0", "s0", "", "", "10", "true", ""}})
	writeRows(filepath.Join(n1, "transaction_lifecycle.csv"), header, [][]string{
		{"120", id, id, "failed", "n1", "s0", "", "", "0", "false", "future_nonce_not_supported"},
		{"501", id, id, "durable_committed", "n1", "s0", "", "", "10", "true", ""},
	})
	writeDrainStatus(t, root, 500)
	summary, err := deriveFinalityArtifacts(root, []v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: n0}, {NodeID: "n1", ShardID: "s0", DataDir: n1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["intra_shard_committed_unique_count"] != 1 || summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 1 || summary["incomplete_unique_tx_count"] != 0 {
		t.Fatalf("authoritative durable commit accounting is wrong: %#v", summary)
	}
	file, err := os.Open(filepath.Join(root, "transaction_finality.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1][3] != "durable_committed" || rows[1][4] != "true" || rows[1][6] != "400" {
		t.Fatalf("earlier replica failure overrode durable commit: %#v", rows)
	}
}

func TestFinalityDurableCommitPreservesUnsuccessfulBusinessOutcome(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	id := "committed-unsuccessful"
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"100", id, id, "submitted", "mbe-client", "s0", "", "", "0", "true", ""},
		{"300", id, id, "durable_committed", "n0", "s0", "", "", "1", "false", "business_failure"},
	})
	writeDrainStatus(t, root, 300)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["intra_shard_committed_unique_count"] != 1 || summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 0 {
		t.Fatalf("authoritative commit incorrectly promoted unsuccessful business outcome: %#v", summary)
	}
}

func TestFinalityFailureRemainsTerminalWithoutAuthoritativeOutcome(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	id := "failed-only"
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"100", id, id, "submitted", "mbe-client", "s0", "", "", "0", "true", ""},
		{"200", id, id, "failed", "n0", "s0", "", "", "1", "false", "execution_failed"},
	})
	writeDrainStatus(t, root, 200)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 0 || summary["incomplete_unique_tx_count"] != 0 {
		t.Fatalf("failed-only lifecycle did not remain terminal failure: %#v", summary)
	}
}

func TestFinalityReplicaLocalAdmissionRejectionRequiresShardAgreement(t *testing.T) {
	root := t.TempDir()
	client := filepath.Join(root, "client")
	n0 := filepath.Join(root, "nodes", "n0")
	n1 := filepath.Join(root, "nodes", "n1")
	for _, dir := range []string{client, n0, n1} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	id := "admission-only"
	writeRows(filepath.Join(client, "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	writeRows(filepath.Join(client, "client_lifecycle.csv"), header, [][]string{{"100", id, id, "submitted", "mbe-client", "s0", "", "", "0", "true", ""}})
	writeRows(filepath.Join(n0, "transaction_lifecycle.csv"), header, [][]string{{"120", id, id, "failed", "n0", "s0", "", "", "0", "false", "future_nonce_not_supported"}})
	writeRows(filepath.Join(n1, "transaction_lifecycle.csv"), header, nil)
	writeDrainStatus(t, root, 200)
	summary, err := deriveFinalityArtifacts(root, []v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: n0}, {NodeID: "n1", ShardID: "s0", DataDir: n1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["terminal_unique_tx_count"] != 0 || summary["incomplete_unique_tx_count"] != 1 {
		t.Fatalf("single-replica admission rejection became final terminal: %#v", summary)
	}

	writeRows(filepath.Join(n1, "transaction_lifecycle.csv"), header, [][]string{{"121", id, id, "failed", "n1", "s0", "", "", "0", "false", "future_nonce_not_supported"}})
	summary, err = deriveFinalityArtifacts(root, []v5.NodePlan{{NodeID: "n0", ShardID: "s0", DataDir: n0}, {NodeID: "n1", ShardID: "s0", DataDir: n1}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 0 || summary["incomplete_unique_tx_count"] != 0 {
		t.Fatalf("shard-wide admission rejection did not become failure fallback: %#v", summary)
	}
}

func TestFinalityExecutionFailureOverridesEarlierAdmissionRejection(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	id := "admission-then-execution-failure"
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"100", id, "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success", "error"}
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"100", id, id, "submitted", "mbe-client", "s0", "", "", "0", "true", ""},
		{"120", id, id, "failed", "n1", "s0", "", "", "0", "false", "future_nonce_not_supported"},
		{"500", id, id, "failed", "n0", "s0", "", "", "9", "false", "execution_failed"},
	})
	writeDrainStatus(t, root, 500)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["terminal_unique_tx_count"] != 1 || summary["finalized_unique_logical_tx_count"] != 0 {
		t.Fatalf("execution failure terminal accounting is wrong: %#v", summary)
	}
	file, err := os.Open(filepath.Join(root, "transaction_finality.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[1][3] != "failed" || rows[1][4] != "false" || rows[1][6] != "400" {
		t.Fatalf("earlier admission rejection overrode committed-block execution failure: %#v", rows)
	}
}

func TestFinalityTimingUsesDrainInclusiveThroughput(t *testing.T) {
	timing := computeFinalityTiming(60, 1000, 5000, 7000, 2)
	if timing.LogicalFinalityDurationMS != 4000 || timing.LogicalFinalityTPS != 15 {
		t.Fatalf("bad logical timing: %#v", timing)
	}
	if timing.CompletionDurationMS != 6000 || timing.EndToEndTPS != 10 {
		t.Fatalf("bad completion timing: %#v", timing)
	}
	if timing.DrainDurationMS != 2000 || timing.TailCompletionOverheadMS != 2000 {
		t.Fatalf("bad drain timing: %#v", timing)
	}
}

func TestFinalityTimingNoDrainWorkKeepsLegacyAndCompletionTPSAligned(t *testing.T) {
	timing := computeFinalityTiming(60, 1000, 5000, 5000, 0)
	if timing.DrainDurationMS != 0 || timing.TailCompletionOverheadMS != 0 {
		t.Fatalf("no-drain timing should not add tail overhead: %#v", timing)
	}
	if timing.LogicalFinalityTPS != timing.EndToEndTPS {
		t.Fatalf("logical and completion TPS should match without drain: %#v", timing)
	}
}

func TestFinalityTimingKeepsTransactionLatencyIndependentFromDrain(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{
		{"1000", "tx-1", "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""},
		{"2000", "tx-2", "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""},
	})
	header := []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), header, [][]string{
		{"1000", "tx-1", "tx-1", "submitted", "n0", "s0", "", "", "1", "true"},
		{"2000", "tx-2", "tx-2", "submitted", "n0", "s0", "", "", "1", "true"},
		{"3000", "tx-1", "tx-1", "durable_committed", "n0", "s0", "", "", "2", "true"},
		{"4000", "tx-2", "tx-2", "durable_committed", "n0", "s0", "", "", "2", "true"},
	})
	writeDrainStatus(t, root, 7000)
	summary, err := deriveFinalityArtifacts(root, nil, false)
	if err != nil {
		t.Fatal(err)
	}
	if summary["p50_finality_ms"] != int64(2000) || summary["p95_finality_ms"] != int64(2000) || summary["p99_finality_ms"] != int64(2000) {
		t.Fatalf("transaction latency was extended by drain: %#v", summary)
	}
	if summary["completion_duration_ms"] != int64(6000) || summary["drain_duration_ms"] != int64(3000) {
		t.Fatalf("completion timing did not include drain: %#v", summary)
	}
}

func TestFinalityWindowCSVSeparatesLogicalAndCompletionRows(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "client"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeRows(filepath.Join(root, "client", "client_submission_log.csv"), []string{"timestamp", "tx_id", "sender", "ingress_node", "shard_id", "workload_path", "is_cross_shard", "source_shard", "target_shard", "submitted", "latency_ms", "error"}, [][]string{{"1000", "tx-1", "sender", "n0", "s0", "", "false", "s0", "", "true", "1", ""}})
	writeRows(filepath.Join(root, "client", "client_lifecycle.csv"), []string{"timestamp_ms", "tx_id", "logical_tx_id", "stage", "node_id", "shard_id", "source_shard", "target_shard", "block_height", "success"}, [][]string{
		{"1000", "tx-1", "tx-1", "submitted", "n0", "s0", "", "", "1", "true"},
		{"5000", "tx-1", "tx-1", "durable_committed", "n0", "s0", "", "", "2", "true"},
	})
	writeDrainStatus(t, root, 7000)
	if _, err := deriveFinalityArtifacts(root, nil, false); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(filepath.Join(root, "throughput_windows.csv"))
	if err != nil {
		t.Fatal(err)
	}
	rows, err := csv.NewReader(file).ReadAll()
	_ = file.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 || rows[1][0] != "logical_finality" || rows[2][0] != "end_to_end_completion" {
		t.Fatalf("throughput_windows.csv did not expose separate timing windows: %#v", rows)
	}
	if rows[1][3] != "1" || rows[2][3] != "1" {
		t.Fatalf("drain block changed logical TPS numerator: %#v", rows)
	}
}

func TestProgressChangedRecognizesBlockSTMInternalProgressWithinSameBlock(t *testing.T) {
	previous := progressSnapshot{BlockExecutionHeight: 3, BlockExecutionTaskCount: 10, BlockValidationTaskCount: 5}
	current := previous
	current.BlockExecutionTaskCount++
	if !progressChanged(previous, current) {
		t.Fatal("Block-STM execution task progress was ignored")
	}
	current = previous
	current.BlockValidationTaskCount++
	if !progressChanged(previous, current) {
		t.Fatal("Block-STM validation task progress was ignored")
	}
	current = previous
	current.BlockExecutionProgressAtMS++
	if progressChanged(previous, current) {
		t.Fatal("timestamp-only heartbeats must not hide an executor livelock")
	}
}

func TestProgressChangedRecognizesNextBlockCounterReset(t *testing.T) {
	previous := progressSnapshot{BlockExecutionHeight: 3, BlockExecutionTaskCount: 1000, BlockValidationTaskCount: 1000}
	current := progressSnapshot{BlockExecutionHeight: 4, BlockExecutionTaskCount: 1}
	if !progressChanged(previous, current) {
		t.Fatal("next Block-STM block was not recognized after counters reset")
	}
}

func TestDeriveLiveTerminalTreatsStatelessCrossShardCommitAsTerminal(t *testing.T) {
	classification := map[string]bool{"cross-1": true}
	statuses := []map[string]any{{"durable_committed_logical_tx_ids": []any{"cross-1"}}}
	legacyTerminal, _, err := deriveLiveTerminal(classification, statuses, false)
	if err != nil {
		t.Fatal(err)
	}
	if legacyTerminal["cross-1"] {
		t.Fatal("legacy cross-shard transaction became terminal before finalize")
	}
	statelessTerminal, _, err := deriveLiveTerminal(classification, statuses, true)
	if err != nil {
		t.Fatal(err)
	}
	if !statelessTerminal["cross-1"] {
		t.Fatal("stateless direct cross-shard transaction was not terminal after durable commit")
	}
}

func TestV5NodeSummaryRetainsStateReadyAndBusinessStateEvidence(t *testing.T) {
	raw := []byte(`{
		"node_id":"n0",
		"shard_id":"s0",
		"business_state_digest":"business-digest",
		"state_ready_wait_count":4,
		"state_ready_resume_count":3,
		"state_prefetch_wait_ms":17,
		"remote_state_fetch_count":6,
		"remote_state_fetch_completed_count":5,
		"state_ready_scheduler_mode":"transaction_level_suspend_resume",
		"versioned_state_ready_wave_count":8,
		"versioned_state_ready_wait_observation_count":7,
		"versioned_state_ready_resolved_token_count":9,
		"versioned_state_probe_count":11,
		"versioned_state_probe_latency_ms":23,
		"versioned_state_ready_max_wave_width":3,
		"versioned_state_ready_scheduler_mode":"per_transaction_per_key_version_frontier"
	}`)
	var summary v5NodeSummary
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.BusinessStateDigest != "business-digest" || summary.StateReadyWaitCount != 4 || summary.StateReadyResumeCount != 3 || summary.StatePrefetchWaitMS != 17 || summary.RemoteStateFetchCount != 6 || summary.RemoteStateFetchCompletedCount != 5 || summary.StateReadySchedulerMode != "transaction_level_suspend_resume" {
		t.Fatalf("native StateReady/business evidence was dropped: %#v", summary)
	}
	if summary.VersionedStateReadyWaveCount != 8 || summary.VersionedStateReadyWaitCount != 7 || summary.VersionedStateReadyResolvedCount != 9 || summary.VersionedStateProbeCount != 11 || summary.VersionedStateProbeLatencyMS != 23 || summary.VersionedStateReadyMaxWaveWidth != 3 || summary.VersionedStateReadySchedulerMode != "per_transaction_per_key_version_frontier" {
		t.Fatalf("version-frontier evidence was dropped: %#v", summary)
	}
}
