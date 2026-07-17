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
	if got := drainBudget(plan).HardTimeout; got != 45*time.Minute {
		t.Fatalf("hard cap changed: %s", got)
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

func blockProducerConfig(blockSize, intervalMS int) v5.PluginConfig {
	return v5.PluginConfig{PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": blockSize, "interval_ms": intervalMS}}
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

	assertSummary := func(wantTerminal, wantIncomplete int) {
		summary, err := deriveFinalityArtifacts(root, nil)
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

func TestDeriveLiveTerminalUsesSubmittedClassification(t *testing.T) {
	classification := map[string]bool{"intra": false, "cross": true}
	statuses := []map[string]any{
		{"durable_committed_logical_tx_ids": []any{"intra", "cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"intra", "cross"}},
		{"durable_committed_logical_tx_ids": []any{"cross"}, "source_finalized_logical_tx_ids": []any{}, "refunded_logical_tx_ids": []any{}, "failed_logical_tx_ids": []any{}, "terminal_logical_tx_ids": []any{"cross"}},
	}
	terminal, counts, err := deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 1 || !terminal["intra"] || counts["incomplete"] != 1 {
		t.Fatalf("cross-shard durable commit was incorrectly terminal: terminal=%v counts=%v", terminal, counts)
	}
	statuses[0]["source_finalized_logical_tx_ids"] = []any{"cross"}
	statuses[1]["source_finalized_logical_tx_ids"] = []any{"cross"}
	terminal, counts, err = deriveLiveTerminal(classification, statuses)
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
	terminal, counts, err := deriveLiveTerminal(classification, statuses)
	if err != nil {
		t.Fatal(err)
	}
	if len(terminal) != 998 || counts["incomplete"] != 2 {
		t.Fatalf("expected 998 terminal and 2 incomplete: terminal=%d counts=%v", len(terminal), counts)
	}
	for i := 248; i < 250; i++ {
		statuses[0]["source_finalized_logical_tx_ids"] = append(statuses[0]["source_finalized_logical_tx_ids"].([]any), fmt.Sprintf("cross-%d", i))
	}
	terminal, counts, err = deriveLiveTerminal(classification, statuses)
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
	summary, err := deriveFinalityArtifacts(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	if summary["intra_shard_committed_unique_count"] != 1 || summary["intra_shard_terminal_unique_count"] != 1 || summary["terminal_unique_tx_count"] != 1 {
		t.Fatalf("same-timestamp stages caused metric drift: %#v", summary)
	}
}
