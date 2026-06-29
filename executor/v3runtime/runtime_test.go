package v3runtime

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestGateAMinimalRuntimeWritesV3Artifacts(t *testing.T) {
	temp := t.TempDir()
	out := filepath.Join(temp, "out")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "test_go_run",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.TxCount != 24 || result.Summary.SuccessCount != 24 || result.Summary.FailureCount != 0 {
		t.Fatalf("unexpected summary: %+v", result.Summary)
	}
	for _, name := range []string{"used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "runtime.log", "summary.csv", "summary.json", "report.md", "block_log.csv", "tx_results.csv", "state_commit_log.csv", "txpool_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	assertCSVFields(t, filepath.Join(out, "block_log.csv"), []string{"block_height", "block_id", "parent_hash", "block_hash", "proposer", "proposer_node", "tx_count", "cut_reason", "pool_size_before_cut", "pool_size_after_cut", "block_producer_plugin", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status", "consensus_domain_id", "validator_count", "execution_shard_count", "state_storage_unit_count"})
	assertCSVFields(t, filepath.Join(out, "tx_results.csv"), []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "consensus_domain_id", "execution_shard_id", "home_state_unit_ids", "accessed_state_unit_ids", "remote_state_unit_count", "remote_fetch_count", "cross_state_unit_access", "state_locality_hit", "read_count", "write_count"})
	assertCSVFields(t, filepath.Join(out, "state_commit_log.csv"), []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status", "state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"})
	assertCSVFields(t, filepath.Join(out, "txpool_log.csv"), []string{"event_time_ms", "event_type", "tx_id", "block_height", "pool_size_before", "pool_size_after", "admitted_count", "selected_count", "rejected_count", "queue_wait_ms", "reason"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"queue_wait_ms", "txpool_admitted_count", "txpool_rejected_count", "txpool_peak_size", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "empty_block_count", "avg_block_size", "max_block_size", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "blockproducer_empty_cut_count", "execution_shard_count", "state_storage_unit_count", "cross_state_unit_access_count", "remote_state_fetch_count", "state_locality_ratio", "execution_shard_load_balance", "state_unit_load_balance"})
	if result.Summary.QueueWaitMS <= 0 || result.Summary.TxPoolAvgWaitMS <= 0 {
		t.Fatalf("queue wait should be derived from txpool and non-zero in smoke profile: %+v", result.Summary)
	}
	if result.Summary.BlockProducerDrainCut != 1 || result.Summary.AvgBlockSize != 24 || result.Summary.MaxBlockSize != 24 {
		t.Fatalf("unexpected default block producer metrics: %+v", result.Summary)
	}
}

func TestGateAMinimalRuntimeIsDeterministicForCounts(t *testing.T) {
	first, err := Run(Input{ChainProfilePath: "../../configs/v3/chains/chain_x_default.yaml", PluginProfilePath: "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml", PluginProfileID: "v3_2_minimal_single_chain", ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml", OutputDir: filepath.Join(t.TempDir(), "first"), RunID: "first"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := Run(Input{ChainProfilePath: "../../configs/v3/chains/chain_x_default.yaml", PluginProfilePath: "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml", PluginProfileID: "v3_2_minimal_single_chain", ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml", OutputDir: filepath.Join(t.TempDir(), "second"), RunID: "second"})
	if err != nil {
		t.Fatal(err)
	}
	if first.Summary.TxCount != second.Summary.TxCount || first.Summary.BlockCount != second.Summary.BlockCount || first.Summary.AvgLatencyMS != second.Summary.AvgLatencyMS {
		t.Fatalf("non-deterministic summaries: %+v vs %+v", first.Summary, second.Summary)
	}
}

func TestRoleSeparatedChainProfileParsesRoles(t *testing.T) {
	bytes, err := os.ReadFile("../../configs/v3/chains/single_chain_research_default.yaml")
	if err != nil {
		t.Fatal(err)
	}
	chain := parseChainProfile(string(bytes))
	if chain.ConsensusDomainCount != 1 || chain.ExecutionShardCount != 4 || chain.StateStorageUnitCount != 4 {
		t.Fatalf("unexpected role counts: %+v", chain)
	}
	if chain.StatePlacementPolicy != "hash_state_storage" || chain.RoutingPlugin != "hash_sharding" {
		t.Fatalf("unexpected placement/routing: %+v", chain)
	}
}

func TestRoleSeparatedSmokeWritesStateAndExecutionFields(t *testing.T) {
	out := filepath.Join(t.TempDir(), "role")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/single_chain_research_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_role_separation_smoke.yaml",
		OutputDir:             out,
		RunID:                 "role_smoke",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.ExecutionShardCount != 4 || result.Summary.StateStorageUnitCount != 4 {
		t.Fatalf("unexpected summary role counts: %+v", result.Summary)
	}
	if len(result.TxResults) == 0 || len(result.TxResults[0].AccessedStateUnitIDs) == 0 {
		t.Fatalf("missing accessed state units: %+v", result.TxResults)
	}
	if result.TxResults[0].ShardID != result.TxResults[0].ExecutionShardID {
		t.Fatalf("legacy shard_id should alias execution_shard_id: %+v", result.TxResults[0])
	}
}

func TestCoAccessRoutingDoesNotChangeStatePlacement(t *testing.T) {
	baseline, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/single_chain_research_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/metatrack_plugin_profiles.yaml",
		PluginProfileID:       "baseline_hash_only",
		ExperimentProfilePath: "../../configs/v3/experiments/metatrack_go_backed_ablation_smoke.yaml",
		OutputDir:             filepath.Join(t.TempDir(), "baseline"),
		RunID:                 "baseline",
	})
	if err != nil {
		t.Fatal(err)
	}
	coAccess, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/single_chain_research_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/metatrack_plugin_profiles.yaml",
		PluginProfileID:       "co_access_only",
		ExperimentProfilePath: "../../configs/v3/experiments/metatrack_go_backed_ablation_smoke.yaml",
		OutputDir:             filepath.Join(t.TempDir(), "co"),
		RunID:                 "co",
	})
	if err != nil {
		t.Fatal(err)
	}
	basePlacement := placementByKey(baseline.StateCommitLog)
	coPlacement := placementByKey(coAccess.StateCommitLog)
	for key, unit := range basePlacement {
		if coPlacement[key] != unit {
			t.Fatalf("state placement changed for %s: baseline=%d co_access=%d", key, unit, coPlacement[key])
		}
	}
}

func TestTxPoolFIFOSelectionOrder(t *testing.T) {
	pool := newTxPool(ChainProfile{MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"})
	pool.Admit(testTx("tx_a", 0), 0)
	pool.Admit(testTx("tx_b", 1), 1)
	pool.Admit(testTx("tx_c", 2), 2)
	selected := pool.SelectForBlock(2, 10, 1)
	if len(selected) != 2 || selected[0].Tx.ID != "tx_a" || selected[1].Tx.ID != "tx_b" {
		t.Fatalf("expected FIFO tx_a, tx_b; got %+v", selected)
	}
	if selected[0].QueueWaitMS != 10 || selected[1].QueueWaitMS != 9 {
		t.Fatalf("unexpected queue waits: %+v", selected)
	}
}

func TestTxPoolDedupRejectsDuplicate(t *testing.T) {
	pool := newTxPool(ChainProfile{MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"})
	if !pool.Admit(testTx("tx_dup", 0), 0) {
		t.Fatal("first admit should succeed")
	}
	if pool.Admit(testTx("tx_dup", 1), 1) {
		t.Fatal("duplicate admit should be rejected")
	}
	if pool.rejectedCount != 1 || pool.events[len(pool.events)-1].Reason != "duplicate_tx" {
		t.Fatalf("unexpected duplicate rejection state: %+v", pool.events)
	}
}

func TestTxPoolMaxPoolSizeRejectsWhenFull(t *testing.T) {
	pool := newTxPool(ChainProfile{MaxPoolSize: 1, DedupEnabled: true, BackpressurePolicy: "reject"})
	if !pool.Admit(testTx("tx_1", 0), 0) {
		t.Fatal("first admit should succeed")
	}
	if pool.Admit(testTx("tx_2", 1), 1) {
		t.Fatal("full pool should reject")
	}
	if pool.rejectedCount != 1 || pool.peakSize != 1 || pool.events[len(pool.events)-1].Reason != "pool_full_reject" {
		t.Fatalf("unexpected full pool state: rejected=%d peak=%d events=%+v", pool.rejectedCount, pool.peakSize, pool.events)
	}
}

func TestBlockProducerSelectsFromTxPool(t *testing.T) {
	chain := ChainProfile{BlockIntervalMS: 10, MaxTxPerBlock: 2, MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"}
	pool := newTxPool(chain)
	producer := newBlockProducer(chain, PluginProfile{BlockProducer: "time_or_count_block_producer"})
	blocks := produceBlocksFromTxPool([]Transaction{
		testTx("tx_1", 0),
		testTx("tx_2", 1),
		testTx("tx_3", 2),
	}, chain, pool, producer)
	if len(blocks) != 2 {
		t.Fatalf("expected 2 blocks from pool selection, got %d", len(blocks))
	}
	if blocks[0].CutReason != "count" || blocks[1].CutReason != "drain" {
		t.Fatalf("unexpected cut reasons: %+v %+v", blocks[0], blocks[1])
	}
	if len(blocks[0].Txs) != 2 || blocks[0].Txs[0].Tx.ID != "tx_1" || blocks[0].Txs[1].Tx.ID != "tx_2" {
		t.Fatalf("unexpected first block txs: %+v", blocks[0].Txs)
	}
	selectCount := 0
	for _, event := range pool.events {
		if event.EventType == "select" && event.BlockHeight == 1 {
			selectCount++
		}
	}
	if selectCount != len(blocks[0].Txs) {
		t.Fatalf("block tx_count should match txpool select events: block=%d select=%d", len(blocks[0].Txs), selectCount)
	}
}

func TestBlockProducerTimeCut(t *testing.T) {
	chain := ChainProfile{BlockIntervalMS: 10, MaxTxPerBlock: 10, MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"}
	pool := newTxPool(chain)
	producer := newBlockProducer(chain, PluginProfile{BlockProducer: "time_or_count_block_producer"})
	blocks := produceBlocksFromTxPool([]Transaction{
		testTx("tx_1", 0),
		testTx("tx_2", 15),
	}, chain, pool, producer)
	if len(blocks) != 2 {
		t.Fatalf("expected time cut then drain cut, got %d blocks", len(blocks))
	}
	if blocks[0].CutReason != "time" || blocks[0].CutTimeMS != 10 {
		t.Fatalf("expected first block to be time cut at 10ms, got %+v", blocks[0])
	}
	if producer.TimeCutCount != 1 || producer.DrainCutCount != 1 {
		t.Fatalf("unexpected producer cut counts: %+v", producer)
	}
}

func TestBlockProducerDrainCut(t *testing.T) {
	chain := ChainProfile{BlockIntervalMS: 100, MaxTxPerBlock: 10, MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"}
	pool := newTxPool(chain)
	producer := newBlockProducer(chain, PluginProfile{BlockProducer: "time_or_count_block_producer"})
	blocks := produceBlocksFromTxPool([]Transaction{testTx("tx_1", 0), testTx("tx_2", 1)}, chain, pool, producer)
	if len(blocks) != 1 || blocks[0].CutReason != "drain" || len(blocks[0].Txs) != 2 {
		t.Fatalf("expected one drain block with two txs, got %+v", blocks)
	}
	if producer.DrainCutCount != 1 || producer.CountCutCount != 0 || producer.TimeCutCount != 0 {
		t.Fatalf("unexpected drain cut metrics: %+v", producer)
	}
}

func TestBlockProducerHashChainIsDeterministic(t *testing.T) {
	chain := ChainProfile{ProfileID: "test_chain", BlockIntervalMS: 10, MaxTxPerBlock: 1, MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"}
	firstPool := newTxPool(chain)
	firstProducer := newBlockProducer(chain, PluginProfile{BlockProducer: "time_or_count_block_producer"})
	first := produceBlocksFromTxPool([]Transaction{testTx("tx_1", 0), testTx("tx_2", 1)}, chain, firstPool, firstProducer)
	secondPool := newTxPool(chain)
	secondProducer := newBlockProducer(chain, PluginProfile{BlockProducer: "time_or_count_block_producer"})
	second := produceBlocksFromTxPool([]Transaction{testTx("tx_1", 0), testTx("tx_2", 1)}, chain, secondPool, secondProducer)
	if len(first) != 2 || len(second) != 2 {
		t.Fatalf("expected two blocks in deterministic hash test")
	}
	if first[0].ParentHash != genesisParentHash("test_chain") {
		t.Fatalf("unexpected genesis parent hash: %s", first[0].ParentHash)
	}
	if first[1].ParentHash != first[0].Hash {
		t.Fatalf("parent hash should chain to previous block hash: %+v", first)
	}
	if first[0].Hash != second[0].Hash || first[1].Hash != second[1].Hash {
		t.Fatalf("block hashes should be deterministic: %+v vs %+v", first, second)
	}
}

func TestBlockLogAlignsWithTxPoolLog(t *testing.T) {
	out := filepath.Join(t.TempDir(), "align")
	_, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "align",
	})
	if err != nil {
		t.Fatal(err)
	}
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	txPoolRows := readCSVRows(t, filepath.Join(out, "txpool_log.csv"))
	selectedByBlock := map[string]int{}
	for _, row := range txPoolRows {
		if row["event_type"] == "select" {
			selectedByBlock[row["block_height"]]++
		}
	}
	for _, row := range blockRows {
		if selectedByBlock[row["block_height"]] != atoi(t, row["tx_count"]) {
			t.Fatalf("block %s tx_count=%s does not match select events=%d", row["block_height"], row["tx_count"], selectedByBlock[row["block_height"]])
		}
	}
}

func TestTxPoolMetricsAllowZeroAndNonZeroWaits(t *testing.T) {
	zeroPool := newTxPool(ChainProfile{MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"})
	zeroPool.Admit(testTx("tx_zero", 5), 5)
	zeroPool.SelectForBlock(1, 5, 1)
	zeroSummary := Summary{}
	applyTxPoolMetrics(&zeroSummary, zeroPool)
	if zeroSummary.QueueWaitMS != 0 {
		t.Fatalf("same-time selection should allow zero wait: %+v", zeroSummary)
	}

	waitPool := newTxPool(ChainProfile{MaxPoolSize: 10, DedupEnabled: true, BackpressurePolicy: "reject"})
	waitPool.Admit(testTx("tx_wait", 0), 0)
	waitPool.SelectForBlock(1, 25, 1)
	waitSummary := Summary{}
	applyTxPoolMetrics(&waitSummary, waitPool)
	if waitSummary.QueueWaitMS != 25 || waitSummary.TxPoolP95WaitMS != 25 {
		t.Fatalf("non-zero queue wait should come from txpool wait stats: %+v", waitSummary)
	}
}

func assertCSVFields(t *testing.T, path string, fields []string) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	header, err := csv.NewReader(file).Read()
	if err != nil {
		t.Fatal(err)
	}
	present := map[string]bool{}
	for _, field := range header {
		present[field] = true
	}
	for _, field := range fields {
		if !present[field] {
			t.Fatalf("%s missing field %s", path, field)
		}
	}
}

func readCSVRows(t *testing.T, path string) []map[string]string {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) == 0 {
		return nil
	}
	result := []map[string]string{}
	header := rows[0]
	for _, row := range rows[1:] {
		item := map[string]string{}
		for index, field := range header {
			if index < len(row) {
				item[field] = row[index]
			}
		}
		result = append(result, item)
	}
	return result
}

func atoi(t *testing.T, value string) int {
	t.Helper()
	parsed, err := strconv.Atoi(value)
	if err != nil {
		t.Fatalf("invalid integer %q: %v", value, err)
	}
	return parsed
}

func placementByKey(commits []StateCommit) map[string]int {
	result := map[string]int{}
	for _, commit := range commits {
		result[commit.StateKey] = commit.StateStorageUnitID
	}
	return result
}

func testTx(id string, submitTimeMS int) Transaction {
	return Transaction{
		ID:           id,
		SubmitTimeMS: submitTimeMS,
		ReadKeys:     []string{"asset_1"},
		WriteDeltas:  map[string]int{"asset_1": 1},
		Commutative:  true,
		ConflictHint: "low",
	}
}
