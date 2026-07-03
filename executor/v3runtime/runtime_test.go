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
	for _, name := range []string{"used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "runtime.log", "summary.csv", "summary.json", "report.md", "block_log.csv", "tx_results.csv", "state_commit_log.csv", "txpool_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv", "state_access_log.csv", "consensus_network_light_log.csv", "network_consensus_summary.json", "pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv", "consensus_network_log.csv", "pbft_network_summary.json", "state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json", "benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	assertCSVFields(t, filepath.Join(out, "block_log.csv"), []string{"block_height", "block_id", "parent_hash", "block_hash", "proposer", "proposer_node", "tx_count", "cut_reason", "pool_size_before_cut", "pool_size_after_cut", "block_producer_plugin", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status", "consensus_domain_id", "validator_count", "execution_shard_count", "state_storage_unit_count"})
	assertCSVFields(t, filepath.Join(out, "tx_results.csv"), []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "consensus_domain_id", "execution_shard_id", "home_state_unit_ids", "accessed_state_unit_ids", "remote_state_unit_count", "remote_fetch_count", "cross_state_unit_access", "state_locality_hit", "read_count", "write_count"})
	assertCSVFields(t, filepath.Join(out, "state_commit_log.csv"), []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status", "state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"})
	assertCSVFields(t, filepath.Join(out, "txpool_log.csv"), []string{"event_time_ms", "event_type", "tx_id", "block_height", "pool_size_before", "pool_size_after", "admitted_count", "selected_count", "rejected_count", "queue_wait_ms", "reason"})
	assertCSVFields(t, filepath.Join(out, "consensus_log.csv"), consensusLogFields())
	assertCSVFields(t, filepath.Join(out, "routing_log.csv"), routingLogFields())
	assertCSVFields(t, filepath.Join(out, "execution_log.csv"), executionLogFields())
	assertCSVFields(t, filepath.Join(out, "state_access_log.csv"), stateAccessLogFields())
	assertCSVFields(t, filepath.Join(out, "consensus_network_light_log.csv"), []string{"event_id", "time_ms", "network_adapter", "consensus_network_path", "consensus_runtime", "consensus_domain_id", "shard_id", "leader_node_id", "validator_node_id", "message_type", "block_height", "sequence_id", "quorum_target", "vote_count", "light_quorum_reached", "status", "details"})
	assertCSVFields(t, filepath.Join(out, "pbft_state_log.csv"), []string{"event_id", "time_ms", "consensus_runtime", "shard_id", "node_id", "leader_node_id", "view", "sequence_id", "block_height", "request_pool_size", "prepare_confirm_count", "commit_confirm_count", "pbft_stage", "quorum_threshold", "quorum_reached", "status", "details"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"queue_wait_ms", "txpool_admitted_count", "txpool_rejected_count", "txpool_peak_size", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "empty_block_count", "avg_block_size", "max_block_size", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "blockproducer_empty_cut_count", "consensus_latency_ms", "avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "view_change_count", "finalized_block_count", "failed_block_count", "routing_plugin", "routing_decision_count", "cross_shard_tx_count", "cross_shard_ratio", "remote_state_access_count", "avg_touched_shards", "hotspot_key_count", "coaccess_group_count", "avg_routing_overhead_ms", "execution_plugin", "execution_tx_count", "fast_track_count", "conservative_track_count", "blocked_tx_count", "dependency_edge_count", "avg_dependency_edges_per_tx", "avg_execution_latency_ms", "p95_execution_latency_ms", "parallelizable_tx_count", "serial_tx_count", "state_access_plugin", "state_access_count", "local_state_access_count", "remote_state_access_ratio", "cache_hit_count", "cache_miss_count", "cache_hit_rate", "prefetch_hit_count", "prefetch_miss_count", "prefetch_hit_rate", "avg_state_access_latency_ms", "p95_state_access_latency_ms", "witness_estimated_count", "proof_estimated_count", "estimated_witness_bytes", "estimated_proof_bytes", "execution_shard_count", "state_storage_unit_count", "cross_state_unit_access_count", "remote_state_fetch_count", "state_locality_ratio", "execution_shard_load_balance", "state_unit_load_balance"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"consensus_over_network_enabled", "consensus_runtime_selected", "proposal_preview_count", "vote_preview_count", "light_quorum_reached_count", "consensus_network_error_count", "consensus_network_path"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"pbft_view", "pbft_sequence", "pbft_preprepare_count", "pbft_prepare_count", "pbft_commit_count", "pbft_quorum_reached_count", "pbft_finalized_block_count", "pbft_consensus_latency_ms", "pbft_preview_enabled", "pbft_quorum_threshold"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"pbft_over_network_enabled", "pbft_network_path", "pbft_network_message_count", "pbft_network_error_count", "pbft_preprepare_network_count", "pbft_prepare_network_count", "pbft_commit_network_count", "pbft_finalized_network_count", "pbft_network_quorum_reached_count"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"state_backend_selected", "persistent_state_enabled", "state_root_enabled", "state_root_count", "state_key_count", "state_update_count", "state_proof_generated_count", "state_proof_verified_count", "state_proof_failed_count", "witness_generated_count", "witness_verified_count", "witness_failed_count", "state_authenticity_error_count"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"benchmark_template_selected", "baseline_profile_selected", "benchmark_run_count", "sweep_parameter_count", "repeat_count", "benchmark_artifact_count", "baseline_comparison_count", "reproducibility_manifest_available", "benchmark_report_available", "paper_grade_benchmark"})
	assertCSVFields(t, filepath.Join(out, "benchmark_run_index.csv"), []string{"benchmark_id", "run_id", "template_id", "baseline_id", "repeat_index", "seed", "tx_count", "shard_count", "hotspot_ratio", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend", "summary_path", "status", "runtime_truth"})
	assertCSVFields(t, filepath.Join(out, "baseline_comparison.csv"), []string{"template_id", "baseline_id", "comparison_target", "metric_name", "baseline_value", "target_value", "delta", "delta_ratio", "interpretation", "truth_boundary"})
	if result.Summary.QueueWaitMS <= 0 || result.Summary.TxPoolAvgWaitMS <= 0 {
		t.Fatalf("queue wait should be derived from txpool and non-zero in smoke profile: %+v", result.Summary)
	}
	if result.Summary.BlockProducerDrainCut != 1 || result.Summary.AvgBlockSize != 24 || result.Summary.MaxBlockSize != 24 {
		t.Fatalf("unexpected default block producer metrics: %+v", result.Summary)
	}
	if result.Summary.ConsensusRoundCount != result.Summary.BlockCount || result.Summary.FinalizedBlockCount != result.Summary.BlockCount || result.Summary.FailedBlockCount != 0 {
		t.Fatalf("unexpected simple leader consensus summary: %+v", result.Summary)
	}
	if !result.Summary.ConsensusOverNetworkEnabled || result.Summary.ProposalPreviewCount == 0 || result.Summary.VotePreviewCount == 0 || result.Summary.LightQuorumReachedCount == 0 {
		t.Fatalf("missing consensus-light over network metrics: %+v", result.Summary)
	}
	if result.Summary.StateRootCount == 0 || result.Summary.StateProofVerifiedCount == 0 || result.Summary.WitnessVerifiedCount == 0 {
		t.Fatalf("missing state authenticity metrics: %+v", result.Summary)
	}
	if result.Summary.BenchmarkRunCount == 0 || !result.Summary.ReproducibilityManifestAvailable || !result.Summary.BenchmarkReportAvailable || result.Summary.PaperGradeBenchmark {
		t.Fatalf("missing V3.10 benchmark hardening metrics: %+v", result.Summary)
	}
}

func TestBenchmarkHardeningPreviewWritesArtifactsAndMetrics(t *testing.T) {
	summary := Summary{
		TxCount:                    24,
		ThroughputTPS:              12,
		P95LatencyMS:               7,
		ShardCount:                 4,
		NetworkAdapterSelected:     "in_memory_message_bus",
		ConsensusRuntimeSelected:   "simple_leader",
		CrossShardProtocolSelected: "relay_preview",
		StateBackendSelected:       "merkle_trie_mvp",
		StateRootCount:             4,
		CrossShardTxCount:          2,
		TypedMessageCount:          3,
	}
	experiment := ExperimentProfile{
		ProfileID:         "benchmark_test",
		BenchmarkTemplate: "state_authenticity_template",
		BaselineProfile:   "baseline_memory_kv",
		RepeatCount:       2,
		Seed:              41,
	}
	preview := RunBenchmarkHardeningPreview(experiment, summary)
	if len(preview.Templates) < 5 || len(preview.Baselines) < 6 || preview.BenchmarkRunCount != 2 || preview.PaperGradeBenchmark {
		t.Fatalf("unexpected benchmark preview: %+v", preview)
	}
	ApplyBenchmarkMetrics(&summary, preview)
	if summary.BenchmarkTemplateSelected != "state_authenticity_template" || summary.BaselineProfileSelected != "baseline_memory_kv" || !summary.BenchmarkReportAvailable {
		t.Fatalf("benchmark metrics not applied: %+v", summary)
	}
	out := t.TempDir()
	if err := WriteBenchmarkArtifacts(out, preview, experiment, summary); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing benchmark artifact %s: %v", name, err)
		}
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

func TestPoALightConsensusFinalizesBlocks(t *testing.T) {
	engine := newConsensusEngine(ChainProfile{ValidatorCount: 4, NodeIDPrefix: "auth", ConsensusBaseLatencyMS: 2}, PluginProfile{ConsensusPlugin: "poa_light"})
	record := engine.FinalizeBlock(Block{Height: 7, Hash: "block_hash", CutTimeMS: 100})
	if record.PluginID != "poa_light" || !record.Finalized || record.Reason != "authority_confirmed" {
		t.Fatalf("unexpected poa record: %+v", record)
	}
	if record.ValidatorCount != 4 || record.TotalMessageCount != 5 || record.ConsensusLatencyMS != 3 || record.ViewChangeCount != 0 {
		t.Fatalf("unexpected poa metrics: %+v", record)
	}
}

func TestPBFTLightConsensusQuorumAndMessages(t *testing.T) {
	engine := newConsensusEngine(ChainProfile{ValidatorCount: 4, NodeIDPrefix: "v", ConsensusBaseLatencyMS: 1}, PluginProfile{ConsensusPlugin: "pbft_light_model"})
	record := engine.FinalizeBlock(Block{Height: 3, Hash: "block_hash", CutTimeMS: 50})
	if record.PluginID != "pbft_light_model" || !record.Finalized || record.Reason != "pbft_light_quorum_reached" {
		t.Fatalf("unexpected pbft-light record: %+v", record)
	}
	if record.FaultToleranceF != 1 || record.PrepareQuorum != 3 || record.CommitQuorum != 3 {
		t.Fatalf("unexpected pbft-light quorum: %+v", record)
	}
	if record.PrePrepareMsgCount != 3 || record.PrepareMsgCount != 12 || record.CommitMsgCount != 12 || record.TotalMessageCount != 27 {
		t.Fatalf("unexpected pbft-light message counts: %+v", record)
	}
	if record.SequenceID != 3 || record.ViewChangeCount != 0 || record.ConsensusLatencyMS != 3 {
		t.Fatalf("unexpected pbft-light timing: %+v", record)
	}
}

func TestConsensusLogAlignsWithBlockLog(t *testing.T) {
	out := filepath.Join(t.TempDir(), "pbft")
	pluginPath := writeConsensusPluginProfile(t, t.TempDir(), "pbft_light_model")
	_, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "test_consensus",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "pbft_light",
	})
	if err != nil {
		t.Fatal(err)
	}
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	consensusRows := readCSVRows(t, filepath.Join(out, "consensus_log.csv"))
	if len(blockRows) != len(consensusRows) {
		t.Fatalf("block and consensus rows should align: block=%d consensus=%d", len(blockRows), len(consensusRows))
	}
	for index, block := range blockRows {
		consensus := consensusRows[index]
		if block["block_height"] != consensus["block_height"] || block["block_hash"] != consensus["block_hash"] {
			t.Fatalf("block/consensus mismatch: block=%+v consensus=%+v", block, consensus)
		}
		if block["consensus_plugin"] != "pbft_light_model" || consensus["consensus_plugin"] != "pbft_light_model" {
			t.Fatalf("expected pbft_light_model in logs: block=%+v consensus=%+v", block, consensus)
		}
	}
	assertCSVFields(t, filepath.Join(out, "consensus_log.csv"), consensusLogFields())
}

func TestHashShardingRoutingLogAndSummary(t *testing.T) {
	out := filepath.Join(t.TempDir(), "routing")
	_, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "routing_hash",
	})
	if err != nil {
		t.Fatal(err)
	}
	rows := readCSVRows(t, filepath.Join(out, "routing_log.csv"))
	if len(rows) == 0 {
		t.Fatal("expected routing rows")
	}
	assertCSVFields(t, filepath.Join(out, "routing_log.csv"), routingLogFields())
	if rows[0]["routing_plugin"] != "hash_sharding" {
		t.Fatalf("expected hash_sharding routing row: %+v", rows[0])
	}
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	txRows := readCSVRows(t, filepath.Join(out, "tx_results.csv"))
	if rows[0]["block_height"] != txRows[0]["block_height"] || rows[0]["tx_id"] != txRows[0]["tx_id"] {
		t.Fatalf("routing log should align with tx_results: routing=%+v tx=%+v", rows[0], txRows[0])
	}
	if blockRows[0]["block_height"] != rows[0]["block_height"] {
		t.Fatalf("routing log should align with block_log: routing=%+v block=%+v", rows[0], blockRows[0])
	}
	if result, err := Run(Input{ChainProfilePath: "../../configs/v3/chains/chain_x_default.yaml", PluginProfilePath: "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml", PluginProfileID: "v3_2_minimal_single_chain", ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml", OutputDir: filepath.Join(t.TempDir(), "summary"), RunID: "routing_summary"}); err != nil {
		t.Fatal(err)
	} else if result.Summary.RoutingDecisionCount != result.Summary.TxCount || result.Summary.RoutingPlugin != "hash_sharding" {
		t.Fatalf("unexpected routing summary: %+v", result.Summary)
	}
}

func TestCoaccessRoutingProducesGroupsDeterministically(t *testing.T) {
	chain := ChainProfile{ExecutionShardCount: 4, HotspotThreshold: 2, CoaccessWindow: 1}
	block := Block{Height: 1, Txs: []PooledTransaction{
		{Tx: testRoutingTx("tx_a", []string{"asset_1", "asset_2"}, map[string]int{"asset_1": 1})},
		{Tx: testRoutingTx("tx_b", []string{"asset_1", "asset_2"}, map[string]int{"asset_2": 1})},
	}}
	first := newRoutingEngine(chain, PluginProfile{ShardingPlugin: "metatrack_coaccess_routing"})
	second := newRoutingEngine(chain, PluginProfile{ShardingPlugin: "metatrack_coaccess_routing"})
	firstRecords := first.RouteBlock(block)
	secondRecords := second.RouteBlock(block)
	if first.CoaccessGroupCount == 0 || firstRecords[0].CoaccessGroupID == "" {
		t.Fatalf("expected coaccess group records: engine=%+v records=%+v", first, firstRecords)
	}
	if firstRecords[0].CoaccessGroupID != secondRecords[0].CoaccessGroupID || firstRecords[0].PrimaryShard != secondRecords[0].PrimaryShard {
		t.Fatalf("coaccess routing should be deterministic: %+v vs %+v", firstRecords, secondRecords)
	}
}

func TestCoaccessRoutingRunsThroughRuntime(t *testing.T) {
	out := filepath.Join(t.TempDir(), "coaccess_runtime")
	pluginPath := writeRoutingPluginProfile(t, t.TempDir(), "metatrack_coaccess_routing")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "test_routing",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "coaccess_runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.RoutingPlugin != "metatrack_coaccess_routing" || result.Summary.RoutingDecisionCount != result.Summary.TxCount {
		t.Fatalf("unexpected coaccess runtime summary: %+v", result.Summary)
	}
	rows := readCSVRows(t, filepath.Join(out, "routing_log.csv"))
	if len(rows) == 0 || rows[0]["routing_plugin"] != "metatrack_coaccess_routing" {
		t.Fatalf("expected coaccess routing log rows: %+v", rows)
	}
}

func TestHotspotAwareRoutingIdentifiesHotspotsDeterministically(t *testing.T) {
	chain := ChainProfile{ExecutionShardCount: 4, HotspotThreshold: 2}
	block := Block{Height: 1, Txs: []PooledTransaction{
		{Tx: testRoutingTx("tx_a", []string{"hot"}, map[string]int{"asset_1": 1})},
		{Tx: testRoutingTx("tx_b", []string{"hot"}, map[string]int{"asset_2": 1})},
	}}
	first := newRoutingEngine(chain, PluginProfile{ShardingPlugin: "hotspot_aware_routing"})
	second := newRoutingEngine(chain, PluginProfile{ShardingPlugin: "hotspot_aware_routing"})
	firstRecords := first.RouteBlock(block)
	secondRecords := second.RouteBlock(block)
	if first.HotspotKeyCount == 0 || firstRecords[0].HotspotKeyCount == 0 {
		t.Fatalf("expected hotspot key counts: engine=%+v records=%+v", first, firstRecords)
	}
	if firstRecords[0].PrimaryShard != secondRecords[0].PrimaryShard {
		t.Fatalf("hotspot routing should be deterministic: %+v vs %+v", firstRecords, secondRecords)
	}
}

func TestSerialExecutionLogAndSummary(t *testing.T) {
	out := filepath.Join(t.TempDir(), "execution_serial")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "execution_serial",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCSVFields(t, filepath.Join(out, "execution_log.csv"), executionLogFields())
	rows := readCSVRows(t, filepath.Join(out, "execution_log.csv"))
	txRows := readCSVRows(t, filepath.Join(out, "tx_results.csv"))
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	if len(rows) == 0 || rows[0]["execution_plugin"] != "serial_execution" || rows[0]["track"] != "serial" {
		t.Fatalf("expected serial execution rows: %+v", rows)
	}
	if rows[0]["tx_id"] != txRows[0]["tx_id"] || rows[0]["block_height"] != txRows[0]["block_height"] {
		t.Fatalf("execution log should align with tx_results: execution=%+v tx=%+v", rows[0], txRows[0])
	}
	if rows[0]["block_height"] != blockRows[0]["block_height"] {
		t.Fatalf("execution log should align with block_log: execution=%+v block=%+v", rows[0], blockRows[0])
	}
	if result.Summary.ExecutionPlugin != "serial_execution" || result.Summary.ExecutionTxCount != result.Summary.TxCount || result.Summary.SerialTxCount != result.Summary.TxCount {
		t.Fatalf("unexpected serial execution summary: %+v", result.Summary)
	}
}

func TestParallelLightExecutionProducesWorkersAndDependencies(t *testing.T) {
	engine := newExecutionEngine(ChainProfile{ExecutionShardCount: 4}, PluginProfile{ExecutionSchedulerPlugin: "parallel_light_execution"})
	block := Block{Height: 1, Txs: []PooledTransaction{
		{Tx: testRoutingTx("tx_a", []string{"asset_1"}, map[string]int{"asset_1": 1})},
		{Tx: testRoutingTx("tx_b", []string{"asset_1"}, map[string]int{"asset_2": 1})},
		{Tx: testRoutingTx("tx_c", []string{"asset_3"}, map[string]int{"asset_4": 1})},
	}}
	first := engine.ExecuteBlock(block, 10)
	second := newExecutionEngine(ChainProfile{ExecutionShardCount: 4}, PluginProfile{ExecutionSchedulerPlugin: "parallel_light_execution"}).ExecuteBlock(block, 10)
	if engine.DependencyEdgeCount == 0 || first[1].DependencyEdgeCount == 0 || !first[1].Blocked {
		t.Fatalf("expected dependency edge and blocked tx: engine=%+v records=%+v", engine, first)
	}
	if first[0].WorkerID != second[0].WorkerID || first[2].WorkerID != second[2].WorkerID {
		t.Fatalf("parallel worker assignment should be deterministic: %+v vs %+v", first, second)
	}
}

func TestDualTrackExecutionProducesFastAndConservativeTracks(t *testing.T) {
	engine := newExecutionEngine(ChainProfile{ExecutionShardCount: 4}, PluginProfile{ExecutionSchedulerPlugin: "metatrack_dual_track_execution"})
	block := Block{Height: 1, Txs: []PooledTransaction{
		{Tx: testRoutingTx("tx_fast", []string{"asset_1"}, map[string]int{"asset_2": 1})},
		{Tx: testRoutingTx("tx_conservative", []string{"asset_1", "asset_2", "asset_3"}, map[string]int{"asset_1": 1, "asset_3": 1})},
	}}
	records := engine.ExecuteBlock(block, 10)
	if records[0].Track != "fast" || records[1].Track != "conservative" {
		t.Fatalf("expected fast then conservative tracks: %+v", records)
	}
	if engine.FastTrackCount != 1 || engine.ConservativeTrackCount != 1 {
		t.Fatalf("unexpected dual-track metrics: %+v", engine)
	}
}

func TestExecutionRuntimePreservesExistingArtifacts(t *testing.T) {
	out := filepath.Join(t.TempDir(), "execution_runtime")
	pluginPath := writeExecutionPluginProfile(t, t.TempDir(), "metatrack_dual_track_execution")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "test_execution",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "execution_runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"txpool_log.csv", "block_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing regression artifact %s: %v", name, err)
		}
	}
	if result.Summary.ExecutionPlugin != "metatrack_dual_track_execution" || result.Summary.ExecutionTxCount != result.Summary.TxCount {
		t.Fatalf("unexpected execution runtime summary: %+v", result.Summary)
	}
	rows := readCSVRows(t, filepath.Join(out, "execution_log.csv"))
	routingRows := readCSVRows(t, filepath.Join(out, "routing_log.csv"))
	if len(rows) == 0 || rows[0]["tx_id"] != routingRows[0]["tx_id"] || rows[0]["block_height"] != routingRows[0]["block_height"] {
		t.Fatalf("execution log should align with routing log: execution=%+v routing=%+v", rows, routingRows)
	}
}

func TestDirectFetchStateAccessLogAndSummary(t *testing.T) {
	out := filepath.Join(t.TempDir(), "state_access_direct")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     "../../configs/v3/plugins/v3_2_minimal_plugin_profile.yaml",
		PluginProfileID:       "v3_2_minimal_single_chain",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "state_access_direct",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCSVFields(t, filepath.Join(out, "state_access_log.csv"), stateAccessLogFields())
	rows := readCSVRows(t, filepath.Join(out, "state_access_log.csv"))
	txRows := readCSVRows(t, filepath.Join(out, "tx_results.csv"))
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	routingRows := readCSVRows(t, filepath.Join(out, "routing_log.csv"))
	executionRows := readCSVRows(t, filepath.Join(out, "execution_log.csv"))
	if len(rows) == 0 || rows[0]["state_access_plugin"] != "direct_fetch" {
		t.Fatalf("expected direct_fetch state access rows: %+v", rows)
	}
	if rows[0]["tx_id"] != txRows[0]["tx_id"] || rows[0]["block_height"] != txRows[0]["block_height"] {
		t.Fatalf("state access should align with tx_results: access=%+v tx=%+v", rows[0], txRows[0])
	}
	if rows[0]["block_height"] != blockRows[0]["block_height"] || rows[0]["tx_id"] != routingRows[0]["tx_id"] || rows[0]["tx_id"] != executionRows[0]["tx_id"] {
		t.Fatalf("state access should align with block/routing/execution logs: access=%+v block=%+v routing=%+v execution=%+v", rows[0], blockRows[0], routingRows[0], executionRows[0])
	}
	if result.Summary.StateAccessPlugin != "direct_fetch" || result.Summary.StateAccessCount == 0 || result.Summary.AvgStateAccessLatencyMS == 0 {
		t.Fatalf("unexpected direct fetch summary: %+v", result.Summary)
	}
}

func TestRemoteStateAccessModelCountsRemoteDeterministically(t *testing.T) {
	chain := ChainProfile{ExecutionShardCount: 4, StateStorageUnitCount: 4, RemoteFetchCostMS: 3}
	tx := testRoutingTx("tx_remote", []string{"asset_1", "asset_2"}, map[string]int{"asset_3": 1})
	first := newStateAccessEngine(chain, PluginProfile{StateAccessPlugin: "remote_state_access_model"})
	second := newStateAccessEngine(chain, PluginProfile{StateAccessPlugin: "remote_state_access_model"})
	firstRecords := first.AccessTransaction(tx, 1, 0, 0, nil, chain)
	secondRecords := second.AccessTransaction(tx, 1, 0, 0, nil, chain)
	if first.RemoteAccessCount == 0 || first.LocalAccessCount+first.RemoteAccessCount != len(firstRecords) {
		t.Fatalf("expected local/remote state access counts: engine=%+v records=%+v", first, firstRecords)
	}
	if first.RemoteAccessCount != second.RemoteAccessCount || first.Latencies[0] != second.Latencies[0] {
		t.Fatalf("remote state access should be deterministic: %+v vs %+v", firstRecords, secondRecords)
	}
}

func TestCachedStateAccessProducesHitsAndMisses(t *testing.T) {
	chain := ChainProfile{ExecutionShardCount: 4, StateStorageUnitCount: 4, RemoteFetchCostMS: 2}
	engine := newStateAccessEngine(chain, PluginProfile{StateAccessPlugin: "cached_state_access"})
	tx1 := testRoutingTx("tx_cache_1", []string{"hot"}, map[string]int{"asset_1": 1})
	tx2 := testRoutingTx("tx_cache_2", []string{"hot"}, map[string]int{"asset_2": 1})
	engine.AccessTransaction(tx1, 1, 0, 0, nil, chain)
	records := engine.AccessTransaction(tx2, 1, 1, 0, nil, chain)
	if engine.CacheHitCount == 0 || engine.CacheMissCount == 0 || !records[0].CacheHit {
		t.Fatalf("expected cache hit and miss records: engine=%+v records=%+v", engine, records)
	}
}

func TestAccessListPrefetchProducesHits(t *testing.T) {
	chain := ChainProfile{ExecutionShardCount: 4, StateStorageUnitCount: 4}
	block := Block{Height: 1, Txs: []PooledTransaction{
		{Tx: testRoutingTx("tx_prefetch", []string{"asset_1"}, map[string]int{"asset_2": 1})},
	}}
	engine := newStateAccessEngine(chain, PluginProfile{StateAccessPlugin: "access_list_prefetch"})
	prefetch := engine.PrefetchSet(block)
	records := engine.AccessTransaction(block.Txs[0].Tx, 1, 0, 0, prefetch, chain)
	if engine.PrefetchHitCount != len(records) || engine.PrefetchMissCount != 0 {
		t.Fatalf("expected all access list prefetch hits: engine=%+v records=%+v", engine, records)
	}
}

func TestStateAccessRuntimePreservesArtifactsAndDoesNotWriteProofFiles(t *testing.T) {
	out := filepath.Join(t.TempDir(), "state_access_runtime")
	pluginPath := writeStateAccessPluginProfile(t, t.TempDir(), "remote_state_access_model")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "test_state_access",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "state_access_runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"txpool_log.csv", "block_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv", "state_access_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing regression artifact %s: %v", name, err)
		}
	}
	for _, forbidden := range []string{"proof.json", "witness.json", "state_root.txt", "snapshot.bin"} {
		if _, err := os.Stat(filepath.Join(out, forbidden)); err == nil {
			t.Fatalf("state access hardening must not generate real proof/witness/snapshot artifact: %s", forbidden)
		}
	}
	if result.Summary.StateAccessPlugin != "remote_state_access_model" || result.Summary.WitnessEstimatedCount == 0 || result.Summary.ProofEstimatedCount == 0 {
		t.Fatalf("unexpected state access runtime summary: %+v", result.Summary)
	}
}

func TestCommitRuntimeWritesLogAndSummary(t *testing.T) {
	out := filepath.Join(t.TempDir(), "commit_runtime")
	pluginPath := writeCommitPluginProfile(t, t.TempDir(), "hot_update_aggregation")
	result, err := Run(Input{
		ChainProfilePath:      "../../configs/v3/chains/chain_x_default.yaml",
		PluginProfilePath:     pluginPath,
		PluginProfileID:       "test_commit",
		ExperimentProfilePath: "../../configs/v3/experiments/single_chain_runtime_smoke.yaml",
		OutputDir:             out,
		RunID:                 "commit_runtime",
	})
	if err != nil {
		t.Fatal(err)
	}
	assertCSVFields(t, filepath.Join(out, "state_commit_log.csv"), stateCommitLogFields())
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"commit_plugin", "commit_tx_count", "commit_update_count", "normal_commit_count", "conservative_commit_count", "hotspot_update_count", "raw_update_count", "aggregation_group_count", "constraint_check_count", "constraint_failed_count", "avg_commit_latency_ms", "p95_commit_latency_ms", "max_commit_latency_ms"})
	rows := readCSVRows(t, filepath.Join(out, "state_commit_log.csv"))
	txRows := readCSVRows(t, filepath.Join(out, "tx_results.csv"))
	blockRows := readCSVRows(t, filepath.Join(out, "block_log.csv"))
	if len(rows) == 0 || rows[0]["tx_id"] != txRows[0]["tx_id"] || rows[0]["block_height"] != blockRows[0]["block_height"] {
		t.Fatalf("commit log should align with tx and block logs: commit=%+v tx=%+v block=%+v", rows, txRows, blockRows)
	}
	if result.Summary.CommitPlugin != "hot_update_aggregation" || result.Summary.CommitUpdateCount == 0 || result.Summary.RawUpdateCount == 0 {
		t.Fatalf("unexpected commit summary: %+v", result.Summary)
	}
	for _, name := range []string{"txpool_log.csv", "block_log.csv", "consensus_log.csv", "routing_log.csv", "execution_log.csv", "state_access_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing regression artifact %s: %v", name, err)
		}
	}
}

func TestCommitEngineModelsConservativeAndConstraintPaths(t *testing.T) {
	chain := ChainProfile{StateStorageUnitCount: 4, StatePlacementPolicy: "hash_state_storage"}
	tx := Transaction{ID: "tx_negative", WriteDeltas: map[string]int{"asset_1": -1}}
	deltas := map[string][3]int{"asset_1": {0, -1, -1}}
	state := map[string]int{}
	engine := newCommitEngine(PluginProfile{CommitPlugin: "constraint_checked_aggregation"})
	records := engine.CommitTransaction(tx, 1, 0, 0, 10, deltas, state, chain, PluginProfile{ShardingPlugin: "hash_sharding"}, CommitPlan{RawByKey: map[string]int{"asset_1": 2}})
	if len(records) != 1 || records[0].CommitPath != "conservative" || !records[0].ConstraintChecked || records[0].ConstraintPassed {
		t.Fatalf("constraint failure should use conservative path: %+v", records)
	}
	if engine.ConstraintFailedCount != 1 || engine.ConservativeCommitCount != 1 || len(engine.Latencies) != 1 || engine.Latencies[0] != 3 {
		t.Fatalf("unexpected constraint metrics: %+v", engine)
	}
	conservative := newCommitEngine(PluginProfile{CommitPlugin: "conservative_commit"})
	records = conservative.CommitTransaction(testTx("tx_conservative", 0), 1, 0, 0, 10, map[string][3]int{"asset_1": {0, 1, 1}}, map[string]int{}, chain, PluginProfile{ShardingPlugin: "hash_sharding"}, CommitPlan{RawByKey: map[string]int{"asset_1": 1}})
	if records[0].CommitPath != "conservative" || conservative.ConservativeCommitCount != 1 {
		t.Fatalf("conservative commit should record conservative path: %+v engine=%+v", records, conservative)
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

func consensusLogFields() []string {
	return []string{"block_height", "block_hash", "consensus_plugin", "round_id", "view_id", "sequence_id", "leader_id", "validator_count", "fault_tolerance_f", "prepare_quorum", "commit_quorum", "preprepare_msg_count", "prepare_msg_count", "commit_msg_count", "total_message_count", "consensus_start_time_ms", "consensus_ordered_time_ms", "consensus_finalized_time_ms", "consensus_latency_ms", "finalized", "view_change_count", "reason"}
}

func routingLogFields() []string {
	return []string{"tx_id", "block_height", "tx_index", "routing_plugin", "access_key_count", "read_key_count", "write_key_count", "primary_shard", "touched_shards", "touched_shard_count", "cross_shard", "remote_state_access_estimate", "hotspot_key_count", "coaccess_group_id", "routing_overhead_ms", "reason"}
}

func executionLogFields() []string {
	return []string{"tx_id", "block_height", "tx_index", "execution_plugin", "track", "access_key_count", "read_key_count", "write_key_count", "dependency_edge_count", "dependency_risk", "ready_time_ms", "start_time_ms", "end_time_ms", "execution_latency_ms", "blocked", "block_reason", "worker_id", "reason"}
}

func stateAccessLogFields() []string {
	return []string{"tx_id", "block_height", "tx_index", "state_access_plugin", "access_key", "access_type", "is_read", "is_write", "home_shard", "execution_shard", "is_remote", "cache_hit", "prefetched", "witness_estimated", "proof_estimated", "access_latency_ms", "reason"}
}

func stateCommitLogFields() []string {
	return []string{"tx_id", "block_height", "tx_index", "commit_plugin", "commit_path", "state_key", "update_type", "is_hotspot", "aggregated", "aggregation_group_id", "raw_update_count", "aggregated_update_count", "constraint_checked", "constraint_passed", "commit_latency_ms", "reason"}
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

func testRoutingTx(id string, readKeys []string, writeDeltas map[string]int) Transaction {
	return Transaction{
		ID:           id,
		SubmitTimeMS: 0,
		ReadKeys:     readKeys,
		WriteDeltas:  writeDeltas,
		Commutative:  true,
		ConflictHint: "low",
	}
}

func writeConsensusPluginProfile(t *testing.T, dir string, consensusPlugin string) string {
	t.Helper()
	path := filepath.Join(dir, "plugin_profile.yaml")
	content := "profile_type: plugin_profile_collection\nversion: v3\nprofiles:\n  - plugin_profile_id: test_consensus\n    plugins:\n      TxPoolPlugin: fifo_pool\n      BlockProducer: time_or_count_block_producer\n      ConsensusPlugin: " + consensusPlugin + "\n      ShardingPlugin: hash_sharding\n      ExecutionSchedulerPlugin: serial_execution\n      StateAccessPlugin: direct_fetch\n      CommitPlugin: normal_commit\n      MetricsPlugin: basic_metrics\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeRoutingPluginProfile(t *testing.T, dir string, routingPlugin string) string {
	t.Helper()
	path := filepath.Join(dir, "plugin_profile.yaml")
	content := "profile_type: plugin_profile_collection\nversion: v3\nprofiles:\n  - plugin_profile_id: test_routing\n    plugins:\n      TxPoolPlugin: fifo_pool\n      BlockProducer: time_or_count_block_producer\n      ConsensusPlugin: simple_leader\n      ShardingPlugin: " + routingPlugin + "\n      ExecutionSchedulerPlugin: serial_execution\n      StateAccessPlugin: direct_fetch\n      CommitPlugin: normal_commit\n      MetricsPlugin: basic_metrics\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeExecutionPluginProfile(t *testing.T, dir string, executionPlugin string) string {
	t.Helper()
	path := filepath.Join(dir, "plugin_profile.yaml")
	content := "profile_type: plugin_profile_collection\nversion: v3\nprofiles:\n  - plugin_profile_id: test_execution\n    plugins:\n      TxPoolPlugin: fifo_pool\n      BlockProducer: time_or_count_block_producer\n      ConsensusPlugin: simple_leader\n      ShardingPlugin: hash_sharding\n      ExecutionSchedulerPlugin: " + executionPlugin + "\n      StateAccessPlugin: direct_fetch\n      CommitPlugin: normal_commit\n      MetricsPlugin: basic_metrics\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeStateAccessPluginProfile(t *testing.T, dir string, stateAccessPlugin string) string {
	t.Helper()
	path := filepath.Join(dir, "plugin_profile.yaml")
	content := "profile_type: plugin_profile_collection\nversion: v3\nprofiles:\n  - plugin_profile_id: test_state_access\n    plugins:\n      TxPoolPlugin: fifo_pool\n      BlockProducer: time_or_count_block_producer\n      ConsensusPlugin: simple_leader\n      ShardingPlugin: hash_sharding\n      ExecutionSchedulerPlugin: serial_execution\n      StateAccessPlugin: " + stateAccessPlugin + "\n      CommitPlugin: normal_commit\n      MetricsPlugin: basic_metrics\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func writeCommitPluginProfile(t *testing.T, dir string, commitPlugin string) string {
	t.Helper()
	path := filepath.Join(dir, "plugin_profile.yaml")
	content := "profile_type: plugin_profile_collection\nversion: v3\nprofiles:\n  - plugin_profile_id: test_commit\n    plugins:\n      TxPoolPlugin: fifo_pool\n      BlockProducer: time_or_count_block_producer\n      ConsensusPlugin: simple_leader\n      ShardingPlugin: hash_sharding\n      ExecutionSchedulerPlugin: serial_execution\n      StateAccessPlugin: direct_fetch\n      CommitPlugin: " + commitPlugin + "\n      MetricsPlugin: basic_metrics\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}
