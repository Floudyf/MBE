package v3runtime

import (
	"encoding/csv"
	"os"
	"path/filepath"
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
	for _, name := range []string{"used_chain_profile.yaml", "used_plugin_profile.yaml", "used_experiment_profile.yaml", "runtime.log", "summary.csv", "summary.json", "report.md", "block_log.csv", "tx_results.csv", "state_commit_log.csv"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatalf("missing artifact %s: %v", name, err)
		}
	}
	assertCSVFields(t, filepath.Join(out, "block_log.csv"), []string{"block_height", "block_id", "proposer_node", "tx_count", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status", "consensus_domain_id", "validator_count", "execution_shard_count", "state_storage_unit_count"})
	assertCSVFields(t, filepath.Join(out, "tx_results.csv"), []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "consensus_domain_id", "execution_shard_id", "home_state_unit_ids", "accessed_state_unit_ids", "remote_state_unit_count", "remote_fetch_count", "cross_state_unit_access", "state_locality_hit", "read_count", "write_count"})
	assertCSVFields(t, filepath.Join(out, "state_commit_log.csv"), []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status", "state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"})
	assertCSVFields(t, filepath.Join(out, "summary.csv"), []string{"execution_shard_count", "state_storage_unit_count", "cross_state_unit_access_count", "remote_state_fetch_count", "state_locality_ratio", "execution_shard_load_balance", "state_unit_load_balance"})
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

func placementByKey(commits []StateCommit) map[string]int {
	result := map[string]int{}
	for _, commit := range commits {
		result[commit.StateKey] = commit.StateStorageUnitID
	}
	return result
}
