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
	assertCSVFields(t, filepath.Join(out, "block_log.csv"), []string{"block_height", "block_id", "proposer_node", "tx_count", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status"})
	assertCSVFields(t, filepath.Join(out, "tx_results.csv"), []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "read_count", "write_count", "remote_fetch_count"})
	assertCSVFields(t, filepath.Join(out, "state_commit_log.csv"), []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status"})
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
