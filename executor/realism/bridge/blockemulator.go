package bridge

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"

	"metaverse-chainlab/executor/realism/metrics"
)

type ComparisonSummary struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	TxCount                        int    `json:"tx_count"`
	NodeCount                      int    `json:"node_count"`
	ShardCount                     int    `json:"shard_count"`
	ConsensusMessageCount          int    `json:"consensus_message_count"`
	NetworkMessageCount            int    `json:"network_message_count"`
	CommittedBlocks                int    `json:"committed_blocks"`
	CommittedTxs                   int    `json:"committed_txs"`
	StateRootMismatchCount         int    `json:"state_root_mismatch_count"`
	CrossShardTxCount              int    `json:"cross_shard_tx_count"`
	RecoverySupported              bool   `json:"recovery_supported"`
	FaultInjectionSupported        bool   `json:"fault_injection_supported"`
	BlockEmulatorBridgeMVP         bool   `json:"blockemulator_bridge_mvp"`
	FullBlockEmulatorCompatibility bool   `json:"full_blockemulator_compatibility"`
}

func ImportTraceCSV(input, outDir string) (int, error) {
	f, err := os.Open(input)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return 0, err
	}
	count := 0
	if len(rows) > 0 {
		count = len(rows) - 1
	}
	if outDir != "" {
		if err := metrics.WriteCSV(filepath.Join(outDir, "blockemulator_trace_import_log.csv"), []string{"source", "imported_rows", "blockemulator_bridge_mvp", "full_blockemulator_compatibility"}, [][]string{{input, fmt.Sprint(count), "true", "false"}}); err != nil {
			return 0, err
		}
	}
	return count, nil
}

func WriteComparisonSummary(outDir string, summary ComparisonSummary) error {
	summary.RuntimeStage = "v4_2_state_cross_shard_recovery_frontend"
	summary.RuntimeTruth = "v4_real_state_cross_shard_recovery"
	summary.BlockEmulatorBridgeMVP = true
	summary.FullBlockEmulatorCompatibility = false
	return metrics.WriteJSON(filepath.Join(outDir, "blockemulator_comparison_summary.json"), summary)
}
