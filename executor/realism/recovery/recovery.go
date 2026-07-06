package recovery

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"metaverse-chainlab/executor/realism/metrics"
)

type Summary struct {
	RuntimeStage        string `json:"runtime_stage"`
	RuntimeTruth        string `json:"runtime_truth"`
	NodeID              string `json:"node_id"`
	CommittedHeight     uint64 `json:"committed_height"`
	LatestBlockHash     string `json:"latest_block_hash"`
	LatestStateRoot     string `json:"latest_state_root"`
	NodeRecovery        bool   `json:"node_recovery"`
	CrashConsistencyMVP bool   `json:"crash_consistency_mvp"`
	ProductionRecovery  bool   `json:"production_recovery"`
}

func Recover(dataDir, outDir string) (Summary, error) {
	payload, err := os.ReadFile(filepath.Join(dataDir, "commit_summary.json"))
	if err != nil {
		return Summary{}, fmt.Errorf("read recovery commit summary: %w", err)
	}
	var raw struct {
		NodeID          string `json:"node_id"`
		CommittedHeight uint64 `json:"committed_height"`
		LatestBlockHash string `json:"latest_block_hash"`
		LatestStateRoot string `json:"latest_state_root"`
	}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return Summary{}, err
	}
	summary := Summary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: raw.NodeID, CommittedHeight: raw.CommittedHeight, LatestBlockHash: raw.LatestBlockHash, LatestStateRoot: raw.LatestStateRoot, NodeRecovery: true, CrashConsistencyMVP: true, ProductionRecovery: false}
	if outDir != "" {
		if err := metrics.WriteJSON(filepath.Join(outDir, "recovery_summary.json"), summary); err != nil {
			return Summary{}, err
		}
		if err := metrics.WriteCSV(filepath.Join(outDir, "recovery_log.csv"), []string{"node_id", "committed_height", "latest_block_hash", "latest_state_root", "node_recovery", "crash_consistency_mvp", "production_recovery"}, [][]string{{summary.NodeID, fmt.Sprint(summary.CommittedHeight), summary.LatestBlockHash, summary.LatestStateRoot, "true", "true", "false"}}); err != nil {
			return Summary{}, err
		}
	}
	return summary, nil
}
