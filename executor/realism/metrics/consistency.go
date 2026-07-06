package metrics

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

type ConsistencyNode struct {
	NodeID            string `json:"node_id"`
	CommittedHeight   uint64 `json:"committed_height"`
	LatestBlockHash   string `json:"latest_block_hash"`
	LatestStateRoot   string `json:"latest_state_root"`
	LatestReceiptRoot string `json:"latest_receipt_root"`
}

type ConsistencyReport struct {
	RuntimeStage  string            `json:"runtime_stage"`
	RuntimeTruth  string            `json:"runtime_truth"`
	Nodes         []ConsistencyNode `json:"nodes"`
	CheckedNodes  int               `json:"checked_nodes"`
	MismatchCount int               `json:"mismatch_count"`
	Consistent    bool              `json:"consistent"`
}

func CheckConsistency(nodeDirs []string, outDir string) (ConsistencyReport, error) {
	report := ConsistencyReport{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", Consistent: true}
	for _, dir := range nodeDirs {
		payload, err := os.ReadFile(filepath.Join(dir, "commit_summary.json"))
		if err != nil {
			return report, fmt.Errorf("read commit summary %s: %w", dir, err)
		}
		var raw struct {
			NodeID            string `json:"node_id"`
			CommittedHeight   uint64 `json:"committed_height"`
			LatestBlockHash   string `json:"latest_block_hash"`
			LatestStateRoot   string `json:"latest_state_root"`
			LatestReceiptRoot string `json:"latest_receipt_root"`
		}
		if err := json.Unmarshal(payload, &raw); err != nil {
			return report, err
		}
		report.Nodes = append(report.Nodes, ConsistencyNode(raw))
	}
	report.CheckedNodes = len(report.Nodes)
	if len(report.Nodes) > 1 {
		ref := report.Nodes[0]
		for _, node := range report.Nodes[1:] {
			if node.CommittedHeight != ref.CommittedHeight || node.LatestBlockHash != ref.LatestBlockHash || node.LatestStateRoot != ref.LatestStateRoot || node.LatestReceiptRoot != ref.LatestReceiptRoot {
				report.MismatchCount++
			}
		}
	}
	report.Consistent = report.MismatchCount == 0
	if outDir != "" {
		if err := WriteJSON(filepath.Join(outDir, "consistency_check_report.json"), report); err != nil {
			return report, err
		}
		rows := [][]string{}
		for _, node := range report.Nodes {
			rows = append(rows, []string{node.NodeID, fmt.Sprint(node.CommittedHeight), node.LatestBlockHash, node.LatestStateRoot, node.LatestReceiptRoot})
		}
		if err := WriteCSV(filepath.Join(outDir, "consistency_check_log.csv"), []string{"node_id", "committed_height", "latest_block_hash", "latest_state_root", "latest_receipt_root"}, rows); err != nil {
			return report, err
		}
	}
	return report, nil
}
