package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/metrics"
)

type TxIndexRecord struct {
	TxID        string `json:"tx_id"`
	BlockHash   string `json:"block_hash"`
	Height      uint64 `json:"height"`
	ReceiptOK   bool   `json:"receipt_success"`
	ReceiptRoot string `json:"receipt_root"`
}

type CommitSummary struct {
	RuntimeStage      string `json:"runtime_stage"`
	RuntimeTruth      string `json:"runtime_truth"`
	NodeID            string `json:"node_id"`
	ShardID           string `json:"shard_id"`
	CommittedHeight   uint64 `json:"committed_height"`
	LatestBlockHash   string `json:"latest_block_hash"`
	LatestStateRoot   string `json:"latest_state_root"`
	LatestReceiptRoot string `json:"latest_receipt_root"`
	BlockDB           bool   `json:"block_db"`
	StateDB           bool   `json:"state_db"`
	ReceiptDB         bool   `json:"receipt_db"`
	TxIndex           bool   `json:"tx_index"`
	StateCommit       bool   `json:"state_commit"`
}

func (s *BlockStore) DurableCommit(b block.Block, result execution.Result) (CommitSummary, error) {
	if err := os.MkdirAll(s.DataDir, 0o755); err != nil {
		return CommitSummary{}, fmt.Errorf("create durable store: %w", err)
	}
	b.StateRootBefore = result.StateRootBefore
	b.StateRootAfter = result.StateRootAfter
	b.ReceiptRoot = result.ReceiptRoot
	b.StateCommit = true
	if err := appendJSON(filepath.Join(s.DataDir, "blocks.jsonl"), b); err != nil {
		return CommitSummary{}, err
	}
	for _, receipt := range result.Receipts {
		if err := appendJSON(filepath.Join(s.DataDir, "receipts.jsonl"), receipt); err != nil {
			return CommitSummary{}, err
		}
		if err := appendJSON(filepath.Join(s.DataDir, "tx_index.jsonl"), TxIndexRecord{TxID: receipt.TxID, BlockHash: receipt.BlockHash, Height: receipt.Height, ReceiptOK: receipt.Success, ReceiptRoot: result.ReceiptRoot}); err != nil {
			return CommitSummary{}, err
		}
	}
	summary := CommitSummary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: s.NodeID, ShardID: s.ShardID, CommittedHeight: b.Height, LatestBlockHash: b.BlockHash, LatestStateRoot: result.StateRootAfter, LatestReceiptRoot: result.ReceiptRoot, BlockDB: true, StateDB: true, ReceiptDB: true, TxIndex: true, StateCommit: true}
	if err := metrics.WriteJSON(filepath.Join(s.DataDir, "commit_summary.json"), summary); err != nil {
		return CommitSummary{}, err
	}
	if err := metrics.WriteCSV(filepath.Join(s.DataDir, "commit_log.csv"), []string{"node_id", "shard_id", "height", "block_hash", "state_root_before", "state_root_after", "receipt_root", "tx_count", "execution_status", "state_commit"}, [][]string{{s.NodeID, s.ShardID, fmt.Sprint(b.Height), b.BlockHash, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, fmt.Sprint(len(b.TxIDs)), "executed", "true"}}); err != nil {
		return CommitSummary{}, err
	}
	return summary, nil
}

func appendJSON(path string, value any) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open jsonl %s: %w", path, err)
	}
	defer f.Close()
	if err := json.NewEncoder(f).Encode(value); err != nil {
		return fmt.Errorf("write jsonl %s: %w", path, err)
	}
	return nil
}
