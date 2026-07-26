package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

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

type CommitMarker struct {
	Version     string `json:"version"`
	NodeID      string `json:"node_id"`
	ShardID     string `json:"shard_id"`
	Height      uint64 `json:"height"`
	BlockHash   string `json:"block_hash"`
	ReceiptRoot string `json:"receipt_root"`
	StateRoot   string `json:"state_root"`
	Committed   bool   `json:"committed"`
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

type PersistenceMetrics struct {
	ReceiptBatchWriteMS int64 `json:"receipt_batch_write_ms"`
	TxIndexBatchWriteMS int64 `json:"tx_index_batch_write_ms"`
	DurableCommitMS     int64 `json:"durable_commit_ms"`
	WrittenBytes        int64 `json:"persistence_written_bytes"`
}

func (s *BlockStore) DurableCommit(b block.Block, result execution.Result) (CommitSummary, error) {
	_, err := s.DurableCommitWithMetrics(b, result)
	if err != nil {
		return CommitSummary{}, err
	}
	summary := CommitSummary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: s.NodeID, ShardID: s.ShardID, CommittedHeight: b.Height, LatestBlockHash: b.BlockHash, LatestStateRoot: result.StateRootAfter, LatestReceiptRoot: result.ReceiptRoot, BlockDB: true, StateDB: true, ReceiptDB: true, TxIndex: true, StateCommit: true}
	return summary, nil
}

func (s *BlockStore) DurableCommitWithMetrics(b block.Block, result execution.Result) (PersistenceMetrics, error) {
	started := time.Now()
	var pm PersistenceMetrics
	if err := os.MkdirAll(s.DataDir, 0o755); err != nil {
		return pm, fmt.Errorf("create durable store: %w", err)
	}
	b.StateRootBefore = result.StateRootBefore
	b.StateRootAfter = result.StateRootAfter
	b.ReceiptRoot = result.ReceiptRoot
	b.StateCommit = true

	receiptStarted := time.Now()
	written, err := appendJSONBatch(filepath.Join(s.DataDir, "receipts.jsonl"), receiptsAsAny(result.Receipts))
	pm.ReceiptBatchWriteMS = time.Since(receiptStarted).Milliseconds()
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written
	if s.failpoint == "after_receipt_append" {
		return pm, fmt.Errorf("injected durable commit failure after receipt append")
	}

	indexRecords := make([]any, 0, len(result.Receipts))
	for _, receipt := range result.Receipts {
		indexRecords = append(indexRecords, TxIndexRecord{TxID: receipt.TxID, BlockHash: receipt.BlockHash, Height: receipt.Height, ReceiptOK: receipt.Success, ReceiptRoot: result.ReceiptRoot})
	}
	indexStarted := time.Now()
	written, err = appendJSONBatch(filepath.Join(s.DataDir, "tx_index.jsonl"), indexRecords)
	pm.TxIndexBatchWriteMS = time.Since(indexStarted).Milliseconds()
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written

	written, err = appendJSON(filepath.Join(s.DataDir, "blocks.jsonl"), b)
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written
	if s.failpoint == "after_block_append" {
		return pm, fmt.Errorf("injected durable commit failure after block append")
	}
	marker := CommitMarker{Version: "durable_commit_marker_v1", NodeID: s.NodeID, ShardID: s.ShardID, Height: b.Height, BlockHash: b.BlockHash, ReceiptRoot: result.ReceiptRoot, StateRoot: result.StateRootAfter, Committed: true}
	written, err = appendJSON(filepath.Join(s.DataDir, "commit_markers.jsonl"), marker)
	if err != nil {
		return pm, err
	}
	pm.WrittenBytes += written
	if s.failpoint == "after_commit_marker" {
		return pm, fmt.Errorf("injected durable commit failure after commit marker")
	}

	summary := CommitSummary{RuntimeStage: "v4_2_state_cross_shard_recovery_frontend", RuntimeTruth: "v4_real_state_cross_shard_recovery", NodeID: s.NodeID, ShardID: s.ShardID, CommittedHeight: b.Height, LatestBlockHash: b.BlockHash, LatestStateRoot: result.StateRootAfter, LatestReceiptRoot: result.ReceiptRoot, BlockDB: true, StateDB: true, ReceiptDB: true, TxIndex: true, StateCommit: true}
	if err := metrics.WriteJSON(filepath.Join(s.DataDir, "commit_summary.json"), summary); err != nil {
		return pm, err
	}
	if err := metrics.WriteCSV(filepath.Join(s.DataDir, "commit_log.csv"), []string{"node_id", "shard_id", "height", "block_hash", "state_root_before", "state_root_after", "receipt_root", "tx_count", "execution_status", "state_commit"}, [][]string{{s.NodeID, s.ShardID, fmt.Sprint(b.Height), b.BlockHash, result.StateRootBefore, result.StateRootAfter, result.ReceiptRoot, fmt.Sprint(len(b.TxIDs)), "executed", "true"}}); err != nil {
		return pm, err
	}
	pm.DurableCommitMS = time.Since(started).Milliseconds()
	return pm, nil
}

func receiptsAsAny(receipts []execution.Receipt) []any {
	out := make([]any, 0, len(receipts))
	for _, receipt := range receipts {
		out = append(out, receipt)
	}
	return out
}

func appendJSON(path string, value any) (int64, error) {
	return appendJSONBatch(path, []any{value})
}

func appendJSONBatch(path string, values []any) (int64, error) {
	before := int64(0)
	if info, err := os.Stat(path); err == nil {
		before = info.Size()
	} else if !os.IsNotExist(err) {
		return 0, fmt.Errorf("stat jsonl %s: %w", path, err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return 0, fmt.Errorf("open jsonl %s: %w", path, err)
	}
	writer := bufio.NewWriter(f)
	encoder := json.NewEncoder(writer)
	for _, value := range values {
		if err := encoder.Encode(value); err != nil {
			_ = f.Close()
			return 0, fmt.Errorf("write jsonl %s: %w", path, err)
		}
	}
	if err := writer.Flush(); err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("flush jsonl %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return 0, fmt.Errorf("sync jsonl %s: %w", path, err)
	}
	if err := f.Close(); err != nil {
		return 0, fmt.Errorf("close jsonl %s: %w", path, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		return 0, fmt.Errorf("stat written jsonl %s: %w", path, err)
	}
	return info.Size() - before, nil
}
