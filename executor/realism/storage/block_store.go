package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/metrics"
)

type CommitRecord struct {
	Timestamp     int64  `json:"timestamp"`
	NodeID        string `json:"node_id"`
	ShardID       string `json:"shard_id"`
	Height        uint64 `json:"height"`
	BlockHash     string `json:"block_hash"`
	ProposerID    string `json:"proposer_id"`
	TxCount       int    `json:"tx_count"`
	PrepareQuorum bool   `json:"prepare_quorum"`
	CommitQuorum  bool   `json:"commit_quorum"`
	Committed     bool   `json:"committed"`
	StateCommit   bool   `json:"state_commit"`
}

type BlockStore struct {
	DataDir string
	NodeID  string
	ShardID string
}

func NewBlockStore(dataDir, nodeID, shardID string) *BlockStore {
	return &BlockStore{DataDir: dataDir, NodeID: nodeID, ShardID: shardID}
}

func (s *BlockStore) AppendCommitted(b block.Block, record CommitRecord) error {
	if err := os.MkdirAll(s.DataDir, 0o755); err != nil {
		return fmt.Errorf("create block store: %w", err)
	}
	path := filepath.Join(s.DataDir, "committed_blocks.jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("open committed block log: %w", err)
	}
	defer f.Close()
	payload := struct {
		Block  block.Block  `json:"block"`
		Commit CommitRecord `json:"commit"`
	}{Block: b, Commit: record}
	if err := json.NewEncoder(f).Encode(payload); err != nil {
		return fmt.Errorf("write committed block: %w", err)
	}
	return nil
}

func (s *BlockStore) ReadCommitted() ([]block.Block, error) {
	path := filepath.Join(s.DataDir, "committed_blocks.jsonl")
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open committed block log: %w", err)
	}
	defer f.Close()
	var out []block.Block
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row struct {
			Block block.Block `json:"block"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return nil, fmt.Errorf("decode committed block: %w", err)
		}
		out = append(out, row.Block)
	}
	return out, scanner.Err()
}

func WriteCommitCSV(path string, records []CommitRecord) error {
	rows := [][]string{}
	for _, r := range records {
		rows = append(rows, []string{
			strconv.FormatInt(r.Timestamp, 10),
			r.NodeID,
			r.ShardID,
			strconv.FormatUint(r.Height, 10),
			r.BlockHash,
			r.ProposerID,
			strconv.Itoa(r.TxCount),
			strconv.FormatBool(r.PrepareQuorum),
			strconv.FormatBool(r.CommitQuorum),
			strconv.FormatBool(r.Committed),
			strconv.FormatBool(r.StateCommit),
		})
	}
	return metrics.WriteCSV(path, []string{"timestamp", "node_id", "shard_id", "height", "block_hash", "proposer_id", "tx_count", "prepare_quorum", "commit_quorum", "committed", "state_commit"}, rows)
}
