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
	DataDir           string
	NodeID            string
	ShardID           string
	failpoint         string
	rollbackFailpoint bool
}

// SetFailpointForTest is intentionally narrow and opt-in. It provides a
// deterministic partial-write failure for rollback tests without changing the
// normal durable commit path.
func (s *BlockStore) SetFailpointForTest(point string) { s.failpoint = point }

func (s *BlockStore) SetRollbackFailpointForTest(enabled bool) { s.rollbackFailpoint = enabled }

type ArtifactCheckpoint struct {
	appendSizes    map[string]int64
	replaceData    map[string][]byte
	replaceMissing map[string]bool
}

func (s *BlockStore) Checkpoint() (ArtifactCheckpoint, error) {
	checkpoint := ArtifactCheckpoint{appendSizes: map[string]int64{}, replaceData: map[string][]byte{}, replaceMissing: map[string]bool{}}
	for _, path := range []string{
		filepath.Join(s.DataDir, "blocks.jsonl"),
		filepath.Join(s.DataDir, "receipts.jsonl"),
		filepath.Join(s.DataDir, "tx_index.jsonl"),
	} {
		info, err := os.Stat(path)
		if err == nil {
			checkpoint.appendSizes[path] = info.Size()
		} else if os.IsNotExist(err) {
			checkpoint.appendSizes[path] = -1
		} else {
			return checkpoint, fmt.Errorf("checkpoint append artifact %s: %w", path, err)
		}
	}
	for _, path := range []string{
		filepath.Join(s.DataDir, "commit_summary.json"),
		filepath.Join(s.DataDir, "commit_log.csv"),
	} {
		data, err := os.ReadFile(path)
		if err == nil {
			checkpoint.replaceData[path] = data
		} else if os.IsNotExist(err) {
			checkpoint.replaceMissing[path] = true
		} else {
			return checkpoint, err
		}
	}
	return checkpoint, nil
}

func (s *BlockStore) Rollback(checkpoint ArtifactCheckpoint) error {
	if s.rollbackFailpoint {
		return fmt.Errorf("injected block store rollback failure")
	}
	for path, size := range checkpoint.appendSizes {
		if size == -1 {
			if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
				return err
			}
			continue
		}
		if size < -1 {
			return fmt.Errorf("invalid checkpoint size %d for %s", size, path)
		}
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				return fmt.Errorf("rollback missing append artifact %s", path)
			}
			return err
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0o644)
		if err != nil {
			return err
		}
		if err := file.Truncate(size); err != nil {
			_ = file.Close()
			return err
		}
		if err := file.Close(); err != nil {
			return err
		}
	}
	for path, data := range checkpoint.replaceData {
		if err := os.WriteFile(path, data, 0o644); err != nil {
			return err
		}
	}
	for path := range checkpoint.replaceMissing {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return nil
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
	if _, err := os.Stat(path); os.IsNotExist(err) {
		// V5 durable commits are stored in the existing blocks.jsonl artifact.
		// Keep committed_blocks.jsonl as the legacy test/compatibility source.
		path = filepath.Join(s.DataDir, "blocks.jsonl")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open committed block log: %w", err)
	}
	defer f.Close()
	var out []block.Block
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		row := scanner.Bytes()
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(row, &fields); err != nil {
			return nil, fmt.Errorf("decode committed block row: %w", err)
		}
		var raw []byte = row
		if wrapped, ok := fields["block"]; ok {
			raw = wrapped
		}
		var item block.Block
		if err := json.Unmarshal(raw, &item); err != nil {
			return nil, fmt.Errorf("decode committed block: %w", err)
		}
		if item.Height == 0 || item.BlockHash == "" {
			return nil, fmt.Errorf("invalid committed block row: height=%d block_hash=%q", item.Height, item.BlockHash)
		}
		out = append(out, item)
	}
	return out, scanner.Err()
}

func (s *BlockStore) ReadCommittedAtHeight(height uint64) (block.Block, bool, error) {
	blocks, err := s.ReadCommitted()
	if err != nil {
		return block.Block{}, false, err
	}
	for _, item := range blocks {
		if item.Height == height {
			return item, true, nil
		}
	}
	return block.Block{}, false, nil
}

func (s *BlockStore) HasTransaction(txID string) (bool, error) {
	path := filepath.Join(s.DataDir, "tx_index.jsonl")
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var row TxIndexRecord
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil {
			return false, err
		}
		if row.TxID == txID {
			return true, nil
		}
	}
	return false, scanner.Err()
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
