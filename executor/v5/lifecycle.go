package v5

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// LifecycleEvent is raw runtime evidence. Finality summaries are derived from
// these events after the processes exit; they are never TCP send measurements.
type LifecycleEvent struct {
	TimestampMS int64  `json:"timestamp_ms"`
	TxID        string `json:"tx_id"`
	LogicalTxID string `json:"logical_tx_id"`
	Stage       string `json:"stage"`
	NodeID      string `json:"node_id"`
	ShardID     string `json:"shard_id"`
	SourceShard string `json:"source_shard,omitempty"`
	TargetShard string `json:"target_shard,omitempty"`
	BlockHeight uint64 `json:"block_height,omitempty"`
	Success     bool   `json:"success"`
	Error       string `json:"error,omitempty"`
}

func writeLifecycleJSONL(path string, events []LifecycleEvent) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	for _, event := range events {
		line, err := json.Marshal(event)
		if err != nil {
			return err
		}
		if _, err := file.Write(append(line, '\n')); err != nil {
			return err
		}
	}
	return nil
}

func lifecycleRow(e LifecycleEvent) []string {
	return []string{fmt.Sprint(e.TimestampMS), e.TxID, e.LogicalTxID, e.Stage, e.NodeID, e.ShardID, e.SourceShard, e.TargetShard, fmt.Sprint(e.BlockHeight), fmt.Sprint(e.Success), e.Error}
}

func nowLifecycle(txID, stage, nodeID, shardID string) LifecycleEvent {
	return LifecycleEvent{TimestampMS: time.Now().UnixMilli(), TxID: txID, LogicalTxID: txID, Stage: stage, NodeID: nodeID, ShardID: shardID, Success: true}
}
