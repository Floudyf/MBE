package config

import (
	"fmt"
	"path/filepath"
)

type NodeConfig struct {
	NodeID  string `json:"node_id"`
	ShardID string `json:"shard_id"`
	DataDir string `json:"data_dir"`
}

type RuntimeConfig struct {
	RuntimeStage string       `json:"runtime_stage"`
	RuntimeTruth string       `json:"runtime_truth"`
	RealP2P      bool         `json:"real_p2p"`
	RealPBFT     bool         `json:"real_pbft"`
	Nodes        []NodeConfig `json:"nodes"`
}

func Generate(nodes, shards int, dataDir string) (RuntimeConfig, error) {
	if nodes <= 0 {
		return RuntimeConfig{}, fmt.Errorf("nodes must be positive")
	}
	if shards <= 0 {
		return RuntimeConfig{}, fmt.Errorf("shards must be positive")
	}
	out := RuntimeConfig{RuntimeStage: "v4_0_real_node_foundation", RuntimeTruth: "v4_real_node_foundation", RealP2P: false, RealPBFT: false}
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("n%d", i)
		shardID := fmt.Sprintf("s%d", i%shards)
		out.Nodes = append(out.Nodes, NodeConfig{NodeID: nodeID, ShardID: shardID, DataDir: filepath.Join(dataDir, nodeID)})
	}
	return out, nil
}
