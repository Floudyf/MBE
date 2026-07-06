package config

import (
	"fmt"
	"path/filepath"
)

type NodeConfig struct {
	NodeID     string   `json:"node_id"`
	ShardID    string   `json:"shard_id"`
	DataDir    string   `json:"data_dir"`
	ListenAddr string   `json:"listen_addr"`
	Role       string   `json:"role"`
	Leader     bool     `json:"leader"`
	Validators []string `json:"validators"`
}

type RuntimeConfig struct {
	RuntimeStage string       `json:"runtime_stage"`
	RuntimeTruth string       `json:"runtime_truth"`
	RealP2P      bool         `json:"real_p2p"`
	RealPBFT     bool         `json:"real_pbft"`
	Consensus    string       `json:"consensus"`
	BlockSize    int          `json:"block_size"`
	Nodes        []NodeConfig `json:"nodes"`
}

func Generate(nodes, shards int, dataDir string) (RuntimeConfig, error) {
	if nodes <= 0 {
		return RuntimeConfig{}, fmt.Errorf("nodes must be positive")
	}
	if shards <= 0 {
		return RuntimeConfig{}, fmt.Errorf("shards must be positive")
	}
	out := RuntimeConfig{RuntimeStage: "v4_1_network_consensus_commit", RuntimeTruth: "v4_real_p2p_consensus_commit", RealP2P: true, RealPBFT: true, Consensus: "pbft_style", BlockSize: 10}
	byShard := map[string][]string{}
	for i := 0; i < nodes; i++ {
		shardID := fmt.Sprintf("s%d", i%shards)
		byShard[shardID] = append(byShard[shardID], fmt.Sprintf("n%d", i))
	}
	for i := 0; i < nodes; i++ {
		nodeID := fmt.Sprintf("n%d", i)
		shardID := fmt.Sprintf("s%d", i%shards)
		validators := byShard[shardID]
		leader := len(validators) > 0 && validators[0] == nodeID
		role := "validator"
		if leader {
			role = "leader"
		}
		out.Nodes = append(out.Nodes, NodeConfig{NodeID: nodeID, ShardID: shardID, DataDir: filepath.Join(dataDir, nodeID), ListenAddr: "127.0.0.1:0", Role: role, Leader: leader, Validators: validators})
	}
	return out, nil
}
