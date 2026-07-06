package config

type AddressEntry struct {
	NodeID     string `json:"node_id"`
	ShardID    string `json:"shard_id"`
	ListenAddr string `json:"listen_addr"`
	Role       string `json:"role"`
	Leader     bool   `json:"leader"`
	RealP2P    bool   `json:"real_p2p"`
}

type AddressTable struct {
	RuntimeStage string         `json:"runtime_stage"`
	RuntimeTruth string         `json:"runtime_truth"`
	RealP2P      bool           `json:"real_p2p"`
	Entries      []AddressEntry `json:"entries"`
}

func BuildAddressTable(cfg RuntimeConfig) AddressTable {
	table := AddressTable{RuntimeStage: cfg.RuntimeStage, RuntimeTruth: cfg.RuntimeTruth, RealP2P: false}
	for _, node := range cfg.Nodes {
		table.Entries = append(table.Entries, AddressEntry{NodeID: node.NodeID, ShardID: node.ShardID, ListenAddr: node.ListenAddr, Role: node.Role, Leader: node.Leader, RealP2P: cfg.RealP2P})
	}
	return table
}

type SupervisorPlan struct {
	RuntimeStage string   `json:"runtime_stage"`
	RuntimeTruth string   `json:"runtime_truth"`
	RealP2P      bool     `json:"real_p2p"`
	RealPBFT     bool     `json:"real_pbft"`
	StateCommit  bool     `json:"state_commit"`
	Commands     []string `json:"commands"`
}

func BuildSupervisorPlan(cfg RuntimeConfig) SupervisorPlan {
	commands := []string{}
	for _, node := range cfg.Nodes {
		commands = append(commands, "go run ./cmd/mbe-node --run-mode server --node-id "+node.NodeID+" --shard-id "+node.ShardID+" --listen-addr "+node.ListenAddr+" --role "+node.Role+" --consensus pbft --data-dir "+node.DataDir)
	}
	return SupervisorPlan{RuntimeStage: cfg.RuntimeStage, RuntimeTruth: cfg.RuntimeTruth, RealP2P: cfg.RealP2P, RealPBFT: cfg.RealPBFT, StateCommit: false, Commands: commands}
}
