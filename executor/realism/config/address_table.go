package config

type AddressEntry struct {
	NodeID     string `json:"node_id"`
	ShardID    string `json:"shard_id"`
	ListenAddr string `json:"listen_addr"`
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
		table.Entries = append(table.Entries, AddressEntry{NodeID: node.NodeID, ShardID: node.ShardID, ListenAddr: "", RealP2P: false})
	}
	return table
}
