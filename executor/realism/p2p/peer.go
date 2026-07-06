package p2p

type Peer struct {
	NodeID     string `json:"node_id"`
	ShardID    string `json:"shard_id"`
	ListenAddr string `json:"listen_addr"`
	Role       string `json:"role,omitempty"`
	Leader     bool   `json:"leader,omitempty"`
}
