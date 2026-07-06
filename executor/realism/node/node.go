package node

import (
	"time"

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/mempool"
)

type Config struct {
	NodeID          string
	ShardID         string
	DataDir         string
	MempoolCapacity int
	InputJSONL      string
	RunMode         string
	SummaryOut      string
	MempoolLogOut   string
	AdmissionLogOut string
	RealTraceImport bool
	TTL             time.Duration
}

type Node struct {
	NodeID  string
	ShardID string
	DataDir string
	Nonces  *account.NonceManager
	Mempool *mempool.Mempool
	Stage   string
	RunMode string
}

func New(cfg Config) *Node {
	nonces := account.NewNonceManager()
	policy := mempool.DefaultPolicy()
	if cfg.MempoolCapacity > 0 {
		policy.Capacity = cfg.MempoolCapacity
	}
	if cfg.TTL > 0 {
		policy.TTL = cfg.TTL
	}
	return &Node{
		NodeID:  cfg.NodeID,
		ShardID: cfg.ShardID,
		DataDir: cfg.DataDir,
		Nonces:  nonces,
		Mempool: mempool.New(cfg.NodeID, cfg.ShardID, policy, nonces),
		Stage:   "v4_0_real_node_foundation",
		RunMode: cfg.RunMode,
	}
}
