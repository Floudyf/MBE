package pbft

import "metaverse-chainlab/executor/realism/block"

type PrePrepare struct {
	View      uint64      `json:"view"`
	Sequence  uint64      `json:"sequence"`
	Height    uint64      `json:"height"`
	LeaderID  string      `json:"leader_id"`
	BlockHash string      `json:"block_hash"`
	Block     block.Block `json:"block"`
}

type Prepare struct {
	View      uint64 `json:"view"`
	Sequence  uint64 `json:"sequence"`
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
	BlockHash string `json:"block_hash"`
}

type Commit struct {
	View      uint64 `json:"view"`
	Sequence  uint64 `json:"sequence"`
	Height    uint64 `json:"height"`
	NodeID    string `json:"node_id"`
	BlockHash string `json:"block_hash"`
}

type ViewChange struct {
	View     uint64 `json:"view"`
	NewView  uint64 `json:"new_view"`
	NodeID   string `json:"node_id"`
	Height   uint64 `json:"height"`
	LeaderID string `json:"leader_id"`
}

type NewView struct {
	View     uint64 `json:"view"`
	LeaderID string `json:"leader_id"`
	Height   uint64 `json:"height"`
}
