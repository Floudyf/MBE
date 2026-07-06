package block

import "metaverse-chainlab/executor/realism/tx"

type Block struct {
	BlockHash          string                 `json:"block_hash"`
	ShardID            string                 `json:"shard_id"`
	Height             uint64                 `json:"height"`
	PreviousHash       string                 `json:"previous_hash"`
	ProposerID         string                 `json:"proposer_id"`
	Timestamp          int64                  `json:"timestamp"`
	TxIDs              []string               `json:"tx_ids"`
	TxList             []tx.SignedTransaction `json:"tx_list"`
	TxRoot             string                 `json:"tx_root"`
	StateRootBefore    string                 `json:"state_root_before"`
	StateRootAfter     string                 `json:"state_root_after"`
	ReceiptRoot        string                 `json:"receipt_root"`
	ProposerDigest     string                 `json:"proposer_digest"`
	StateCommit        bool                   `json:"state_commit"`
	CrossShardProtocol bool                   `json:"cross_shard_protocol"`
}
