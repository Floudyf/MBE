package execution

type Receipt struct {
	TxID             string   `json:"tx_id"`
	BlockHash        string   `json:"block_hash"`
	Height           uint64   `json:"height"`
	Success          bool     `json:"success"`
	Error            string   `json:"error"`
	ExecutionCost    int64    `json:"execution_cost"`
	StateKeys        []string `json:"state_keys"`
	StateRootAfterTx string   `json:"state_root_after_tx"`
}
