package node

type Summary struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	NodeID                         string `json:"node_id"`
	ShardID                        string `json:"shard_id"`
	TotalInputTxs                  int    `json:"total_input_txs"`
	AcceptedTxs                    int    `json:"accepted_txs"`
	RejectedTxs                    int    `json:"rejected_txs"`
	MempoolSize                    int    `json:"mempool_size"`
	RealSignedTx                   bool   `json:"real_signed_tx"`
	RealNonceCheck                 bool   `json:"real_nonce_check"`
	PerNodeMempool                 bool   `json:"per_node_mempool"`
	RealTraceImport                bool   `json:"real_trace_import"`
	RealP2P                        bool   `json:"real_p2p"`
	RealPBFT                       bool   `json:"real_pbft"`
	BlockProposer                  bool   `json:"block_proposer"`
	StateCommit                    bool   `json:"state_commit"`
	CrossShardProtocol             bool   `json:"cross_shard_protocol"`
	FrontendRealismMode            bool   `json:"frontend_realism_mode"`
	NotBlockEmulatorReplacementYet bool   `json:"not_blockemulator_replacement_yet"`
	RunMode                        string `json:"run_mode"`
}

func NewSummary(cfg Config, total, accepted, rejected, size int, realTraceImport bool) Summary {
	return Summary{
		RuntimeStage:                   "v4_0_real_node_foundation",
		RuntimeTruth:                   "v4_real_node_foundation",
		NodeID:                         cfg.NodeID,
		ShardID:                        cfg.ShardID,
		TotalInputTxs:                  total,
		AcceptedTxs:                    accepted,
		RejectedTxs:                    rejected,
		MempoolSize:                    size,
		RealSignedTx:                   true,
		RealNonceCheck:                 true,
		PerNodeMempool:                 true,
		RealTraceImport:                realTraceImport,
		RealP2P:                        false,
		RealPBFT:                       false,
		BlockProposer:                  false,
		StateCommit:                    false,
		CrossShardProtocol:             false,
		FrontendRealismMode:            false,
		NotBlockEmulatorReplacementYet: true,
		RunMode:                        cfg.RunMode,
	}
}
