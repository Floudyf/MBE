package node

type RuntimeSummaryV41 struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	NodeID                         string `json:"node_id"`
	ShardID                        string `json:"shard_id"`
	ListenAddr                     string `json:"listen_addr"`
	Role                           string `json:"role"`
	RealSignedTx                   bool   `json:"real_signed_tx"`
	PerNodeMempool                 bool   `json:"per_node_mempool"`
	RealP2P                        bool   `json:"real_p2p"`
	TxGossip                       bool   `json:"tx_gossip"`
	BlockProposer                  bool   `json:"block_proposer"`
	PBFTStyleConsensus             bool   `json:"pbft_style_consensus"`
	RealPBFTMessages               bool   `json:"real_pbft_messages"`
	BlockCommit                    bool   `json:"block_commit"`
	StateCommit                    bool   `json:"state_commit"`
	CrossShardProtocol             bool   `json:"cross_shard_protocol"`
	FrontendRealismMode            bool   `json:"frontend_realism_mode"`
	ProductionPBFT                 bool   `json:"production_pbft"`
	FullByzantineSecurity          bool   `json:"full_byzantine_security"`
	BasicViewChange                bool   `json:"basic_view_change"`
	ProductionViewChangeProof      bool   `json:"production_view_change_proof"`
	Checkpoint                     bool   `json:"checkpoint"`
	StableLog                      bool   `json:"stable_log"`
	NotBlockEmulatorReplacementYet bool   `json:"not_blockemulator_replacement_yet"`
	CommittedHeight                uint64 `json:"committed_height"`
	CommittedBlockHash             string `json:"committed_block_hash"`
}
