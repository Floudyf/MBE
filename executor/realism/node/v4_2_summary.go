package node

type FinalSummaryV42 struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	RealSignedTx                   bool   `json:"real_signed_tx"`
	PerNodeMempool                 bool   `json:"per_node_mempool"`
	RealP2P                        bool   `json:"real_p2p"`
	PBFTStyleConsensus             bool   `json:"pbft_style_consensus"`
	RealPBFTMessages               bool   `json:"real_pbft_messages"`
	ProductionPBFT                 bool   `json:"production_pbft"`
	FullByzantineSecurity          bool   `json:"full_byzantine_security"`
	BlockCommit                    bool   `json:"block_commit"`
	DeterministicExecution         bool   `json:"deterministic_execution"`
	PersistentStateDB              bool   `json:"persistent_state_db"`
	StateRootFromRealStateUpdates  bool   `json:"state_root_from_real_state_updates"`
	EthereumMPTCompatible          bool   `json:"ethereum_mpt_compatible"`
	ProductionStatelessWitness     bool   `json:"production_stateless_witness"`
	WitnessMVP                     bool   `json:"witness_mvp"`
	ReceiptDB                      bool   `json:"receipt_db"`
	TxIndex                        bool   `json:"tx_index"`
	RealCrossShardStateMachine     bool   `json:"real_cross_shard_state_machine"`
	DeterministicCertificateMVP    bool   `json:"deterministic_certificate_mvp"`
	ByzantineSecureRelay           bool   `json:"byzantine_secure_relay"`
	ProductionAtomicCommit         bool   `json:"production_atomic_commit"`
	RecoverySupported              bool   `json:"recovery_supported"`
	NodeRecovery                   bool   `json:"node_recovery"`
	CrashConsistencyMVP            bool   `json:"crash_consistency_mvp"`
	ProductionRecovery             bool   `json:"production_recovery"`
	FaultInjectionSupported        bool   `json:"fault_injection_supported"`
	ByzantineFaultModel            bool   `json:"byzantine_fault_model"`
	ProductionFaultTolerance       bool   `json:"production_fault_tolerance"`
	FrontendRealismMode            bool   `json:"frontend_realism_mode"`
	BlockEmulatorBridgeMVP         bool   `json:"blockemulator_bridge_mvp"`
	FullBlockEmulatorCompatibility bool   `json:"full_blockemulator_compatibility"`
	ResearchGradeRealEmulator      bool   `json:"research_grade_real_emulator"`
	ProductionBlockchain           bool   `json:"production_blockchain"`
	CommittedHeight                uint64 `json:"committed_height"`
	CommittedBlockHash             string `json:"committed_block_hash"`
	LatestStateRoot                string `json:"latest_state_root"`
	StateRootMismatchCount         int    `json:"state_root_mismatch_count"`
	CrossShardTxCount              int    `json:"cross_shard_tx_count"`
	ReadyToCommit                  bool   `json:"ready_to_commit"`
}
