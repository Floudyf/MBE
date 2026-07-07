package node

type FinalSummaryV43 struct {
	RuntimeStage                   string `json:"runtime_stage"`
	RuntimeTruth                   string `json:"runtime_truth"`
	RealSignedTx                   bool   `json:"real_signed_tx"`
	SenderPublicKeyBinding         bool   `json:"sender_public_key_binding"`
	SignedTxAuthenticity           bool   `json:"signed_tx_authenticity"`
	PerNodeMempool                 bool   `json:"per_node_mempool"`
	RealP2P                        bool   `json:"real_p2p"`
	PBFTStyleConsensus             bool   `json:"pbft_style_consensus"`
	RealPBFTMessages               bool   `json:"real_pbft_messages"`
	DeterministicExecution         bool   `json:"deterministic_execution"`
	PersistentStateDB              bool   `json:"persistent_state_db"`
	StateRootFromRealStateUpdates  bool   `json:"state_root_from_real_state_updates"`
	ReceiptDB                      bool   `json:"receipt_db"`
	TxIndex                        bool   `json:"tx_index"`
	RealCrossShardStateMachine     bool   `json:"real_cross_shard_state_machine"`
	RealCrossShardNetworkCommit    bool   `json:"real_cross_shard_network_commit"`
	DeterministicCertificateMVP    bool   `json:"deterministic_certificate_mvp"`
	RealFaultInjection             bool   `json:"real_fault_injection"`
	FaultInjectionSupported        bool   `json:"fault_injection_supported"`
	FaultEventCount                int    `json:"fault_event_count"`
	BlockEmulatorTraceToSignedTx   bool   `json:"blockemulator_trace_to_signed_tx"`
	BlockEmulatorImportedTxCount   int    `json:"blockemulator_imported_tx_count"`
	SignedTxVerifyPassCount        int    `json:"signed_tx_verify_pass_count"`
	BlockEmulatorV4RunCompleted    bool   `json:"blockemulator_v4_run_completed"`
	BlockEmulatorBridgeUpgraded    bool   `json:"blockemulator_bridge_upgraded"`
	FrontendRealismMode            bool   `json:"frontend_realism_mode"`
	FrontendE2EPass                bool   `json:"frontend_e2e_pass"`
	ResearchGradeRealEmulator      bool   `json:"research_grade_real_emulator"`
	ProductionPBFT                 bool   `json:"production_pbft"`
	FullByzantineSecurity          bool   `json:"full_byzantine_security"`
	ProductionBlockchain           bool   `json:"production_blockchain"`
	ProductionAtomicCommit         bool   `json:"production_atomic_commit"`
	ByzantineSecureRelay           bool   `json:"byzantine_secure_relay"`
	ByzantineFaultModel            bool   `json:"byzantine_fault_model"`
	ProductionFaultTolerance       bool   `json:"production_fault_tolerance"`
	FullBlockEmulatorCompatibility bool   `json:"full_blockemulator_compatibility"`
	CommittedHeight                uint64 `json:"committed_height"`
	CommittedBlockHash             string `json:"committed_block_hash"`
	LatestStateRoot                string `json:"latest_state_root"`
	StateRootMismatchCount         int    `json:"state_root_mismatch_count"`
	CrossShardTxCount              int    `json:"cross_shard_tx_count"`
	ReadyToCommit                  bool   `json:"ready_to_commit"`
}

type SmokeOptionsV43 struct {
	OutDir               string
	Nodes                int
	Shards               int
	TxCount              int
	EnableCrossShard     bool
	EnableFaults         bool
	FaultProfile         string
	BlockEmulatorCSV     string
	BlockEmulatorTxLimit int
	RunDurationMS        int
	FrontendAvailable    bool
	FrontendE2EPass      bool
}
