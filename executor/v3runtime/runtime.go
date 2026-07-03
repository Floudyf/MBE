package v3runtime

import (
	"crypto/sha256"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type Input struct {
	ChainProfilePath      string
	PluginProfilePath     string
	PluginProfileID       string
	ExperimentProfilePath string
	OutputDir             string
	RunID                 string
}

type ChainProfile struct {
	ProfileID              string
	NodeIDPrefix           string
	NodeCount              int
	ValidatorCount         int
	ConsensusDomainCount   int
	ConsensusBaseLatencyMS int
	BlockIntervalMS        int
	MaxTxPerBlock          int
	EmptyBlockEnabled      bool
	ShardCount             int
	ExecutionShardCount    int
	StateStorageUnitCount  int
	StatePlacementPolicy   string
	StateBackend           string
	RemoteFetchCostMS      int
	RoutingPlugin          string
	RoutingScope           string
	HotspotThreshold       int
	CoaccessWindow         int
	NetworkPlugin          string
	NetworkBaseDelayMS     int
	KeyCount               int
	MaxPoolSize            int
	DedupEnabled           bool
	BackpressurePolicy     string
}

type PluginProfile struct {
	ProfileID                string
	ShardingPlugin           string
	ExecutionSchedulerPlugin string
	StateAccessPlugin        string
	CommitPlugin             string
	TxPoolPlugin             string
	BlockProducer            string
	ConsensusPlugin          string
	ConsensusRuntimePlugin   string
	MetricsPlugin            string
}

type ExperimentProfile struct {
	ProfileID                    string
	Stage                        string
	Type                         string
	TruthLabel                   string
	BackendType                  string
	RuntimeMode                  string
	ChainProfileID               string
	TxCount                      int
	Seed                         int
	SubmitRate                   int
	KeyCount                     int
	HotKeyCount                  int
	HotspotRatio                 float64
	AccessListEnabled            bool
	AggregationCandidatesEnabled bool
	ShardCount                   int
	ValidatorsPerShard           int
	ExecutorsPerShard            int
	StorageNodesPerShard         int
	SupervisorEnabled            bool
	NodeRuntimeMode              string
	NetworkMode                  string
	NetworkAdapter               string
	CrossShardProtocol           string
}

type Transaction struct {
	ID           string
	SubmitTimeMS int
	ReadKeys     []string
	WriteDeltas  map[string]int
	Commutative  bool
	ConflictHint string
}

type Block struct {
	Height            int
	ID                string
	ParentHash        string
	Hash              string
	ProducerPlugin    string
	CutReason         string
	PoolSizeBeforeCut int
	PoolSizeAfterCut  int
	Txs               []PooledTransaction
	CutTimeMS         int
}

type BlockProducer struct {
	PluginID           string
	BlockIntervalMS    int
	MaxTxPerBlock      int
	EmptyBlockEnabled  bool
	ParentHash         string
	NextHeight         int
	LastCutTimeMS      int
	ProducedBlockCount int
	EmptyBlockCount    int
	CountCutCount      int
	TimeCutCount       int
	DrainCutCount      int
	EmptyCutCount      int
	BlockSizes         []int
	CutIntervals       []int
}

type PooledTransaction struct {
	Tx          Transaction
	AdmitTimeMS int
	QueueWaitMS int
}

type TxPoolEvent struct {
	EventTimeMS    int
	EventType      string
	TxID           string
	BlockHeight    int
	PoolSizeBefore int
	PoolSizeAfter  int
	AdmittedCount  int
	SelectedCount  int
	RejectedCount  int
	QueueWaitMS    int
	Reason         string
}

type ConsensusRecord struct {
	BlockHeight              int
	BlockHash                string
	PluginID                 string
	RoundID                  int
	ViewID                   int
	SequenceID               int
	LeaderID                 string
	ValidatorCount           int
	FaultToleranceF          int
	PrepareQuorum            int
	CommitQuorum             int
	PrePrepareMsgCount       int
	PrepareMsgCount          int
	CommitMsgCount           int
	TotalMessageCount        int
	ConsensusStartTimeMS     int
	ConsensusOrderedTimeMS   int
	ConsensusFinalizedTimeMS int
	ConsensusLatencyMS       int
	Finalized                bool
	ViewChangeCount          int
	Reason                   string
}

type ConsensusEngine struct {
	PluginID       string
	ValidatorCount int
	NodeIDPrefix   string
	BaseLatencyMS  int
}

type RoutingRecord struct {
	TxID                      string
	BlockHeight               int
	TxIndex                   int
	RoutingPlugin             string
	AccessKeyCount            int
	ReadKeyCount              int
	WriteKeyCount             int
	PrimaryShard              int
	TouchedShards             []int
	TouchedShardCount         int
	CrossShard                bool
	RemoteStateAccessEstimate int
	HotspotKeyCount           int
	CoaccessGroupID           string
	RoutingOverheadMS         int
	Reason                    string
}

type RoutingEngine struct {
	PluginID                  string
	ShardCount                int
	HotspotThreshold          int
	CoaccessWindow            int
	DecisionCount             int
	CrossShardDecisionCount   int
	LocalDecisionCount        int
	RemoteStateAccessEstimate int
	HotspotKeyCount           int
	CoaccessGroupCount        int
	Overheads                 []int
}

type ExecutionRecord struct {
	TxID                string
	BlockHeight         int
	TxIndex             int
	ExecutionPlugin     string
	Track               string
	AccessKeyCount      int
	ReadKeyCount        int
	WriteKeyCount       int
	DependencyEdgeCount int
	DependencyRisk      string
	ReadyAtMS           int
	StartAtMS           int
	EndAtMS             int
	ExecutionLatencyMS  int
	Blocked             bool
	BlockReason         string
	WorkerID            int
	Reason              string
}

type ExecutionEngine struct {
	PluginID                string
	Parallelism             int
	FastTrackThreshold      int
	DependencyEdgeCount     int
	FastTrackCount          int
	ConservativeTrackCount  int
	BlockedTxCount          int
	ExecutedTxCount         int
	ExecutionLatencyTotalMS int
	Latencies               []int
	ParallelizableTxCount   int
	SerialTxCount           int
}

type StateAccessRecord struct {
	TxID              string
	BlockHeight       int
	TxIndex           int
	StateAccessPlugin string
	AccessKey         string
	AccessType        string
	IsRead            bool
	IsWrite           bool
	HomeShard         int
	ExecutionShard    int
	IsRemote          bool
	CacheHit          bool
	Prefetched        bool
	WitnessEstimated  bool
	ProofEstimated    bool
	AccessLatencyMS   int
	Reason            string
}

type StateAccessEngine struct {
	PluginID                  string
	LocalLatencyMS            int
	RemoteLatencyMS           int
	CacheHitLatencyMS         int
	PrefetchLatencyMS         int
	Cache                     map[string]bool
	CacheHitCount             int
	CacheMissCount            int
	LocalAccessCount          int
	RemoteAccessCount         int
	PrefetchHitCount          int
	PrefetchMissCount         int
	StateAccessLatencyTotalMS int
	Latencies                 []int
	RemoteLatencies           []int
	WitnessEstimatedCount     int
	ProofEstimatedCount       int
	EstimatedWitnessBytes     int
	EstimatedProofBytes       int
}

type CommitEngine struct {
	PluginID                string
	CommitTxCount           int
	CommitUpdateCount       int
	NormalCommitCount       int
	ConservativeCommitCount int
	HotspotUpdateCount      int
	AggregatedUpdateCount   int
	RawUpdateCount          int
	AggregationGroupCount   int
	ConstraintCheckCount    int
	ConstraintPassedCount   int
	ConstraintFailedCount   int
	Latencies               []int
	aggregationGroups       map[string]bool
}

type CommitPlan struct {
	RawByKey map[string]int
}

type txPoolEntry struct {
	tx          Transaction
	admitTimeMS int
}

type TxPool struct {
	maxPoolSize   int
	dedupEnabled  bool
	backpressure  string
	queue         []txPoolEntry
	seen          map[string]bool
	events        []TxPoolEvent
	admittedCount int
	selectedCount int
	rejectedCount int
	peakSize      int
	queueWaits    []int
}

type TxResult struct {
	TxID                 string
	SubmitTimeMS         int
	AdmitTimeMS          int
	BlockHeight          int
	ExecutionStartMS     int
	ExecutionEndMS       int
	CommitTimeMS         int
	LatencyMS            int
	Status               string
	ShardID              int
	ConsensusDomainID    string
	ExecutionShardID     int
	HomeStateUnitIDs     []int
	AccessedStateUnitIDs []int
	RemoteStateUnitCount int
	CrossStateUnitAccess bool
	StateLocalityHit     bool
	ReadCount            int
	WriteCount           int
	RemoteFetchCount     int
	Track                string
	Deltas               map[string][3]int
}

type StateCommit struct {
	BlockHeight        int
	TxIndex            int
	TxID               string
	StateKey           string
	OldValue           int
	Delta              int
	NewValue           int
	CommitPlugin       string
	CommitPath         string
	UpdateType         string
	IsHotspot          bool
	Aggregated         bool
	AggregationGroupID string
	RawUpdateCount     int
	AggregatedCount    int
	ConstraintChecked  bool
	ConstraintPassed   bool
	CommitLatencyMS    int
	Reason             string
	CommitTimeMS       int
	Status             string
	StateStorageUnitID int
	ExecutionShardID   int
	IsRemoteCommit     bool
	PlacementPolicy    string
	RoutingPlugin      string
}

type Summary struct {
	RunID                          string  `json:"run_id"`
	Stage                          string  `json:"stage"`
	BackendType                    string  `json:"backend_type"`
	TruthLabel                     string  `json:"truth_label"`
	ChainProfileID                 string  `json:"chain_profile_id"`
	PluginProfileID                string  `json:"plugin_profile_id"`
	ExperimentProfileID            string  `json:"experiment_profile_id"`
	TxCount                        int     `json:"tx_count"`
	SuccessCount                   int     `json:"success_count"`
	FailureCount                   int     `json:"failure_count"`
	BlockCount                     int     `json:"block_count"`
	ThroughputTPS                  float64 `json:"throughput_tps"`
	AvgLatencyMS                   float64 `json:"avg_latency_ms"`
	P95LatencyMS                   float64 `json:"p95_latency_ms"`
	P99LatencyMS                   float64 `json:"p99_latency_ms"`
	RuntimeMode                    string  `json:"runtime_mode"`
	RemoteFetchCount               int     `json:"remote_fetch_count"`
	CrossShardRatio                float64 `json:"cross_shard_ratio"`
	FastTrackCount                 int     `json:"fast_track_count"`
	ConservativeTrackCount         int     `json:"conservative_track_count"`
	AggregatedUpdateCount          int     `json:"aggregated_update_count"`
	AggregationRatio               float64 `json:"aggregation_ratio"`
	ConflictCount                  int     `json:"conflict_count"`
	QueueWaitMS                    float64 `json:"queue_wait_ms"`
	TxPoolAdmittedCount            int     `json:"txpool_admitted_count"`
	TxPoolRejectedCount            int     `json:"txpool_rejected_count"`
	TxPoolPeakSize                 int     `json:"txpool_peak_size"`
	TxPoolAvgWaitMS                float64 `json:"txpool_avg_wait_ms"`
	TxPoolP95WaitMS                float64 `json:"txpool_p95_wait_ms"`
	EmptyBlockCount                int     `json:"empty_block_count"`
	AvgBlockSize                   float64 `json:"avg_block_size"`
	MaxBlockSize                   int     `json:"max_block_size"`
	BlockIntervalMS                int     `json:"block_interval_ms"`
	AvgBlockIntervalMS             float64 `json:"avg_block_interval_ms"`
	BlockProducerCountCut          int     `json:"blockproducer_count_cut_count"`
	BlockProducerTimeCut           int     `json:"blockproducer_time_cut_count"`
	BlockProducerDrainCut          int     `json:"blockproducer_drain_cut_count"`
	BlockProducerEmptyCut          int     `json:"blockproducer_empty_cut_count"`
	BlockCommitLatencyMS           float64 `json:"block_commit_latency_ms"`
	ConsensusLatencyMS             float64 `json:"consensus_latency_ms"`
	AvgConsensusLatencyMS          float64 `json:"avg_consensus_latency_ms"`
	P95ConsensusLatencyMS          float64 `json:"p95_consensus_latency_ms"`
	ConsensusMessageCount          int     `json:"consensus_message_count"`
	AvgConsensusMessageCount       float64 `json:"avg_consensus_message_count"`
	ConsensusRoundCount            int     `json:"consensus_round_count"`
	ViewChangeCount                int     `json:"view_change_count"`
	FinalizedBlockCount            int     `json:"finalized_block_count"`
	FailedBlockCount               int     `json:"failed_block_count"`
	RoutingDecisionCount           int     `json:"routing_decision_count"`
	CrossShardTxCount              int     `json:"cross_shard_tx_count"`
	LocalTxCount                   int     `json:"local_tx_count"`
	RemoteStateAccessCount         int     `json:"remote_state_access_count"`
	AvgTouchedShards               float64 `json:"avg_touched_shards"`
	MaxTouchedShards               int     `json:"max_touched_shards"`
	HotspotKeyCount                int     `json:"hotspot_key_count"`
	CoaccessGroupCount             int     `json:"coaccess_group_count"`
	AvgRoutingOverheadMS           float64 `json:"avg_routing_overhead_ms"`
	RoutingPlugin                  string  `json:"routing_plugin"`
	ExecutionPlugin                string  `json:"execution_plugin"`
	ExecutionTxCount               int     `json:"execution_tx_count"`
	BlockedTxCount                 int     `json:"blocked_tx_count"`
	DependencyEdgeCount            int     `json:"dependency_edge_count"`
	AvgDependencyEdgesPerTx        float64 `json:"avg_dependency_edges_per_tx"`
	AvgExecutionLatencyMS          float64 `json:"avg_execution_latency_ms"`
	P95ExecutionLatencyMS          float64 `json:"p95_execution_latency_ms"`
	MaxExecutionLatencyMS          int     `json:"max_execution_latency_ms"`
	LogicalWorkerCount             int     `json:"logical_worker_count"`
	ParallelizableTxCount          int     `json:"parallelizable_tx_count"`
	SerialTxCount                  int     `json:"serial_tx_count"`
	StateAccessPlugin              string  `json:"state_access_plugin"`
	StateAccessCount               int     `json:"state_access_count"`
	LocalStateAccessCount          int     `json:"local_state_access_count"`
	RemoteStateAccessRatio         float64 `json:"remote_state_access_ratio"`
	CacheHitCount                  int     `json:"cache_hit_count"`
	CacheMissCount                 int     `json:"cache_miss_count"`
	CacheHitRate                   float64 `json:"cache_hit_rate"`
	PrefetchHitCount               int     `json:"prefetch_hit_count"`
	PrefetchMissCount              int     `json:"prefetch_miss_count"`
	PrefetchHitRate                float64 `json:"prefetch_hit_rate"`
	AvgStateAccessLatencyMS        float64 `json:"avg_state_access_latency_ms"`
	P95StateAccessLatencyMS        float64 `json:"p95_state_access_latency_ms"`
	MaxStateAccessLatencyMS        int     `json:"max_state_access_latency_ms"`
	RemoteStateAccessLatencyMS     float64 `json:"remote_state_access_latency_ms"`
	WitnessEstimatedCount          int     `json:"witness_estimated_count"`
	ProofEstimatedCount            int     `json:"proof_estimated_count"`
	EstimatedWitnessBytes          int     `json:"estimated_witness_bytes"`
	EstimatedProofBytes            int     `json:"estimated_proof_bytes"`
	CommitPlugin                   string  `json:"commit_plugin"`
	CommitTxCount                  int     `json:"commit_tx_count"`
	CommitUpdateCount              int     `json:"commit_update_count"`
	NormalCommitCount              int     `json:"normal_commit_count"`
	ConservativeCommitCount        int     `json:"conservative_commit_count"`
	HotspotUpdateCount             int     `json:"hotspot_update_count"`
	RawUpdateCount                 int     `json:"raw_update_count"`
	AggregationGroupCount          int     `json:"aggregation_group_count"`
	ConstraintCheckCount           int     `json:"constraint_check_count"`
	ConstraintPassedCount          int     `json:"constraint_passed_count"`
	ConstraintFailedCount          int     `json:"constraint_failed_count"`
	AvgCommitLatencyMS             float64 `json:"avg_commit_latency_ms"`
	P95CommitLatencyMS             float64 `json:"p95_commit_latency_ms"`
	MaxCommitLatencyMS             int     `json:"max_commit_latency_ms"`
	ExecutionShardCount            int     `json:"execution_shard_count"`
	StateStorageUnitCount          int     `json:"state_storage_unit_count"`
	CrossStateUnitAccessCount      int     `json:"cross_state_unit_access_count"`
	RemoteStateFetchCount          int     `json:"remote_state_fetch_count"`
	StateLocalityRatio             float64 `json:"state_locality_ratio"`
	ExecutionShardLoadBalance      float64 `json:"execution_shard_load_balance"`
	StateUnitLoadBalance           float64 `json:"state_unit_load_balance"`
	ShardCount                     int     `json:"shard_count"`
	ValidatorsPerShard             int     `json:"validators_per_shard"`
	LogicalNodeCount               int     `json:"logical_node_count"`
	ValidatorNodeCount             int     `json:"validator_node_count"`
	ExecutorNodeCount              int     `json:"executor_node_count"`
	StorageNodeCount               int     `json:"storage_node_count"`
	SupervisorNodeCount            int     `json:"supervisor_node_count"`
	MessageCount                   int     `json:"message_count"`
	NetworkMessageCount            int     `json:"network_message_count"`
	NodeEventCount                 int     `json:"node_event_count"`
	LauncherMode                   string  `json:"launcher_mode"`
	LauncherScriptCount            int     `json:"launcher_script_count"`
	LaunchableNodeCount            int     `json:"launchable_node_count"`
	NodeAddressCount               int     `json:"node_address_count"`
	WindowsLauncherAvailable       bool    `json:"windows_launcher_available"`
	LinuxLauncherAvailable         bool    `json:"linux_launcher_available"`
	LauncherPreviewOnly            bool    `json:"launcher_preview_only"`
	NodeProcessEntrypointAvailable bool    `json:"node_process_entrypoint_available"`
	NodeProcessPreviewAvailable    bool    `json:"node_process_preview_available"`
	NodeProcessStatusAvailable     bool    `json:"node_process_status_available"`
	NodeProcessManifestAvailable   bool    `json:"node_process_manifest_available"`
	NodeProcessPreviewOnly         bool    `json:"node_process_preview_only"`
	NetworkAdapterSelected         string  `json:"network_adapter_selected"`
	TCPPreviewEnabled              bool    `json:"tcp_preview_enabled"`
	TCPListenNodeCount             int     `json:"tcp_listen_node_count"`
	TCPSendCount                   int     `json:"tcp_send_count"`
	TCPReceiveCount                int     `json:"tcp_receive_count"`
	TypedMessageCount              int     `json:"typed_message_count"`
	NetworkErrorCount              int     `json:"network_error_count"`
	ConsensusOverNetworkEnabled    bool    `json:"consensus_over_network_enabled"`
	ConsensusRuntimeSelected       string  `json:"consensus_runtime_selected"`
	ProposalPreviewCount           int     `json:"proposal_preview_count"`
	VotePreviewCount               int     `json:"vote_preview_count"`
	LightQuorumReachedCount        int     `json:"light_quorum_reached_count"`
	ConsensusNetworkErrorCount     int     `json:"consensus_network_error_count"`
	ConsensusNetworkPath           string  `json:"consensus_network_path"`
	PBFTView                       int     `json:"pbft_view"`
	PBFTSequence                   int     `json:"pbft_sequence"`
	PBFTPrePrepareCount            int     `json:"pbft_preprepare_count"`
	PBFTPrepareCount               int     `json:"pbft_prepare_count"`
	PBFTCommitCount                int     `json:"pbft_commit_count"`
	PBFTQuorumReachedCount         int     `json:"pbft_quorum_reached_count"`
	PBFTFinalizedBlockCount        int     `json:"pbft_finalized_block_count"`
	PBFTConsensusLatencyMS         int     `json:"pbft_consensus_latency_ms"`
	PBFTPreviewEnabled             bool    `json:"pbft_preview_enabled"`
	PBFTQuorumThreshold            int     `json:"pbft_quorum_threshold"`
	PBFTOverNetworkEnabled         bool    `json:"pbft_over_network_enabled"`
	PBFTNetworkPath                string  `json:"pbft_network_path"`
	PBFTNetworkMessageCount        int     `json:"pbft_network_message_count"`
	PBFTNetworkErrorCount          int     `json:"pbft_network_error_count"`
	PBFTPrePrepareNetworkCount     int     `json:"pbft_preprepare_network_count"`
	PBFTPrepareNetworkCount        int     `json:"pbft_prepare_network_count"`
	PBFTCommitNetworkCount         int     `json:"pbft_commit_network_count"`
	PBFTFinalizedNetworkCount      int     `json:"pbft_finalized_network_count"`
	PBFTNetworkQuorumReachedCount  int     `json:"pbft_network_quorum_reached_count"`
	CrossShardProtocolSelected     string  `json:"cross_shard_protocol_selected"`
	CrossShardMessageCount         int     `json:"cross_shard_message_count"`
	RelayPreviewCount              int     `json:"relay_preview_count"`
	CrossShardCompletedCount       int     `json:"cross_shard_completed_count"`
	CrossShardFailedCount          int     `json:"cross_shard_failed_count"`
	CrossShardAvgLatencyMS         float64 `json:"cross_shard_avg_latency_ms"`
}

type Result struct {
	OutputDir      string
	Summary        Summary
	BlockLog       []map[string]string
	TxResults      []TxResult
	StateCommitLog []StateCommit
	TxPoolLog      []TxPoolEvent
	ConsensusLog   []ConsensusRecord
	RoutingLog     []RoutingRecord
	ExecutionLog   []ExecutionRecord
	StateAccessLog []StateAccessRecord
	NodeRuntime    NodeRuntimeArtifacts
	Launcher       LauncherPreview
	NetworkAdapter NetworkAdapterPreview
	ConsensusNet   ConsensusNetworkLightPreview
	PBFTPreview    PBFTPreview
	PBFTNetwork    PBFTNetworkPreview
	CrossShard     CrossShardProtocolPreview
	FinalState     map[string]int
}

func Run(input Input) (Result, error) {
	if input.OutputDir == "" {
		return Result{}, fmt.Errorf("output directory is required")
	}
	chainBytes, err := os.ReadFile(input.ChainProfilePath)
	if err != nil {
		return Result{}, err
	}
	pluginBytes, err := os.ReadFile(input.PluginProfilePath)
	if err != nil {
		return Result{}, err
	}
	experimentBytes, err := os.ReadFile(input.ExperimentProfilePath)
	if err != nil {
		return Result{}, err
	}
	chain := parseChainProfile(string(chainBytes))
	plugin := parsePluginProfile(string(pluginBytes), input.PluginProfileID)
	experiment := parseExperimentProfile(string(experimentBytes))
	if plugin.ProfileID == "" {
		return Result{}, fmt.Errorf("plugin profile not found: %s", input.PluginProfileID)
	}
	if err := requireSupportedPlugins(plugin); err != nil {
		return Result{}, err
	}
	runID := input.RunID
	if runID == "" {
		runID = "v3go_run"
	}
	txs := generateWorkload(experiment, chain)
	txPool := newTxPool(chain)
	blockProducer := newBlockProducer(chain, plugin)
	consensusEngine := newConsensusEngine(chain, plugin)
	routingEngine := newRoutingEngine(chain, plugin)
	executionEngine := newExecutionEngine(chain, plugin)
	stateAccessEngine := newStateAccessEngine(chain, plugin)
	commitEngine := newCommitEngine(plugin)
	blocks := produceBlocksFromTxPool(txs, chain, txPool, blockProducer)
	state := map[string]int{}
	blockLog := []map[string]string{}
	txResults := []TxResult{}
	stateCommits := []StateCommit{}
	consensusLog := []ConsensusRecord{}
	routingLog := []RoutingRecord{}
	executionLog := []ExecutionRecord{}
	stateAccessLog := []StateAccessRecord{}
	for _, block := range blocks {
		consensusRecord := consensusEngine.FinalizeBlock(block)
		consensusLog = append(consensusLog, consensusRecord)
		blockLog = append(blockLog, map[string]string{
			"block_height":             strconv.Itoa(block.Height),
			"block_id":                 block.ID,
			"parent_hash":              block.ParentHash,
			"block_hash":               block.Hash,
			"proposer":                 consensusRecord.LeaderID,
			"proposer_node":            consensusRecord.LeaderID,
			"tx_count":                 strconv.Itoa(len(block.Txs)),
			"cut_reason":               block.CutReason,
			"pool_size_before_cut":     strconv.Itoa(block.PoolSizeBeforeCut),
			"pool_size_after_cut":      strconv.Itoa(block.PoolSizeAfterCut),
			"block_producer_plugin":    block.ProducerPlugin,
			"cut_time_ms":              strconv.Itoa(block.CutTimeMS),
			"ordered_time_ms":          strconv.Itoa(consensusRecord.ConsensusOrderedTimeMS),
			"finalized_time_ms":        strconv.Itoa(consensusRecord.ConsensusFinalizedTimeMS),
			"consensus_plugin":         consensusRecord.PluginID,
			"status":                   "finalized",
			"consensus_domain_id":      consensusDomainID(0),
			"validator_count":          strconv.Itoa(consensusRecord.ValidatorCount),
			"execution_shard_count":    strconv.Itoa(chain.ExecutionShardCount),
			"state_storage_unit_count": strconv.Itoa(chain.StateStorageUnitCount),
		})
		cursor := consensusRecord.ConsensusFinalizedTimeMS
		blockRoutingRecords := routingEngine.RouteBlock(block)
		routingByTx := map[string]RoutingRecord{}
		for _, record := range blockRoutingRecords {
			routingLog = append(routingLog, record)
			routingByTx[record.TxID] = record
		}
		blockExecutionRecords := executionEngine.ExecuteBlock(block, consensusRecord.ConsensusFinalizedTimeMS)
		executionByTx := map[string]ExecutionRecord{}
		for _, record := range blockExecutionRecords {
			executionLog = append(executionLog, record)
			executionByTx[record.TxID] = record
		}
		blockPrefetchSet := stateAccessEngine.PrefetchSet(block)
		commitPlan := commitEngine.PlanBlock(block)
		for txIndex, pooledTx := range block.Txs {
			tx := pooledTx.Tx
			routingRecord := routingByTx[tx.ID]
			if routingRecord.TxID == "" {
				routingRecord = routingEngine.RouteTransaction(tx, block.Height, txIndex, nil, nil)
				routingLog = append(routingLog, routingRecord)
			}
			executionRecord := executionByTx[tx.ID]
			if executionRecord.TxID == "" {
				executionRecord = executionEngine.ExecuteTransaction(tx, block.Height, txIndex, consensusRecord.ConsensusFinalizedTimeMS, 0)
				executionLog = append(executionLog, executionRecord)
			}
			executionShardID := routingRecord.PrimaryShard
			stateAccessRecords := stateAccessEngine.AccessTransaction(tx, block.Height, txIndex, executionShardID, blockPrefetchSet, chain)
			stateAccessLog = append(stateAccessLog, stateAccessRecords...)
			accessedUnits := accessedStateUnits(tx, chain)
			homeUnits := writeStateUnits(tx, chain)
			remoteAccesses := stateAccessRemoteCount(stateAccessRecords)
			remoteStateUnits := remoteStateUnitCount(accessedUnits, executionShardID)
			if remoteAccesses > 0 {
				remoteStateUnits = remoteAccesses
			}
			start := executionRecord.StartAtMS
			end := executionRecord.EndAtMS + stateAccessLatencyForTx(stateAccessRecords)
			commitTime := end + 1
			deltas := map[string][3]int{}
			for key, delta := range tx.WriteDeltas {
				oldValue := state[key]
				newValue := oldValue + delta
				deltas[key] = [3]int{oldValue, delta, newValue}
			}
			result := TxResult{
				TxID:                 tx.ID,
				SubmitTimeMS:         tx.SubmitTimeMS,
				AdmitTimeMS:          pooledTx.AdmitTimeMS,
				BlockHeight:          block.Height,
				ExecutionStartMS:     start,
				ExecutionEndMS:       end,
				CommitTimeMS:         commitTime,
				LatencyMS:            commitTime - tx.SubmitTimeMS,
				Status:               "success",
				ShardID:              executionShardID,
				ConsensusDomainID:    consensusDomainID(0),
				ExecutionShardID:     executionShardID,
				HomeStateUnitIDs:     homeUnits,
				AccessedStateUnitIDs: accessedUnits,
				RemoteStateUnitCount: remoteStateUnits,
				CrossStateUnitAccess: len(accessedUnits) > 1,
				StateLocalityHit:     remoteStateUnits == 0,
				ReadCount:            len(tx.ReadKeys),
				WriteCount:           len(tx.WriteDeltas),
				RemoteFetchCount:     stateAccessRemoteFetchCount(stateAccessRecords, plugin),
				Track:                executionRecord.Track,
				Deltas:               deltas,
			}
			txResults = append(txResults, result)
			stateCommits = append(stateCommits, commitEngine.CommitTransaction(tx, block.Height, txIndex, executionShardID, commitTime, deltas, state, chain, plugin, commitPlan)...)
			if end > cursor {
				cursor = end
			}
		}
	}
	summary := buildSummary(runID, experiment, chain, plugin.ProfileID, txResults, len(blocks), runtimeMode(plugin))
	applyMechanismMetrics(&summary, txResults, plugin, chain)
	applyTxPoolMetrics(&summary, txPool)
	applyBlockProducerMetrics(&summary, blockProducer)
	applyConsensusMetrics(&summary, consensusLog)
	applyRoutingMetrics(&summary, routingEngine, routingLog)
	applyExecutionMetrics(&summary, executionEngine, executionLog)
	applyStateAccessMetrics(&summary, stateAccessEngine)
	applyCommitMetrics(&summary, commitEngine)
	nodeRuntime := BuildLogicalNodeArtifacts(topologyFromExperiment(experiment), blocks, consensusLog)
	launcher := BuildLauncherPreview(nodeRuntime)
	networkAdapter := RunNetworkAdapterPreview(launcher)
	consensusRuntime := firstNonEmpty(plugin.ConsensusRuntimePlugin, plugin.ConsensusPlugin)
	consensusNetwork := RunConsensusNetworkLightPreview(nodeRuntime, launcher, networkAdapter, consensusLog, consensusRuntime)
	networkAdapter.AppendConsensusNetworkLight(consensusNetwork)
	pbftPreview := RunPBFTPreview(nodeRuntime, consensusLog, consensusRuntime)
	pbftNetwork := RunPBFTNetworkPreview(nodeRuntime, launcher, networkAdapter, pbftPreview)
	networkAdapter.AppendPBFTNetwork(pbftNetwork)
	crossShard := RunCrossShardProtocolPreview(experiment, networkAdapter, txResults, routingLog)
	networkAdapter.AppendCrossShardProtocol(crossShard)
	applyNodeRuntimeMetrics(&summary, nodeRuntime)
	applyLauncherPreviewMetrics(&summary, launcher)
	applyNodeProcessPreviewMetrics(&summary)
	applyNetworkAdapterMetrics(&summary, networkAdapter)
	applyConsensusNetworkLightMetrics(&summary, consensusNetwork)
	applyPBFTPreviewMetrics(&summary, pbftPreview)
	applyPBFTNetworkMetrics(&summary, pbftNetwork)
	applyCrossShardProtocolMetrics(&summary, crossShard)
	if err := writeArtifacts(input.OutputDir, chainBytes, pluginBytes, experimentBytes, summary, blockLog, txResults, stateCommits, txPool.events, consensusLog, routingLog, executionLog, stateAccessLog, nodeRuntime, launcher, networkAdapter, consensusNetwork, pbftPreview, pbftNetwork, crossShard, "V3.8 CrossShardProtocol Skeleton Closure run"); err != nil {
		return Result{}, err
	}
	return Result{OutputDir: input.OutputDir, Summary: summary, BlockLog: blockLog, TxResults: txResults, StateCommitLog: stateCommits, TxPoolLog: txPool.events, ConsensusLog: consensusLog, RoutingLog: routingLog, ExecutionLog: executionLog, StateAccessLog: stateAccessLog, NodeRuntime: nodeRuntime, Launcher: launcher, NetworkAdapter: networkAdapter, ConsensusNet: consensusNetwork, PBFTPreview: pbftPreview, PBFTNetwork: pbftNetwork, CrossShard: crossShard, FinalState: state}, nil
}

func parseChainProfile(text string) ChainProfile {
	executionShardCount := sectionFieldInt(text, "execution", "shard_count", sectionFieldInt(text, "sharding", "shard_count", fieldInt(text, "shard_count", 4)))
	stateStorageUnitCount := sectionFieldInt(text, "state", "storage_unit_count", sectionFieldInt(text, "sharding", "shard_count", executionShardCount))
	return ChainProfile{
		ProfileID:              fieldString(text, "profile_id", "chain_x_default"),
		NodeIDPrefix:           fieldString(text, "node_id_prefix", "node"),
		NodeCount:              sectionFieldInt(text, "deployment", "node_count", fieldInt(text, "node_count", 4)),
		ValidatorCount:         sectionFieldInt(text, "consensus", "validator_count", sectionFieldInt(text, "deployment", "validator_count", fieldInt(text, "validator_count", 4))),
		ConsensusDomainCount:   sectionFieldInt(text, "consensus", "domain_count", 1),
		ConsensusBaseLatencyMS: sectionFieldInt(text, "consensus", "base_latency_ms", 1),
		BlockIntervalMS:        fieldInt(text, "block_interval_ms", 100),
		MaxTxPerBlock:          fieldInt(text, "max_tx_per_block", 500),
		EmptyBlockEnabled:      sectionFieldBool(text, "block", "empty_block_enabled", fieldBool(text, "empty_block_enabled", false)),
		ShardCount:             executionShardCount,
		ExecutionShardCount:    executionShardCount,
		StateStorageUnitCount:  stateStorageUnitCount,
		StatePlacementPolicy:   sectionFieldString(text, "state", "placement_policy", "hash_state_storage"),
		StateBackend:           sectionFieldString(text, "state", "backend", "memory"),
		RemoteFetchCostMS:      sectionFieldInt(text, "state", "remote_fetch_cost_ms", 1),
		RoutingPlugin:          sectionFieldString(text, "routing", "plugin", sectionFieldString(text, "sharding", "plugin", "hash_sharding")),
		RoutingScope:           sectionFieldString(text, "routing", "routing_scope", "execution_shard"),
		HotspotThreshold:       sectionFieldInt(text, "routing", "hotspot_threshold", 2),
		CoaccessWindow:         sectionFieldInt(text, "routing", "coaccess_window", 1),
		NetworkPlugin:          sectionFieldString(text, "network", "plugin", "fixed_delay"),
		NetworkBaseDelayMS:     sectionFieldInt(text, "network", "base_delay_ms", sectionFieldInt(text, "network", "delay_ms", 0)),
		KeyCount:               fieldInt(text, "key_count", 100000),
		MaxPoolSize:            fieldInt(text, "max_pool_size", 100000),
		DedupEnabled:           fieldBool(text, "dedup_enabled", true),
		BackpressurePolicy:     fieldString(text, "backpressure_policy", "reject"),
	}
}

func parsePluginProfile(text, profileID string) PluginProfile {
	block := text
	if strings.Contains(text, "plugin_profile_collection") && profileID != "" {
		block = collectionItem(text, profileID)
	}
	return PluginProfile{
		ProfileID:                firstNonEmpty(fieldString(block, "plugin_profile_id", ""), profileID),
		ShardingPlugin:           fieldString(block, "ShardingPlugin", ""),
		ExecutionSchedulerPlugin: fieldString(block, "ExecutionSchedulerPlugin", ""),
		StateAccessPlugin:        fieldString(block, "StateAccessPlugin", ""),
		CommitPlugin:             fieldString(block, "CommitPlugin", ""),
		TxPoolPlugin:             fieldString(block, "TxPoolPlugin", "fifo_pool"),
		BlockProducer:            fieldString(block, "BlockProducer", "time_or_count_block_producer"),
		ConsensusPlugin:          fieldString(block, "ConsensusPlugin", "simple_leader"),
		ConsensusRuntimePlugin:   fieldString(block, "ConsensusRuntimePlugin", fieldString(block, "ConsensusPlugin", "simple_leader")),
		MetricsPlugin:            fieldString(block, "MetricsPlugin", "basic_metrics"),
	}
}

func parseExperimentProfile(text string) ExperimentProfile {
	return ExperimentProfile{
		ProfileID:                    fieldString(text, "profile_id", ""),
		Stage:                        fieldString(text, "stage", "v3.2"),
		Type:                         fieldString(text, "type", ""),
		TruthLabel:                   fieldString(text, "truth_label", "modular_runtime"),
		BackendType:                  fieldString(text, "backend_type", "modular_research_chain"),
		RuntimeMode:                  fieldString(text, "runtime_mode", "go_backed"),
		ChainProfileID:               fieldString(text, "chain_profile", "chain_x_default"),
		TxCount:                      fieldInt(text, "tx_count", 24),
		Seed:                         fieldInt(text, "seed", 42),
		SubmitRate:                   fieldInt(text, "submit_rate", 120),
		KeyCount:                     fieldInt(text, "key_count", 32),
		HotKeyCount:                  fieldInt(text, "hot_key_count", 4),
		HotspotRatio:                 fieldFloat(text, "hotspot_ratio", 0.25),
		AccessListEnabled:            fieldBool(text, "access_list_enabled", true),
		AggregationCandidatesEnabled: fieldBool(text, "aggregation_candidates_enabled", false),
		ShardCount:                   fieldInt(text, "shard_count", 4),
		ValidatorsPerShard:           fieldInt(text, "validators_per_shard", 4),
		ExecutorsPerShard:            fieldInt(text, "executors_per_shard", 1),
		StorageNodesPerShard:         fieldInt(text, "storage_nodes_per_shard", 1),
		SupervisorEnabled:            fieldBool(text, "supervisor_enabled", true),
		NodeRuntimeMode:              fieldString(text, "node_runtime_mode", "logical_single_process"),
		NetworkMode:                  fieldString(text, "network_mode", "in_memory_message_bus"),
		NetworkAdapter:               fieldString(text, "network_adapter", fieldString(text, "network_mode", "in_memory_message_bus")),
		CrossShardProtocol:           fieldString(text, "cross_shard_protocol", sectionFieldString(text, "cross_shard", "protocol", CrossShardProtocolNone)),
	}
}

func newConsensusEngine(chain ChainProfile, plugin PluginProfile) *ConsensusEngine {
	validatorCount := chain.ValidatorCount
	if validatorCount <= 0 {
		validatorCount = 4
	}
	baseLatencyMS := chain.ConsensusBaseLatencyMS
	if baseLatencyMS <= 0 {
		baseLatencyMS = 1
	}
	return &ConsensusEngine{
		PluginID:       plugin.ConsensusPlugin,
		ValidatorCount: validatorCount,
		NodeIDPrefix:   firstNonEmpty(chain.NodeIDPrefix, "node"),
		BaseLatencyMS:  baseLatencyMS,
	}
}

func (engine *ConsensusEngine) FinalizeBlock(block Block) ConsensusRecord {
	validatorCount := max(1, engine.ValidatorCount)
	viewID := 0
	leaderIndex := viewID % validatorCount
	leaderID := fmt.Sprintf("%s_%d", engine.NodeIDPrefix, leaderIndex)
	record := ConsensusRecord{
		BlockHeight:          block.Height,
		BlockHash:            block.Hash,
		PluginID:             engine.PluginID,
		RoundID:              block.Height,
		ViewID:               viewID,
		SequenceID:           block.Height,
		LeaderID:             leaderID,
		ValidatorCount:       validatorCount,
		ConsensusStartTimeMS: block.CutTimeMS,
		Finalized:            true,
		ViewChangeCount:      0,
	}
	switch engine.PluginID {
	case "poa_light":
		latency := max(1, engine.BaseLatencyMS)
		record.ConsensusOrderedTimeMS = block.CutTimeMS + latency
		record.ConsensusFinalizedTimeMS = record.ConsensusOrderedTimeMS + 1
		record.ConsensusLatencyMS = record.ConsensusFinalizedTimeMS - record.ConsensusStartTimeMS
		record.TotalMessageCount = validatorCount + 1
		record.Reason = "authority_confirmed"
	case "pbft_light_model":
		f := (validatorCount - 1) / 3
		prepareQuorum := 2*f + 1
		commitQuorum := 2*f + 1
		prePrepareCount := max(0, validatorCount-1)
		prepareCount := validatorCount * max(0, validatorCount-1)
		commitCount := validatorCount * max(0, validatorCount-1)
		record.FaultToleranceF = f
		record.PrepareQuorum = prepareQuorum
		record.CommitQuorum = commitQuorum
		record.PrePrepareMsgCount = prePrepareCount
		record.PrepareMsgCount = prepareCount
		record.CommitMsgCount = commitCount
		record.TotalMessageCount = prePrepareCount + prepareCount + commitCount
		record.ConsensusOrderedTimeMS = block.CutTimeMS + max(1, engine.BaseLatencyMS)*2
		record.ConsensusFinalizedTimeMS = block.CutTimeMS + max(1, engine.BaseLatencyMS)*3
		record.ConsensusLatencyMS = record.ConsensusFinalizedTimeMS - record.ConsensusStartTimeMS
		record.Reason = "pbft_light_quorum_reached"
	default:
		record.PluginID = "simple_leader"
		record.ConsensusOrderedTimeMS = block.CutTimeMS + 1
		record.ConsensusFinalizedTimeMS = record.ConsensusOrderedTimeMS + 1
		record.ConsensusLatencyMS = record.ConsensusFinalizedTimeMS - record.ConsensusStartTimeMS
		record.Reason = "simple_leader_finalized"
	}
	return record
}

func newRoutingEngine(chain ChainProfile, plugin PluginProfile) *RoutingEngine {
	pluginID := normalizeRoutingPlugin(firstNonEmpty(plugin.ShardingPlugin, chain.RoutingPlugin, "hash_sharding"))
	threshold := chain.HotspotThreshold
	if threshold <= 0 {
		threshold = 2
	}
	return &RoutingEngine{
		PluginID:         pluginID,
		ShardCount:       max(1, chain.ExecutionShardCount),
		HotspotThreshold: threshold,
		CoaccessWindow:   max(1, chain.CoaccessWindow),
	}
}

func (engine *RoutingEngine) RouteBlock(block Block) []RoutingRecord {
	keyFrequency := map[string]int{}
	coaccessGroups := map[string]string{}
	groupShard := map[string]int{}
	if engine.PluginID == "hotspot_aware_routing" || engine.PluginID == "metatrack_coaccess_routing" {
		for _, pooledTx := range block.Txs {
			for _, key := range transactionAccessKeys(pooledTx.Tx) {
				keyFrequency[key]++
			}
		}
	}
	if engine.PluginID == "metatrack_coaccess_routing" {
		nextGroup := 0
		for _, pooledTx := range block.Txs {
			keys := transactionAccessKeys(pooledTx.Tx)
			if len(keys) < 2 {
				continue
			}
			sort.Strings(keys)
			groupID := "coaccess_" + strconv.Itoa(nextGroup)
			nextGroup++
			shardID := shard(strings.Join(keys, "|"), engine.ShardCount)
			groupShard[groupID] = shardID
			for _, key := range keys {
				if coaccessGroups[key] == "" {
					coaccessGroups[key] = groupID
				}
			}
		}
		engine.CoaccessGroupCount += len(groupShard)
	}
	records := []RoutingRecord{}
	metadata := map[string]any{"coaccess_groups": coaccessGroups, "group_shard": groupShard}
	for index, pooledTx := range block.Txs {
		records = append(records, engine.RouteTransaction(pooledTx.Tx, block.Height, index, keyFrequency, metadata))
	}
	return records
}

func (engine *RoutingEngine) RouteTransaction(tx Transaction, blockHeight, txIndex int, keyFrequency map[string]int, metadata map[string]any) RoutingRecord {
	keys := transactionAccessKeys(tx)
	touched := []int{}
	seenShard := map[int]bool{}
	hotspotCount := 0
	coaccessGroupID := ""
	for _, key := range keys {
		shardID := engine.routeKey(key, keyFrequency, metadata)
		if engine.PluginID == "hotspot_aware_routing" && keyFrequency != nil && keyFrequency[key] >= engine.HotspotThreshold {
			hotspotCount++
		}
		if engine.PluginID == "metatrack_coaccess_routing" && metadata != nil {
			if groups, ok := metadata["coaccess_groups"].(map[string]string); ok && groups[key] != "" && coaccessGroupID == "" {
				coaccessGroupID = groups[key]
			}
		}
		if !seenShard[shardID] {
			seenShard[shardID] = true
			touched = append(touched, shardID)
		}
	}
	if len(touched) == 0 {
		touched = []int{shard(tx.ID, engine.ShardCount)}
	}
	sort.Ints(touched)
	primaryShard := touched[0]
	if len(keys) > 0 {
		primaryShard = engine.routeKey(primaryRoutingKey(tx), keyFrequency, metadata)
	}
	remoteEstimate := max(0, len(touched)-1)
	overhead := routingOverhead(engine.PluginID, len(keys), hotspotCount, coaccessGroupID)
	record := RoutingRecord{
		TxID:                      tx.ID,
		BlockHeight:               blockHeight,
		TxIndex:                   txIndex,
		RoutingPlugin:             engine.PluginID,
		AccessKeyCount:            len(keys),
		ReadKeyCount:              len(tx.ReadKeys),
		WriteKeyCount:             len(tx.WriteDeltas),
		PrimaryShard:              primaryShard,
		TouchedShards:             touched,
		TouchedShardCount:         len(touched),
		CrossShard:                len(touched) > 1,
		RemoteStateAccessEstimate: remoteEstimate,
		HotspotKeyCount:           hotspotCount,
		CoaccessGroupID:           coaccessGroupID,
		RoutingOverheadMS:         overhead,
		Reason:                    routingReason(engine.PluginID, coaccessGroupID, hotspotCount),
	}
	engine.DecisionCount++
	if record.CrossShard {
		engine.CrossShardDecisionCount++
	} else {
		engine.LocalDecisionCount++
	}
	engine.RemoteStateAccessEstimate += remoteEstimate
	engine.HotspotKeyCount += hotspotCount
	engine.Overheads = append(engine.Overheads, overhead)
	return record
}

func (engine *RoutingEngine) routeKey(key string, keyFrequency map[string]int, metadata map[string]any) int {
	if key == "" {
		return 0
	}
	switch engine.PluginID {
	case "metatrack_coaccess_routing":
		if metadata != nil {
			if groups, ok := metadata["coaccess_groups"].(map[string]string); ok {
				groupID := groups[key]
				if groupID != "" {
					if shards, ok := metadata["group_shard"].(map[string]int); ok {
						return shards[groupID]
					}
				}
			}
		}
	case "hotspot_aware_routing":
		if keyFrequency != nil && keyFrequency[key] >= engine.HotspotThreshold {
			return shard("hotspot:"+key, engine.ShardCount)
		}
	}
	return shard(key, engine.ShardCount)
}

func normalizeRoutingPlugin(pluginID string) string {
	switch pluginID {
	case "", "hash_sharding":
		return "hash_sharding"
	case "co_access_sharding", "metatrack_coaccess_routing":
		return "metatrack_coaccess_routing"
	case "hotspot_aware_routing":
		return "hotspot_aware_routing"
	default:
		return pluginID
	}
}

func transactionAccessKeys(tx Transaction) []string {
	keys := append([]string(nil), tx.ReadKeys...)
	for key := range tx.WriteDeltas {
		keys = append(keys, key)
	}
	return unique(keys)
}

func primaryRoutingKey(tx Transaction) string {
	writeKeys := make([]string, 0, len(tx.WriteDeltas))
	for key := range tx.WriteDeltas {
		writeKeys = append(writeKeys, key)
	}
	sort.Strings(writeKeys)
	if len(writeKeys) > 0 {
		return writeKeys[0]
	}
	if len(tx.ReadKeys) > 0 {
		return tx.ReadKeys[0]
	}
	return tx.ID
}

func routingOverhead(pluginID string, keyCount, hotspotCount int, coaccessGroupID string) int {
	switch pluginID {
	case "metatrack_coaccess_routing":
		if coaccessGroupID != "" {
			return 1
		}
		return max(0, keyCount-1)
	case "hotspot_aware_routing":
		return hotspotCount
	default:
		return 0
	}
}

func routingReason(pluginID, coaccessGroupID string, hotspotCount int) string {
	switch pluginID {
	case "metatrack_coaccess_routing":
		if coaccessGroupID != "" {
			return "coaccess_group_routing"
		}
		return "hash_fallback"
	case "hotspot_aware_routing":
		if hotspotCount > 0 {
			return "hotspot_key_routing"
		}
		return "hash_fallback"
	default:
		return "hash_sharding"
	}
}

func newExecutionEngine(chain ChainProfile, plugin PluginProfile) *ExecutionEngine {
	pluginID := normalizeExecutionPlugin(plugin.ExecutionSchedulerPlugin)
	parallelism := max(1, chain.ExecutionShardCount)
	return &ExecutionEngine{
		PluginID:           pluginID,
		Parallelism:        parallelism,
		FastTrackThreshold: 2,
	}
}

func (engine *ExecutionEngine) ExecuteBlock(block Block, blockReadyMS int) []ExecutionRecord {
	records := []ExecutionRecord{}
	previousAccessKeys := []map[string]bool{}
	for index, pooledTx := range block.Txs {
		edgeCount := 0
		currentKeys := keySet(transactionAccessKeys(pooledTx.Tx))
		for _, previous := range previousAccessKeys {
			if hasKeyOverlap(currentKeys, previous) {
				edgeCount++
			}
		}
		records = append(records, engine.ExecuteTransaction(pooledTx.Tx, block.Height, index, blockReadyMS, edgeCount))
		previousAccessKeys = append(previousAccessKeys, currentKeys)
	}
	return records
}

func (engine *ExecutionEngine) ExecuteTransaction(tx Transaction, blockHeight, txIndex, blockReadyMS, dependencyEdges int) ExecutionRecord {
	keys := transactionAccessKeys(tx)
	readCount := len(tx.ReadKeys)
	writeCount := len(tx.WriteDeltas)
	risk := dependencyRisk(dependencyEdges, writeCount, len(keys))
	track := "serial"
	blocked := false
	blockReason := ""
	workerID := 0
	reason := "serial_order"
	readyAt := blockReadyMS + txIndex
	latency := 1
	switch engine.PluginID {
	case "parallel_light_execution":
		if dependencyEdges == 0 {
			track = "parallel"
			workerID = shard(tx.ID, engine.Parallelism)
			reason = "parallel_no_conflict"
			readyAt = blockReadyMS
		} else {
			track = "blocked"
			blocked = true
			blockReason = "dependency_conflict"
			workerID = shard(tx.ID, engine.Parallelism)
			reason = "logical_dependency_delay"
			readyAt = blockReadyMS + dependencyEdges
			latency = 1 + min(dependencyEdges, 3)
		}
	case "metatrack_dual_track_execution":
		if len(keys) <= engine.FastTrackThreshold && writeCount <= 2 && risk == "low" {
			track = "fast"
			workerID = shard(tx.ID, engine.Parallelism)
			reason = "fast_track_low_dependency"
			readyAt = blockReadyMS
		} else {
			track = "conservative"
			workerID = 0
			reason = "conservative_track_dependency_guard"
			readyAt = blockReadyMS + dependencyEdges
			if risk == "high" {
				blocked = true
				blockReason = "high_dependency_risk"
			}
			latency = 1 + min(dependencyEdges, 3)
		}
	default:
		track = "serial"
		workerID = 0
		reason = "serial_execution_order"
		readyAt = blockReadyMS + txIndex
	}
	start := readyAt
	end := start + latency
	record := ExecutionRecord{
		TxID:                tx.ID,
		BlockHeight:         blockHeight,
		TxIndex:             txIndex,
		ExecutionPlugin:     engine.PluginID,
		Track:               track,
		AccessKeyCount:      len(keys),
		ReadKeyCount:        readCount,
		WriteKeyCount:       writeCount,
		DependencyEdgeCount: dependencyEdges,
		DependencyRisk:      risk,
		ReadyAtMS:           readyAt,
		StartAtMS:           start,
		EndAtMS:             end,
		ExecutionLatencyMS:  latency,
		Blocked:             blocked,
		BlockReason:         blockReason,
		WorkerID:            workerID,
		Reason:              reason,
	}
	engine.ExecutedTxCount++
	engine.DependencyEdgeCount += dependencyEdges
	engine.ExecutionLatencyTotalMS += latency
	engine.Latencies = append(engine.Latencies, latency)
	if blocked {
		engine.BlockedTxCount++
	}
	switch track {
	case "fast":
		engine.FastTrackCount++
		engine.ParallelizableTxCount++
	case "parallel":
		engine.ParallelizableTxCount++
	case "serial":
		engine.SerialTxCount++
	case "conservative":
		engine.ConservativeTrackCount++
	default:
		engine.ConservativeTrackCount++
	}
	return record
}

func normalizeExecutionPlugin(pluginID string) string {
	switch pluginID {
	case "", "serial_execution":
		return "serial_execution"
	case "dual_track_execution", "metatrack_dual_track_execution":
		return "metatrack_dual_track_execution"
	case "parallel_light_execution":
		return "parallel_light_execution"
	default:
		return pluginID
	}
}

func dependencyRisk(edges, writeCount, accessCount int) string {
	if edges >= 2 || (edges >= 1 && writeCount > 1) || accessCount >= 4 {
		return "high"
	}
	if edges == 1 || writeCount > 1 || accessCount == 3 {
		return "medium"
	}
	return "low"
}

func keySet(keys []string) map[string]bool {
	result := map[string]bool{}
	for _, key := range keys {
		result[key] = true
	}
	return result
}

func hasKeyOverlap(left, right map[string]bool) bool {
	for key := range left {
		if right[key] {
			return true
		}
	}
	return false
}

func newStateAccessEngine(chain ChainProfile, plugin PluginProfile) *StateAccessEngine {
	return &StateAccessEngine{
		PluginID:          normalizeStateAccessPlugin(plugin.StateAccessPlugin),
		LocalLatencyMS:    1,
		RemoteLatencyMS:   max(2, chain.RemoteFetchCostMS+1),
		CacheHitLatencyMS: 0,
		PrefetchLatencyMS: 0,
		Cache:             map[string]bool{},
	}
}

func (engine *StateAccessEngine) PrefetchSet(block Block) map[string]bool {
	prefetch := map[string]bool{}
	if engine.PluginID != "access_list_prefetch" {
		return prefetch
	}
	for _, pooledTx := range block.Txs {
		for _, key := range transactionAccessKeys(pooledTx.Tx) {
			prefetch[key] = true
		}
	}
	return prefetch
}

func (engine *StateAccessEngine) AccessTransaction(tx Transaction, blockHeight, txIndex, executionShard int, prefetchSet map[string]bool, chain ChainProfile) []StateAccessRecord {
	keys := transactionAccessItems(tx)
	records := []StateAccessRecord{}
	for _, item := range keys {
		homeShard := stateUnit(item.key, chain)
		isRemote := homeShard != executionShard
		cacheHit := false
		prefetched := false
		latency := engine.LocalLatencyMS
		reason := "local_direct_fetch"
		switch engine.PluginID {
		case "remote_state_access_model":
			if isRemote {
				latency = engine.RemoteLatencyMS
				reason = "remote_state_access_estimate"
			}
		case "cached_state_access":
			cacheHit = engine.Cache[item.key]
			if cacheHit {
				latency = engine.CacheHitLatencyMS
				reason = "cache_hit"
			} else if isRemote {
				latency = engine.RemoteLatencyMS
				reason = "cache_miss_remote_fetch"
			} else {
				reason = "cache_miss_local_fetch"
			}
			engine.Cache[item.key] = true
		case "access_list_prefetch":
			prefetched = prefetchSet != nil && prefetchSet[item.key]
			if prefetched {
				latency = engine.PrefetchLatencyMS
				reason = "access_list_prefetch_hit"
			} else if isRemote {
				latency = engine.RemoteLatencyMS
				reason = "access_list_prefetch_miss_remote"
			} else {
				reason = "access_list_prefetch_miss_local"
			}
		default:
			reason = "direct_fetch"
		}
		witnessEstimated := isRemote || engine.PluginID == "remote_state_access_model"
		proofEstimated := witnessEstimated
		record := StateAccessRecord{
			TxID:              tx.ID,
			BlockHeight:       blockHeight,
			TxIndex:           txIndex,
			StateAccessPlugin: engine.PluginID,
			AccessKey:         item.key,
			AccessType:        item.accessType,
			IsRead:            item.isRead,
			IsWrite:           item.isWrite,
			HomeShard:         homeShard,
			ExecutionShard:    executionShard,
			IsRemote:          isRemote,
			CacheHit:          cacheHit,
			Prefetched:        prefetched,
			WitnessEstimated:  witnessEstimated,
			ProofEstimated:    proofEstimated,
			AccessLatencyMS:   latency,
			Reason:            reason,
		}
		engine.record(record)
		records = append(records, record)
	}
	return records
}

func (engine *StateAccessEngine) record(record StateAccessRecord) {
	if record.IsRemote {
		engine.RemoteAccessCount++
		engine.RemoteLatencies = append(engine.RemoteLatencies, record.AccessLatencyMS)
	} else {
		engine.LocalAccessCount++
	}
	if engine.PluginID == "cached_state_access" {
		if record.CacheHit {
			engine.CacheHitCount++
		} else {
			engine.CacheMissCount++
		}
	}
	if engine.PluginID == "access_list_prefetch" {
		if record.Prefetched {
			engine.PrefetchHitCount++
		} else {
			engine.PrefetchMissCount++
		}
	}
	if record.WitnessEstimated {
		engine.WitnessEstimatedCount++
		engine.EstimatedWitnessBytes += 128
	}
	if record.ProofEstimated {
		engine.ProofEstimatedCount++
		engine.EstimatedProofBytes += 256
	}
	engine.StateAccessLatencyTotalMS += record.AccessLatencyMS
	engine.Latencies = append(engine.Latencies, record.AccessLatencyMS)
}

type accessItem struct {
	key        string
	accessType string
	isRead     bool
	isWrite    bool
}

func transactionAccessItems(tx Transaction) []accessItem {
	items := []accessItem{}
	seen := map[string]int{}
	for _, key := range tx.ReadKeys {
		if key == "" {
			continue
		}
		index, ok := seen[key]
		if ok {
			items[index].isRead = true
			if items[index].isWrite {
				items[index].accessType = "read_write"
			}
			continue
		}
		seen[key] = len(items)
		items = append(items, accessItem{key: key, accessType: "read", isRead: true})
	}
	writeKeys := make([]string, 0, len(tx.WriteDeltas))
	for key := range tx.WriteDeltas {
		writeKeys = append(writeKeys, key)
	}
	sort.Strings(writeKeys)
	for _, key := range writeKeys {
		if key == "" {
			continue
		}
		index, ok := seen[key]
		if ok {
			items[index].isWrite = true
			if items[index].isRead {
				items[index].accessType = "read_write"
			} else {
				items[index].accessType = "write"
			}
			continue
		}
		seen[key] = len(items)
		items = append(items, accessItem{key: key, accessType: "write", isWrite: true})
	}
	return items
}

func normalizeStateAccessPlugin(pluginID string) string {
	switch pluginID {
	case "", "direct_fetch":
		return "direct_fetch"
	case "remote_state_access_model":
		return "remote_state_access_model"
	case "cached_state_access":
		return "cached_state_access"
	case "access_list_prefetch":
		return "access_list_prefetch"
	default:
		return pluginID
	}
}

func stateAccessRemoteCount(records []StateAccessRecord) int {
	count := 0
	for _, record := range records {
		if record.IsRemote {
			count++
		}
	}
	return count
}

func stateAccessRemoteFetchCount(records []StateAccessRecord, plugin PluginProfile) int {
	if normalizeStateAccessPlugin(plugin.StateAccessPlugin) == "access_list_prefetch" {
		return 0
	}
	return stateAccessRemoteCount(records)
}

func stateAccessLatencyForTx(records []StateAccessRecord) int {
	total := 0
	for _, record := range records {
		total += record.AccessLatencyMS
	}
	return total
}

func newCommitEngine(plugin PluginProfile) *CommitEngine {
	return &CommitEngine{
		PluginID:          normalizeCommitPlugin(plugin.CommitPlugin),
		aggregationGroups: map[string]bool{},
	}
}

func (engine *CommitEngine) PlanBlock(block Block) CommitPlan {
	rawByKey := map[string]int{}
	for _, pooledTx := range block.Txs {
		for key := range pooledTx.Tx.WriteDeltas {
			rawByKey[key]++
		}
	}
	return CommitPlan{RawByKey: rawByKey}
}

func (engine *CommitEngine) CommitTransaction(tx Transaction, blockHeight, txIndex, executionShardID, commitTime int, deltas map[string][3]int, state map[string]int, chain ChainProfile, plugin PluginProfile, plan CommitPlan) []StateCommit {
	records := []StateCommit{}
	if len(deltas) > 0 {
		engine.CommitTxCount++
	}
	keys := sortedDeltaKeys(deltas)
	for _, key := range keys {
		values := deltas[key]
		oldValue, delta, newValue := values[0], values[1], values[2]
		state[key] = newValue
		rawCount := plan.RawByKey[key]
		if rawCount <= 0 {
			rawCount = 1
		}
		hotspot := commitHotspotKey(key) || rawCount >= 2
		path := "normal"
		reason := "normal_commit"
		aggregated := false
		groupID := ""
		aggregatedCount := 0
		constraintChecked := false
		constraintPassed := true
		latency := 1
		switch engine.PluginID {
		case "conservative_commit":
			path = "conservative"
			reason = "conservative_path"
			latency = 2
		case "hot_update_aggregation":
			if hotspot && rawCount >= 2 {
				path = "aggregated"
				reason = "hot_update_aggregated"
				aggregated = true
				aggregatedCount = 1
				groupID = fmt.Sprintf("agg_%d_%s", blockHeight, key)
			} else {
				reason = "hot_update_no_group"
			}
		case "constraint_checked_aggregation":
			constraintChecked = true
			constraintPassed = newValue >= 0
			latency = 2
			if constraintPassed && hotspot && rawCount >= 2 {
				path = "aggregated"
				reason = "constraint_checked_aggregation"
				aggregated = true
				aggregatedCount = 1
				groupID = fmt.Sprintf("agg_%d_%s", blockHeight, key)
			} else if !constraintPassed {
				path = "conservative"
				reason = "constraint_failed_conservative_path"
				latency = 3
			} else {
				reason = "constraint_checked_no_group"
			}
		}
		storageUnit := stateUnit(key, chain)
		record := StateCommit{
			BlockHeight:        blockHeight,
			TxIndex:            txIndex,
			TxID:               tx.ID,
			StateKey:           key,
			OldValue:           oldValue,
			Delta:              delta,
			NewValue:           newValue,
			CommitPlugin:       engine.PluginID,
			CommitPath:         path,
			UpdateType:         "delta",
			IsHotspot:          hotspot,
			Aggregated:         aggregated,
			AggregationGroupID: groupID,
			RawUpdateCount:     rawCount,
			AggregatedCount:    aggregatedCount,
			ConstraintChecked:  constraintChecked,
			ConstraintPassed:   constraintPassed,
			CommitLatencyMS:    latency,
			Reason:             reason,
			CommitTimeMS:       commitTime,
			Status:             "success",
			StateStorageUnitID: storageUnit,
			ExecutionShardID:   executionShardID,
			IsRemoteCommit:     storageUnit != executionShardID,
			PlacementPolicy:    chain.StatePlacementPolicy,
			RoutingPlugin:      plugin.ShardingPlugin,
		}
		engine.recordCommit(record)
		records = append(records, record)
	}
	return records
}

func (engine *CommitEngine) recordCommit(record StateCommit) {
	engine.CommitUpdateCount++
	engine.RawUpdateCount++
	engine.Latencies = append(engine.Latencies, record.CommitLatencyMS)
	if record.IsHotspot {
		engine.HotspotUpdateCount++
	}
	if record.Aggregated {
		engine.AggregatedUpdateCount++
		if record.AggregationGroupID != "" && !engine.aggregationGroups[record.AggregationGroupID] {
			engine.aggregationGroups[record.AggregationGroupID] = true
			engine.AggregationGroupCount++
		}
	}
	if record.ConstraintChecked {
		engine.ConstraintCheckCount++
		if record.ConstraintPassed {
			engine.ConstraintPassedCount++
		} else {
			engine.ConstraintFailedCount++
		}
	}
	switch record.CommitPath {
	case "conservative":
		engine.ConservativeCommitCount++
	default:
		engine.NormalCommitCount++
	}
}

func normalizeCommitPlugin(pluginID string) string {
	switch pluginID {
	case "", "normal_commit":
		return "normal_commit"
	case "conservative_commit":
		return "conservative_commit"
	case "hot_update_aggregation", "hot_update_aggregation_commit":
		return "hot_update_aggregation"
	case "constraint_checked_aggregation":
		return "constraint_checked_aggregation"
	default:
		return pluginID
	}
}

func sortedDeltaKeys(deltas map[string][3]int) []string {
	keys := make([]string, 0, len(deltas))
	for key := range deltas {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

func commitHotspotKey(key string) bool {
	return strings.HasPrefix(key, "asset_0") || strings.HasPrefix(key, "asset_1") || strings.HasPrefix(key, "asset_2") || strings.HasPrefix(key, "asset_3")
}

func requireSupportedPlugins(plugin PluginProfile) error {
	baseExpected := map[string]string{
		"TxPoolPlugin":  "fifo_pool",
		"BlockProducer": "time_or_count_block_producer",
		"MetricsPlugin": "basic_metrics",
	}
	actual := map[string]string{
		"TxPoolPlugin":             plugin.TxPoolPlugin,
		"BlockProducer":            plugin.BlockProducer,
		"ConsensusPlugin":          plugin.ConsensusPlugin,
		"ConsensusRuntimePlugin":   plugin.ConsensusRuntimePlugin,
		"ShardingPlugin":           plugin.ShardingPlugin,
		"ExecutionSchedulerPlugin": plugin.ExecutionSchedulerPlugin,
		"StateAccessPlugin":        plugin.StateAccessPlugin,
		"CommitPlugin":             plugin.CommitPlugin,
		"MetricsPlugin":            plugin.MetricsPlugin,
	}
	for key, value := range baseExpected {
		if actual[key] != value {
			return fmt.Errorf("V3 Go runtime requires %s=%s", key, value)
		}
	}
	allowed := map[string]map[string]bool{
		"ShardingPlugin":           {"hash_sharding": true, "co_access_sharding": true, "metatrack_coaccess_routing": true, "hotspot_aware_routing": true},
		"ExecutionSchedulerPlugin": {"serial_execution": true, "dual_track_execution": true, "parallel_light_execution": true, "metatrack_dual_track_execution": true},
		"StateAccessPlugin":        {"direct_fetch": true, "remote_state_access_model": true, "cached_state_access": true, "access_list_prefetch": true},
		"CommitPlugin":             {"normal_commit": true, "conservative_commit": true, "hot_update_aggregation": true, "hot_update_aggregation_commit": true, "constraint_checked_aggregation": true},
		"ConsensusPlugin":          {"simple_leader": true, "poa_light": true, "pbft_light_model": true},
		"ConsensusRuntimePlugin":   {"simple_leader": true, "poa_light": true, "pbft_light_model": true, ConsensusRuntimeBlockEmulatorPBFTPreview: true},
	}
	for key, values := range allowed {
		if !values[actual[key]] {
			return fmt.Errorf("unsupported V3 Go runtime plugin %s=%s", key, actual[key])
		}
	}
	return nil
}

func generateWorkload(exp ExperimentProfile, chain ChainProfile) []Transaction {
	keyCount := exp.KeyCount
	if keyCount <= 0 {
		keyCount = chain.KeyCount
	}
	if keyCount <= 0 {
		keyCount = 1
	}
	hotKeyCount := exp.HotKeyCount
	if hotKeyCount <= 0 || hotKeyCount > keyCount {
		hotKeyCount = max(1, keyCount/10)
	}
	interval := int(math.Round(1000 / float64(max(1, exp.SubmitRate))))
	if interval <= 0 {
		interval = 1
	}
	txs := make([]Transaction, 0, exp.TxCount)
	for i := 0; i < exp.TxCount; i++ {
		keyIndex := deterministicKey(i, exp.Seed, keyCount)
		if exp.HotspotRatio > 0 && (i+exp.Seed)%4 == 0 {
			keyIndex = deterministicKey(i, exp.Seed, hotKeyCount)
		}
		key := fmt.Sprintf("asset_%d", keyIndex)
		keys := []string{key}
		if exp.AccessListEnabled && exp.AggregationCandidatesEnabled && i%3 == 0 {
			keys = append(keys, fmt.Sprintf("asset_%d", deterministicKey(i+7, exp.Seed, keyCount)))
		}
		delta := 1 + (i % 5)
		txs = append(txs, Transaction{
			ID:           fmt.Sprintf("tx_%06d", i),
			SubmitTimeMS: i * interval,
			ReadKeys:     keys,
			WriteDeltas:  map[string]int{key: delta},
			Commutative:  true,
			ConflictHint: conflictHint(i),
		})
	}
	return txs
}

func conflictHint(index int) string {
	if index%11 == 0 {
		return "high"
	}
	return "low"
}

func deterministicKey(index, seed, keyCount int) int {
	value := (index*1103515245 + seed*12345 + 67890) & 0x7fffffff
	return value % max(1, keyCount)
}

func newTxPool(chain ChainProfile) *TxPool {
	maxPoolSize := chain.MaxPoolSize
	if maxPoolSize <= 0 {
		maxPoolSize = 100000
	}
	backpressure := chain.BackpressurePolicy
	if backpressure == "" {
		backpressure = "reject"
	}
	return &TxPool{
		maxPoolSize:  maxPoolSize,
		dedupEnabled: chain.DedupEnabled,
		backpressure: backpressure,
		seen:         map[string]bool{},
	}
}

func (pool *TxPool) Admit(tx Transaction, nowMS int) bool {
	before := len(pool.queue)
	if pool.dedupEnabled && pool.seen[tx.ID] {
		pool.rejectedCount++
		pool.events = append(pool.events, TxPoolEvent{
			EventTimeMS:    nowMS,
			EventType:      "reject",
			TxID:           tx.ID,
			PoolSizeBefore: before,
			PoolSizeAfter:  before,
			RejectedCount:  1,
			Reason:         "duplicate_tx",
		})
		return false
	}
	if before >= pool.maxPoolSize {
		pool.rejectedCount++
		pool.events = append(pool.events, TxPoolEvent{
			EventTimeMS:    nowMS,
			EventType:      "reject",
			TxID:           tx.ID,
			PoolSizeBefore: before,
			PoolSizeAfter:  before,
			RejectedCount:  1,
			Reason:         "pool_full_" + pool.backpressure,
		})
		if pool.dedupEnabled {
			pool.seen[tx.ID] = true
		}
		return false
	}
	pool.queue = append(pool.queue, txPoolEntry{tx: tx, admitTimeMS: nowMS})
	if pool.dedupEnabled {
		pool.seen[tx.ID] = true
	}
	pool.admittedCount++
	if len(pool.queue) > pool.peakSize {
		pool.peakSize = len(pool.queue)
	}
	pool.events = append(pool.events, TxPoolEvent{
		EventTimeMS:    nowMS,
		EventType:      "admit",
		TxID:           tx.ID,
		PoolSizeBefore: before,
		PoolSizeAfter:  len(pool.queue),
		AdmittedCount:  1,
		Reason:         "admitted",
	})
	return true
}

func (pool *TxPool) SelectForBlock(maxTx, cutTimeMS, blockHeight int) []PooledTransaction {
	limit := max(1, maxTx)
	if limit > len(pool.queue) {
		limit = len(pool.queue)
	}
	selected := make([]PooledTransaction, 0, limit)
	for i := 0; i < limit; i++ {
		before := len(pool.queue)
		entry := pool.queue[0]
		pool.queue = pool.queue[1:]
		wait := cutTimeMS - entry.admitTimeMS
		if wait < 0 {
			wait = 0
		}
		pool.queueWaits = append(pool.queueWaits, wait)
		pool.selectedCount++
		selected = append(selected, PooledTransaction{Tx: entry.tx, AdmitTimeMS: entry.admitTimeMS, QueueWaitMS: wait})
		pool.events = append(pool.events, TxPoolEvent{
			EventTimeMS:    cutTimeMS,
			EventType:      "select",
			TxID:           entry.tx.ID,
			BlockHeight:    blockHeight,
			PoolSizeBefore: before,
			PoolSizeAfter:  len(pool.queue),
			SelectedCount:  1,
			QueueWaitMS:    wait,
			Reason:         "selected_for_block",
		})
	}
	return selected
}

func (pool *TxPool) Len() int {
	return len(pool.queue)
}

func newBlockProducer(chain ChainProfile, plugin PluginProfile) *BlockProducer {
	pluginID := plugin.BlockProducer
	if pluginID == "" {
		pluginID = "time_or_count_block_producer"
	}
	return &BlockProducer{
		PluginID:          pluginID,
		BlockIntervalMS:   max(1, chain.BlockIntervalMS),
		MaxTxPerBlock:     max(1, chain.MaxTxPerBlock),
		EmptyBlockEnabled: chain.EmptyBlockEnabled,
		ParentHash:        genesisParentHash(chain.ProfileID),
		NextHeight:        1,
	}
}

func (producer *BlockProducer) ShouldCut(nowMS, poolSize int, workloadDone bool) (bool, string) {
	if poolSize >= producer.MaxTxPerBlock {
		return true, "count"
	}
	if poolSize > 0 && workloadDone {
		return true, "drain"
	}
	if poolSize > 0 && nowMS-producer.LastCutTimeMS >= producer.BlockIntervalMS {
		return true, "time"
	}
	if poolSize == 0 && producer.EmptyBlockEnabled && nowMS-producer.LastCutTimeMS >= producer.BlockIntervalMS {
		return true, "empty"
	}
	return false, ""
}

func (producer *BlockProducer) ProduceBlock(nowMS int, pool *TxPool, cutReason string) Block {
	height := producer.NextHeight
	before := pool.Len()
	selected := []PooledTransaction{}
	if before > 0 {
		selected = pool.SelectForBlock(producer.MaxTxPerBlock, nowMS, height)
	}
	after := pool.Len()
	block := Block{
		Height:            height,
		ID:                fmt.Sprintf("block_%06d", height),
		ParentHash:        producer.ParentHash,
		ProducerPlugin:    producer.PluginID,
		CutReason:         cutReason,
		PoolSizeBeforeCut: before,
		PoolSizeAfterCut:  after,
		Txs:               selected,
		CutTimeMS:         nowMS,
	}
	block.Hash = producer.makeBlockHash(block)
	producer.ParentHash = block.Hash
	producer.NextHeight++
	producer.ProducedBlockCount++
	if len(selected) == 0 {
		producer.EmptyBlockCount++
	}
	switch cutReason {
	case "count":
		producer.CountCutCount++
	case "time":
		producer.TimeCutCount++
	case "drain":
		producer.DrainCutCount++
	case "empty":
		producer.EmptyCutCount++
	}
	if producer.LastCutTimeMS > 0 {
		producer.CutIntervals = append(producer.CutIntervals, nowMS-producer.LastCutTimeMS)
	}
	producer.LastCutTimeMS = nowMS
	producer.BlockSizes = append(producer.BlockSizes, len(selected))
	return block
}

func (producer *BlockProducer) makeBlockHash(block Block) string {
	parts := []string{strconv.Itoa(block.Height), block.ParentHash, strconv.Itoa(block.CutTimeMS), producer.PluginID, block.CutReason}
	for _, pooledTx := range block.Txs {
		parts = append(parts, pooledTx.Tx.ID)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return fmt.Sprintf("%x", sum[:])
}

func genesisParentHash(chainID string) string {
	sum := sha256.Sum256([]byte("genesis|" + chainID))
	return fmt.Sprintf("%x", sum[:])
}

func produceBlocksFromTxPool(txs []Transaction, chain ChainProfile, pool *TxPool, producer *BlockProducer) []Block {
	blocks := []Block{}
	nextTx := 0
	for nextTx < len(txs) || pool.Len() > 0 {
		nowMS := producer.LastCutTimeMS + producer.BlockIntervalMS
		lastAdmitTime := -1
		idleTimeCut := false
		for nextTx < len(txs) {
			tx := txs[nextTx]
			if pool.Len() >= producer.MaxTxPerBlock {
				break
			}
			if pool.Len() > 0 && lastAdmitTime >= 0 && tx.SubmitTimeMS-lastAdmitTime >= producer.BlockIntervalMS {
				nowMS = max(producer.LastCutTimeMS+producer.BlockIntervalMS, lastAdmitTime)
				idleTimeCut = true
				break
			}
			pool.Admit(tx, tx.SubmitTimeMS)
			lastAdmitTime = tx.SubmitTimeMS
			nextTx++
			if pool.Len() >= producer.MaxTxPerBlock {
				nowMS = max(producer.LastCutTimeMS+producer.BlockIntervalMS, tx.SubmitTimeMS)
				break
			}
		}
		workloadDone := nextTx >= len(txs)
		if workloadDone && lastAdmitTime >= 0 {
			nowMS = max(producer.LastCutTimeMS+producer.BlockIntervalMS, lastAdmitTime)
		}
		if pool.Len() == 0 && !producer.EmptyBlockEnabled {
			continue
		}
		shouldCut, cutReason := producer.ShouldCut(nowMS, pool.Len(), workloadDone)
		if idleTimeCut && pool.Len() > 0 && pool.Len() < producer.MaxTxPerBlock {
			shouldCut = true
			cutReason = "time"
		}
		if !shouldCut {
			continue
		}
		block := producer.ProduceBlock(nowMS, pool, cutReason)
		if len(block.Txs) == 0 && !producer.EmptyBlockEnabled {
			continue
		}
		blocks = append(blocks, block)
	}
	return blocks
}

func buildSummary(runID string, exp ExperimentProfile, chain ChainProfile, pluginProfileID string, txs []TxResult, blockCount int, runtimeMode string) Summary {
	latencies := []int{}
	firstSubmit, lastCommit := 0, 0
	for i, tx := range txs {
		latencies = append(latencies, tx.LatencyMS)
		if i == 0 || tx.SubmitTimeMS < firstSubmit {
			firstSubmit = tx.SubmitTimeMS
		}
		if tx.CommitTimeMS > lastCommit {
			lastCommit = tx.CommitTimeMS
		}
	}
	duration := float64(lastCommit-firstSubmit) / 1000
	if duration <= 0 {
		duration = 0.001
	}
	return Summary{
		RunID:                 runID,
		Stage:                 exp.Stage,
		BackendType:           exp.BackendType,
		TruthLabel:            exp.TruthLabel,
		ChainProfileID:        chain.ProfileID,
		PluginProfileID:       pluginProfileID,
		ExperimentProfileID:   exp.ProfileID,
		TxCount:               len(txs),
		SuccessCount:          len(txs),
		FailureCount:          0,
		BlockCount:            blockCount,
		ThroughputTPS:         round(float64(len(txs)) / duration),
		AvgLatencyMS:          round(avg(latencies)),
		P95LatencyMS:          percentileInt(latencies, 95),
		P99LatencyMS:          percentileInt(latencies, 99),
		RuntimeMode:           runtimeMode,
		ExecutionShardCount:   chain.ExecutionShardCount,
		StateStorageUnitCount: chain.StateStorageUnitCount,
	}
}

func applyMechanismMetrics(summary *Summary, txs []TxResult, plugin PluginProfile, chain ChainProfile) {
	remote := 0
	crossShard := 0
	crossStateUnit := 0
	localityHits := 0
	fast := 0
	conservative := 0
	conflicts := 0
	aggregated := 0
	hotCounts := map[string]int{}
	executionLoads := map[int]int{}
	stateUnitLoads := map[int]int{}
	for _, tx := range txs {
		remote += tx.RemoteFetchCount
		if tx.RemoteFetchCount > 0 {
			crossShard++
		}
		if tx.CrossStateUnitAccess {
			crossStateUnit++
		}
		if tx.StateLocalityHit {
			localityHits++
		}
		executionLoads[tx.ExecutionShardID]++
		for _, unitID := range tx.AccessedStateUnitIDs {
			stateUnitLoads[unitID]++
		}
		if tx.Track == "fast" {
			fast++
		} else {
			conservative++
		}
		if tx.Track == "conservative" && plugin.ExecutionSchedulerPlugin == "dual_track_execution" {
			conflicts++
		}
		for key := range tx.Deltas {
			hotCounts[key]++
		}
	}
	if plugin.CommitPlugin == "hot_update_aggregation_commit" {
		for _, count := range hotCounts {
			if count >= 2 {
				aggregated += count
			}
		}
	}
	summary.RemoteFetchCount = remote
	summary.RemoteStateFetchCount = remote
	summary.CrossStateUnitAccessCount = crossStateUnit
	if len(txs) > 0 {
		summary.CrossShardRatio = round(float64(crossShard) / float64(len(txs)))
		summary.AggregationRatio = round(float64(aggregated) / float64(len(txs)))
		summary.StateLocalityRatio = round(float64(localityHits) / float64(len(txs)))
	}
	summary.FastTrackCount = fast
	summary.ConservativeTrackCount = conservative
	summary.AggregatedUpdateCount = aggregated
	summary.ConflictCount = conflicts
	summary.BlockCommitLatencyMS = summary.AvgLatencyMS
	summary.ExecutionShardLoadBalance = loadBalance(executionLoads, chain.ExecutionShardCount)
	summary.StateUnitLoadBalance = loadBalance(stateUnitLoads, chain.StateStorageUnitCount)
}

func applyTxPoolMetrics(summary *Summary, pool *TxPool) {
	summary.TxPoolAdmittedCount = pool.admittedCount
	summary.TxPoolRejectedCount = pool.rejectedCount
	summary.TxPoolPeakSize = pool.peakSize
	summary.TxPoolAvgWaitMS = round(avg(pool.queueWaits))
	summary.TxPoolP95WaitMS = percentileInt(pool.queueWaits, 95)
	summary.QueueWaitMS = summary.TxPoolAvgWaitMS
}

func applyBlockProducerMetrics(summary *Summary, producer *BlockProducer) {
	summary.EmptyBlockCount = producer.EmptyBlockCount
	summary.AvgBlockSize = round(avg(producer.BlockSizes))
	summary.MaxBlockSize = maxInt(producer.BlockSizes)
	summary.BlockIntervalMS = producer.BlockIntervalMS
	summary.AvgBlockIntervalMS = round(avg(producer.CutIntervals))
	summary.BlockProducerCountCut = producer.CountCutCount
	summary.BlockProducerTimeCut = producer.TimeCutCount
	summary.BlockProducerDrainCut = producer.DrainCutCount
	summary.BlockProducerEmptyCut = producer.EmptyCutCount
}

func applyConsensusMetrics(summary *Summary, records []ConsensusRecord) {
	latencies := []int{}
	messageCounts := []int{}
	totalMessages := 0
	viewChanges := 0
	finalized := 0
	failed := 0
	for _, record := range records {
		latencies = append(latencies, record.ConsensusLatencyMS)
		messageCounts = append(messageCounts, record.TotalMessageCount)
		totalMessages += record.TotalMessageCount
		viewChanges += record.ViewChangeCount
		if record.Finalized {
			finalized++
		} else {
			failed++
		}
	}
	summary.ConsensusLatencyMS = round(avg(latencies))
	summary.AvgConsensusLatencyMS = round(avg(latencies))
	summary.P95ConsensusLatencyMS = percentileInt(latencies, 95)
	summary.ConsensusMessageCount = totalMessages
	summary.AvgConsensusMessageCount = round(avg(messageCounts))
	summary.ConsensusRoundCount = len(records)
	summary.ViewChangeCount = viewChanges
	summary.FinalizedBlockCount = finalized
	summary.FailedBlockCount = failed
}

func applyRoutingMetrics(summary *Summary, engine *RoutingEngine, records []RoutingRecord) {
	touchedCounts := []int{}
	maxTouched := 0
	for _, record := range records {
		touchedCounts = append(touchedCounts, record.TouchedShardCount)
		if record.TouchedShardCount > maxTouched {
			maxTouched = record.TouchedShardCount
		}
	}
	summary.RoutingPlugin = engine.PluginID
	summary.RoutingDecisionCount = engine.DecisionCount
	summary.CrossShardTxCount = engine.CrossShardDecisionCount
	summary.LocalTxCount = engine.LocalDecisionCount
	if engine.DecisionCount > 0 {
		summary.CrossShardRatio = round(float64(engine.CrossShardDecisionCount) / float64(engine.DecisionCount))
	}
	summary.RemoteStateAccessCount = engine.RemoteStateAccessEstimate
	summary.AvgTouchedShards = round(avg(touchedCounts))
	summary.MaxTouchedShards = maxTouched
	summary.HotspotKeyCount = engine.HotspotKeyCount
	summary.CoaccessGroupCount = engine.CoaccessGroupCount
	summary.AvgRoutingOverheadMS = round(avg(engine.Overheads))
}

func applyExecutionMetrics(summary *Summary, engine *ExecutionEngine, records []ExecutionRecord) {
	summary.ExecutionPlugin = engine.PluginID
	summary.ExecutionTxCount = engine.ExecutedTxCount
	summary.FastTrackCount = engine.FastTrackCount
	summary.ConservativeTrackCount = engine.ConservativeTrackCount
	summary.BlockedTxCount = engine.BlockedTxCount
	summary.DependencyEdgeCount = engine.DependencyEdgeCount
	if engine.ExecutedTxCount > 0 {
		summary.AvgDependencyEdgesPerTx = round(float64(engine.DependencyEdgeCount) / float64(engine.ExecutedTxCount))
	}
	summary.AvgExecutionLatencyMS = round(avg(engine.Latencies))
	summary.P95ExecutionLatencyMS = percentileInt(engine.Latencies, 95)
	summary.MaxExecutionLatencyMS = maxInt(engine.Latencies)
	summary.LogicalWorkerCount = engine.Parallelism
	summary.ParallelizableTxCount = engine.ParallelizableTxCount
	summary.SerialTxCount = engine.SerialTxCount
	conflicts := 0
	for _, record := range records {
		if record.DependencyEdgeCount > 0 {
			conflicts++
		}
	}
	summary.ConflictCount = conflicts
}

func applyStateAccessMetrics(summary *Summary, engine *StateAccessEngine) {
	total := engine.LocalAccessCount + engine.RemoteAccessCount
	summary.StateAccessPlugin = engine.PluginID
	summary.StateAccessCount = total
	summary.LocalStateAccessCount = engine.LocalAccessCount
	summary.RemoteStateAccessCount = engine.RemoteAccessCount
	if total > 0 {
		summary.RemoteStateAccessRatio = round(float64(engine.RemoteAccessCount) / float64(total))
	}
	summary.CacheHitCount = engine.CacheHitCount
	summary.CacheMissCount = engine.CacheMissCount
	if engine.CacheHitCount+engine.CacheMissCount > 0 {
		summary.CacheHitRate = round(float64(engine.CacheHitCount) / float64(engine.CacheHitCount+engine.CacheMissCount))
	}
	summary.PrefetchHitCount = engine.PrefetchHitCount
	summary.PrefetchMissCount = engine.PrefetchMissCount
	if engine.PrefetchHitCount+engine.PrefetchMissCount > 0 {
		summary.PrefetchHitRate = round(float64(engine.PrefetchHitCount) / float64(engine.PrefetchHitCount+engine.PrefetchMissCount))
	}
	summary.AvgStateAccessLatencyMS = round(avg(engine.Latencies))
	summary.P95StateAccessLatencyMS = percentileInt(engine.Latencies, 95)
	summary.MaxStateAccessLatencyMS = maxInt(engine.Latencies)
	summary.RemoteStateAccessLatencyMS = round(avg(engine.RemoteLatencies))
	summary.WitnessEstimatedCount = engine.WitnessEstimatedCount
	summary.ProofEstimatedCount = engine.ProofEstimatedCount
	summary.EstimatedWitnessBytes = engine.EstimatedWitnessBytes
	summary.EstimatedProofBytes = engine.EstimatedProofBytes
	summary.RemoteFetchCount = engine.RemoteAccessCount
	summary.RemoteStateFetchCount = engine.RemoteAccessCount
}

func applyCommitMetrics(summary *Summary, engine *CommitEngine) {
	summary.CommitPlugin = engine.PluginID
	summary.CommitTxCount = engine.CommitTxCount
	summary.CommitUpdateCount = engine.CommitUpdateCount
	summary.NormalCommitCount = engine.NormalCommitCount
	summary.ConservativeCommitCount = engine.ConservativeCommitCount
	summary.HotspotUpdateCount = engine.HotspotUpdateCount
	summary.AggregatedUpdateCount = engine.AggregatedUpdateCount
	summary.RawUpdateCount = engine.RawUpdateCount
	summary.AggregationGroupCount = engine.AggregationGroupCount
	if engine.RawUpdateCount > 0 {
		summary.AggregationRatio = round(float64(engine.AggregatedUpdateCount) / float64(engine.RawUpdateCount))
	}
	summary.ConstraintCheckCount = engine.ConstraintCheckCount
	summary.ConstraintPassedCount = engine.ConstraintPassedCount
	summary.ConstraintFailedCount = engine.ConstraintFailedCount
	summary.AvgCommitLatencyMS = round(avg(engine.Latencies))
	summary.P95CommitLatencyMS = percentileInt(engine.Latencies, 95)
	summary.MaxCommitLatencyMS = maxInt(engine.Latencies)
	summary.BlockCommitLatencyMS = summary.AvgCommitLatencyMS
}

func applyNodeRuntimeMetrics(summary *Summary, nodeRuntime NodeRuntimeArtifacts) {
	summary.ShardCount = nodeRuntime.Config.ShardCount
	summary.ValidatorsPerShard = nodeRuntime.Config.ValidatorsPerShard
	summary.LogicalNodeCount = len(nodeRuntime.Nodes)
	summary.ValidatorNodeCount = nodeRuntime.CountRole("validator")
	summary.ExecutorNodeCount = nodeRuntime.CountRole("executor")
	summary.StorageNodeCount = nodeRuntime.CountRole("storage")
	summary.SupervisorNodeCount = nodeRuntime.CountRole("supervisor")
	summary.NetworkMessageCount = len(nodeRuntime.NetworkMessages)
	summary.ConsensusMessageCount = len(nodeRuntime.ConsensusMessages)
	summary.MessageCount = summary.NetworkMessageCount + summary.ConsensusMessageCount
	summary.NodeEventCount = len(nodeRuntime.NodeEvents)
}

func applyLauncherPreviewMetrics(summary *Summary, launcher LauncherPreview) {
	summary.LauncherMode = "local_multi_process_launcher_preview"
	summary.LauncherScriptCount = 2
	summary.LaunchableNodeCount = len(launcher.Addresses)
	summary.NodeAddressCount = len(launcher.Addresses)
	summary.WindowsLauncherAvailable = true
	summary.LinuxLauncherAvailable = true
	summary.LauncherPreviewOnly = true
}

func applyNodeProcessPreviewMetrics(summary *Summary) {
	summary.NodeProcessEntrypointAvailable = true
	summary.NodeProcessPreviewAvailable = true
	summary.NodeProcessStatusAvailable = true
	summary.NodeProcessManifestAvailable = true
	summary.NodeProcessPreviewOnly = true
}

func applyNetworkAdapterMetrics(summary *Summary, preview NetworkAdapterPreview) {
	summary.NetworkAdapterSelected = preview.SelectedAdapter
	summary.TCPPreviewEnabled = preview.TCPPreview
	summary.TCPListenNodeCount = preview.ListenNodeCount()
	summary.TCPSendCount = len(preview.SendRows)
	summary.TCPReceiveCount = len(preview.ReceiveRows)
	summary.TypedMessageCount = len(preview.TypedMessages)
	summary.NetworkErrorCount = preview.ErrorCount
}

func applyConsensusNetworkLightMetrics(summary *Summary, preview ConsensusNetworkLightPreview) {
	summary.ConsensusOverNetworkEnabled = preview.Enabled
	summary.ConsensusRuntimeSelected = preview.ConsensusRuntimeSelected
	summary.ProposalPreviewCount = preview.ProposalPreviewCount
	summary.VotePreviewCount = preview.VotePreviewCount
	summary.LightQuorumReachedCount = preview.LightQuorumReachedCount
	summary.ConsensusNetworkErrorCount = preview.ErrorCount
	summary.ConsensusNetworkPath = preview.NetworkPath
}

func applyPBFTPreviewMetrics(summary *Summary, preview PBFTPreview) {
	summary.ConsensusRuntimeSelected = preview.ConsensusRuntimeSelected
	summary.PBFTView = preview.View
	summary.PBFTSequence = preview.Sequence
	summary.PBFTPrePrepareCount = preview.PrePrepareCount
	summary.PBFTPrepareCount = preview.PrepareCount
	summary.PBFTCommitCount = preview.CommitCount
	summary.PBFTQuorumReachedCount = preview.QuorumReachedCount
	summary.PBFTFinalizedBlockCount = preview.FinalizedBlockCount
	summary.PBFTConsensusLatencyMS = preview.ConsensusLatencyMS
	summary.PBFTPreviewEnabled = preview.Enabled
	summary.PBFTQuorumThreshold = preview.QuorumThreshold
}

func applyPBFTNetworkMetrics(summary *Summary, preview PBFTNetworkPreview) {
	summary.PBFTOverNetworkEnabled = preview.Enabled
	summary.PBFTNetworkPath = preview.NetworkPath
	summary.PBFTNetworkMessageCount = preview.MessageCount
	summary.PBFTNetworkErrorCount = preview.ErrorCount
	summary.PBFTPrePrepareNetworkCount = preview.PrePrepareNetworkCount
	summary.PBFTPrepareNetworkCount = preview.PrepareNetworkCount
	summary.PBFTCommitNetworkCount = preview.CommitNetworkCount
	summary.PBFTFinalizedNetworkCount = preview.FinalizedNetworkCount
	summary.PBFTNetworkQuorumReachedCount = preview.NetworkQuorumReachedCount
}

func applyCrossShardProtocolMetrics(summary *Summary, preview CrossShardProtocolPreview) {
	summary.CrossShardProtocolSelected = preview.ProtocolSelected
	summary.CrossShardTxCount = preview.TxCount
	if len(preview.TxRows) > 0 {
		summary.CrossShardRatio = round(float64(preview.TxCount) / float64(len(preview.TxRows)))
	}
	summary.CrossShardMessageCount = preview.MessageCount
	summary.RelayPreviewCount = preview.RelayPreviewCount
	summary.CrossShardCompletedCount = preview.CompletedCount
	summary.CrossShardFailedCount = preview.FailedCount
	summary.CrossShardAvgLatencyMS = preview.AvgLatencyMS
}

func writeArtifacts(out string, chainBytes, pluginBytes, experimentBytes []byte, summary Summary, blockLog []map[string]string, txResults []TxResult, commits []StateCommit, txPoolLog []TxPoolEvent, consensusLog []ConsensusRecord, routingLog []RoutingRecord, executionLog []ExecutionRecord, stateAccessLog []StateAccessRecord, nodeRuntime NodeRuntimeArtifacts, launcher LauncherPreview, networkAdapter NetworkAdapterPreview, consensusNetwork ConsensusNetworkLightPreview, pbftPreview PBFTPreview, pbftNetwork PBFTNetworkPreview, crossShard CrossShardProtocolPreview, title string) error {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "used_chain_profile.yaml"), chainBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "used_plugin_profile.yaml"), pluginBytes, 0o644); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "used_experiment_profile.yaml"), experimentBytes, 0o644); err != nil {
		return err
	}
	if err := writeSummaryCSV(filepath.Join(out, "summary.csv"), summary); err != nil {
		return err
	}
	jsonBytes, _ := json.MarshalIndent(summary, "", "  ")
	if err := os.WriteFile(filepath.Join(out, "summary.json"), jsonBytes, 0o644); err != nil {
		return err
	}
	if err := writeBlockLog(filepath.Join(out, "block_log.csv"), blockLog); err != nil {
		return err
	}
	if err := writeTxResults(filepath.Join(out, "tx_results.csv"), txResults); err != nil {
		return err
	}
	if err := writeStateCommitLog(filepath.Join(out, "state_commit_log.csv"), commits); err != nil {
		return err
	}
	if err := writeTxPoolLog(filepath.Join(out, "txpool_log.csv"), txPoolLog); err != nil {
		return err
	}
	if err := writeConsensusLog(filepath.Join(out, "consensus_log.csv"), consensusLog); err != nil {
		return err
	}
	if err := writeRoutingLog(filepath.Join(out, "routing_log.csv"), routingLog); err != nil {
		return err
	}
	if err := writeExecutionLog(filepath.Join(out, "execution_log.csv"), executionLog); err != nil {
		return err
	}
	if err := writeStateAccessLog(filepath.Join(out, "state_access_log.csv"), stateAccessLog); err != nil {
		return err
	}
	if err := writeNodeTopologyCSV(filepath.Join(out, "node_topology.csv"), nodeRuntime.Nodes); err != nil {
		return err
	}
	if err := writeNodeLogCSV(filepath.Join(out, "node_log.csv"), nodeRuntime.NodeEvents); err != nil {
		return err
	}
	if err := writeNetworkLogCSV(filepath.Join(out, "network_log.csv"), nodeRuntime.NetworkMessages); err != nil {
		return err
	}
	if err := writeConsensusMessageLogCSV(filepath.Join(out, "consensus_message_log.csv"), nodeRuntime.ConsensusMessages); err != nil {
		return err
	}
	if err := writeLauncherPreviewArtifacts(out, nodeRuntime, launcher); err != nil {
		return err
	}
	if err := WriteNetworkAdapterPreviewArtifacts(out, networkAdapter); err != nil {
		return err
	}
	if err := WriteConsensusNetworkLightArtifacts(out, consensusNetwork); err != nil {
		return err
	}
	if err := WritePBFTPreviewArtifacts(out, pbftPreview); err != nil {
		return err
	}
	if err := WritePBFTNetworkArtifacts(out, pbftNetwork); err != nil {
		return err
	}
	if err := WriteCrossShardProtocolArtifacts(out, crossShard); err != nil {
		return err
	}
	if len(launcher.Addresses) > 0 {
		_, err := RunNodeProcessPreview(NodeProcessPreviewInput{
			NodeID:       launcher.Addresses[0].NodeID,
			Role:         launcher.Addresses[0].Role,
			ShardID:      launcher.Addresses[0].ShardID,
			HasShardID:   true,
			TopologyFile: filepath.Join(out, "topology.json"),
			OutputDir:    out,
			PreviewOnly:  true,
		})
		if err != nil {
			return err
		}
	}
	report := "# " + title + "\n\nThis is V3.8 output generated from the V3.5 logical topology, V3.6 configurable NetworkAdapter typed message preview, V3.7 selectable ConsensusRuntime / PBFT preview boundary, and a V3.8 CrossShardProtocol skeleton under Routing/Sharding.\n\nIt keeps FIFO TxPool, BlockProducer, selectable ConsensusRuntime, Routing/Sharding, Execution, StateAccess, Commit, and node-level logical artifacts. CrossShardProtocol is not a new main transaction-flow module; it is a Routing/Sharding sub-capability.\n\nIt writes node_topology.csv, node_log.csv, network_log.csv, consensus_message_log.csv, node_address_table.csv, topology.json, launch_nodes_windows.bat, launch_nodes_linux.sh, launcher_readme.md, node_process_status.csv, node_process_manifest.json, node_process_log_sample.log, tcp_adapter_status.csv, network_send_log.csv, network_receive_log.csv, typed_message_log.csv, consensus_network_light_log.csv, network_consensus_summary.json, pbft_state_log.csv, pbft_message_log.csv, quorum_log.csv, finalized_block_log.csv, consensus_network_log.csv, pbft_network_summary.json, cross_shard_tx_log.csv, cross_shard_message_log.csv, relay_preview_log.csv, cross_shard_status.csv, and cross_shard_summary.json.\n\nThe V3.8 relay_preview skeleton detects cross-shard transactions from Routing/Sharding preview records, emits cross_shard_relay preview messages, records target receive preview, and marks preview_completed. It does not implement source shard locks, target real execution commits, atomic commit, state proofs, rollback, timeout recovery, broker middle accounts, or 2PC prepare/commit/abort.\n\nStatePlacement phi(key) maps each key to a persistent state storage unit. ExecutionRouting M_t routes a transaction to a logical execution shard. Co-access routing changes execution-side placement/routing; it does not migrate persistent state storage placement.\n\nIt is not a complete Relay protocol, not Broker, not 2PC, not atomic cross-shard commit, not cross-shard state proof, not production PBFT, not HotStuff, not Raft, not Fabric/EVM live execution, not a BlockEmulator backend, and not final paper-scale performance evidence.\n"
	if err := os.WriteFile(filepath.Join(out, "report.md"), []byte(report), 0o644); err != nil {
		return err
	}
	log := "v3 cross-shard protocol skeleton closure start\nruntime_mode=" + summary.RuntimeMode + "\ntruth_label=" + summary.TruthLabel + "\nnode_runtime_mode=" + nodeRuntime.Config.NodeRuntimeMode + "\nnetwork_mode=" + nodeRuntime.Config.NetworkMode + "\nnetwork_adapter_selected=" + summary.NetworkAdapterSelected + "\ntcp_preview_enabled=" + strconv.FormatBool(summary.TCPPreviewEnabled) + "\nlogical_node_count=" + strconv.Itoa(summary.LogicalNodeCount) + "\nlauncher_mode=" + summary.LauncherMode + "\nlaunchable_node_count=" + strconv.Itoa(summary.LaunchableNodeCount) + "\nlauncher_preview_only=true\nnode_process_entrypoint_available=true\nnode_process_preview_only=true\n" + networkAdapter.SummaryLine() + consensusNetwork.SummaryLine() + pbftPreview.SummaryLine() + pbftNetwork.SummaryLine() + crossShard.SummaryLine() + "txpool=fifo_pool\nblock_producer=time_or_count_block_producer\nconsensus_runtime_configurable=true\npbft_over_network_preview=true\ncross_shard_protocol_skeleton=true\natomic_cross_shard_commit=false\ncomplete_relay=false\ncomplete_broker=false\ncomplete_2pc=false\ncross_shard_proof=false\nrollback_timeout_recovery=false\nproduction_pbft=false\nview_change_hardening=false\ncheckpoint_hardening=false\nsignature_hardening=false\nrouting_plugin=" + summary.RoutingPlugin + "\nexecution_plugin=" + summary.ExecutionPlugin + "\nstate_access_plugin=" + summary.StateAccessPlugin + "\nfabric_live=false\nevm_live=false\nblockemulator_backend=false\nproduction_network=false\nreal_multi_process_runtime=false\nmetaflow=false\nreal_pbft=false\nhotstuff=false\nraft=false\nreal_cross_shard_protocol=false\nreal_concurrent_execution=false\nreal_rollback=false\nreal_remote_storage=false\nreal_proof_witness=false\nmpt=false\nstate_root=false\nsnapshot=false\npaper_grade_benchmark=false\nv3 cross-shard protocol skeleton closure done\n"
	return os.WriteFile(filepath.Join(out, "runtime.log"), []byte(log), 0o644)
}

func writeSummaryCSV(path string, s Summary) error {
	return writeCSV(path, summaryFields(), [][]string{{
		s.RunID, s.Stage, s.BackendType, s.TruthLabel, s.ChainProfileID, s.PluginProfileID, s.ExperimentProfileID,
		strconv.Itoa(s.TxCount), strconv.Itoa(s.SuccessCount), strconv.Itoa(s.FailureCount), strconv.Itoa(s.BlockCount),
		fmt.Sprint(s.ThroughputTPS), fmt.Sprint(s.AvgLatencyMS), fmt.Sprint(s.P95LatencyMS), fmt.Sprint(s.P99LatencyMS), s.RuntimeMode,
		strconv.Itoa(s.RemoteFetchCount), fmt.Sprint(s.CrossShardRatio), strconv.Itoa(s.FastTrackCount), strconv.Itoa(s.ConservativeTrackCount), strconv.Itoa(s.AggregatedUpdateCount), fmt.Sprint(s.AggregationRatio), strconv.Itoa(s.ConflictCount), fmt.Sprint(s.QueueWaitMS), strconv.Itoa(s.TxPoolAdmittedCount), strconv.Itoa(s.TxPoolRejectedCount), strconv.Itoa(s.TxPoolPeakSize), fmt.Sprint(s.TxPoolAvgWaitMS), fmt.Sprint(s.TxPoolP95WaitMS), strconv.Itoa(s.EmptyBlockCount), fmt.Sprint(s.AvgBlockSize), strconv.Itoa(s.MaxBlockSize), strconv.Itoa(s.BlockIntervalMS), fmt.Sprint(s.AvgBlockIntervalMS), strconv.Itoa(s.BlockProducerCountCut), strconv.Itoa(s.BlockProducerTimeCut), strconv.Itoa(s.BlockProducerDrainCut), strconv.Itoa(s.BlockProducerEmptyCut), fmt.Sprint(s.BlockCommitLatencyMS),
		fmt.Sprint(s.ConsensusLatencyMS), fmt.Sprint(s.AvgConsensusLatencyMS), fmt.Sprint(s.P95ConsensusLatencyMS), strconv.Itoa(s.ConsensusMessageCount), fmt.Sprint(s.AvgConsensusMessageCount), strconv.Itoa(s.ConsensusRoundCount), strconv.Itoa(s.ViewChangeCount), strconv.Itoa(s.FinalizedBlockCount), strconv.Itoa(s.FailedBlockCount),
		strconv.Itoa(s.RoutingDecisionCount), strconv.Itoa(s.CrossShardTxCount), strconv.Itoa(s.LocalTxCount), strconv.Itoa(s.RemoteStateAccessCount), fmt.Sprint(s.AvgTouchedShards), strconv.Itoa(s.MaxTouchedShards), strconv.Itoa(s.HotspotKeyCount), strconv.Itoa(s.CoaccessGroupCount), fmt.Sprint(s.AvgRoutingOverheadMS), s.RoutingPlugin,
		s.ExecutionPlugin, strconv.Itoa(s.ExecutionTxCount), strconv.Itoa(s.BlockedTxCount), strconv.Itoa(s.DependencyEdgeCount), fmt.Sprint(s.AvgDependencyEdgesPerTx), fmt.Sprint(s.AvgExecutionLatencyMS), fmt.Sprint(s.P95ExecutionLatencyMS), strconv.Itoa(s.MaxExecutionLatencyMS), strconv.Itoa(s.LogicalWorkerCount), strconv.Itoa(s.ParallelizableTxCount), strconv.Itoa(s.SerialTxCount),
		s.StateAccessPlugin, strconv.Itoa(s.StateAccessCount), strconv.Itoa(s.LocalStateAccessCount), strconv.Itoa(s.RemoteStateAccessCount), fmt.Sprint(s.RemoteStateAccessRatio), strconv.Itoa(s.CacheHitCount), strconv.Itoa(s.CacheMissCount), fmt.Sprint(s.CacheHitRate), strconv.Itoa(s.PrefetchHitCount), strconv.Itoa(s.PrefetchMissCount), fmt.Sprint(s.PrefetchHitRate), fmt.Sprint(s.AvgStateAccessLatencyMS), fmt.Sprint(s.P95StateAccessLatencyMS), strconv.Itoa(s.MaxStateAccessLatencyMS), fmt.Sprint(s.RemoteStateAccessLatencyMS), strconv.Itoa(s.WitnessEstimatedCount), strconv.Itoa(s.ProofEstimatedCount), strconv.Itoa(s.EstimatedWitnessBytes), strconv.Itoa(s.EstimatedProofBytes),
		s.CommitPlugin, strconv.Itoa(s.CommitTxCount), strconv.Itoa(s.CommitUpdateCount), strconv.Itoa(s.NormalCommitCount), strconv.Itoa(s.ConservativeCommitCount), strconv.Itoa(s.HotspotUpdateCount), strconv.Itoa(s.RawUpdateCount), strconv.Itoa(s.AggregationGroupCount), strconv.Itoa(s.ConstraintCheckCount), strconv.Itoa(s.ConstraintPassedCount), strconv.Itoa(s.ConstraintFailedCount), fmt.Sprint(s.AvgCommitLatencyMS), fmt.Sprint(s.P95CommitLatencyMS), strconv.Itoa(s.MaxCommitLatencyMS),
		strconv.Itoa(s.ExecutionShardCount), strconv.Itoa(s.StateStorageUnitCount), strconv.Itoa(s.CrossStateUnitAccessCount), strconv.Itoa(s.RemoteStateFetchCount), fmt.Sprint(s.StateLocalityRatio), fmt.Sprint(s.ExecutionShardLoadBalance), fmt.Sprint(s.StateUnitLoadBalance),
		strconv.Itoa(s.ShardCount), strconv.Itoa(s.ValidatorsPerShard), strconv.Itoa(s.LogicalNodeCount), strconv.Itoa(s.ValidatorNodeCount), strconv.Itoa(s.ExecutorNodeCount), strconv.Itoa(s.StorageNodeCount), strconv.Itoa(s.SupervisorNodeCount), strconv.Itoa(s.MessageCount), strconv.Itoa(s.NetworkMessageCount), strconv.Itoa(s.NodeEventCount),
		s.LauncherMode, strconv.Itoa(s.LauncherScriptCount), strconv.Itoa(s.LaunchableNodeCount), strconv.Itoa(s.NodeAddressCount), fmt.Sprint(s.WindowsLauncherAvailable), fmt.Sprint(s.LinuxLauncherAvailable), fmt.Sprint(s.LauncherPreviewOnly),
		fmt.Sprint(s.NodeProcessEntrypointAvailable), fmt.Sprint(s.NodeProcessPreviewAvailable), fmt.Sprint(s.NodeProcessStatusAvailable), fmt.Sprint(s.NodeProcessManifestAvailable), fmt.Sprint(s.NodeProcessPreviewOnly),
		s.NetworkAdapterSelected, fmt.Sprint(s.TCPPreviewEnabled), strconv.Itoa(s.TCPListenNodeCount), strconv.Itoa(s.TCPSendCount), strconv.Itoa(s.TCPReceiveCount), strconv.Itoa(s.TypedMessageCount), strconv.Itoa(s.NetworkErrorCount),
		fmt.Sprint(s.ConsensusOverNetworkEnabled), s.ConsensusRuntimeSelected, strconv.Itoa(s.ProposalPreviewCount), strconv.Itoa(s.VotePreviewCount), strconv.Itoa(s.LightQuorumReachedCount), strconv.Itoa(s.ConsensusNetworkErrorCount), s.ConsensusNetworkPath,
		strconv.Itoa(s.PBFTView), strconv.Itoa(s.PBFTSequence), strconv.Itoa(s.PBFTPrePrepareCount), strconv.Itoa(s.PBFTPrepareCount), strconv.Itoa(s.PBFTCommitCount), strconv.Itoa(s.PBFTQuorumReachedCount), strconv.Itoa(s.PBFTFinalizedBlockCount), strconv.Itoa(s.PBFTConsensusLatencyMS), fmt.Sprint(s.PBFTPreviewEnabled), strconv.Itoa(s.PBFTQuorumThreshold),
		fmt.Sprint(s.PBFTOverNetworkEnabled), s.PBFTNetworkPath, strconv.Itoa(s.PBFTNetworkMessageCount), strconv.Itoa(s.PBFTNetworkErrorCount), strconv.Itoa(s.PBFTPrePrepareNetworkCount), strconv.Itoa(s.PBFTPrepareNetworkCount), strconv.Itoa(s.PBFTCommitNetworkCount), strconv.Itoa(s.PBFTFinalizedNetworkCount), strconv.Itoa(s.PBFTNetworkQuorumReachedCount),
		s.CrossShardProtocolSelected, strconv.Itoa(s.CrossShardMessageCount), strconv.Itoa(s.RelayPreviewCount), strconv.Itoa(s.CrossShardCompletedCount), strconv.Itoa(s.CrossShardFailedCount), fmt.Sprint(s.CrossShardAvgLatencyMS),
	}})
}

func summaryFields() []string {
	return []string{"run_id", "stage", "backend_type", "truth_label", "chain_profile_id", "plugin_profile_id", "experiment_profile_id", "tx_count", "success_count", "failure_count", "block_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "runtime_mode", "remote_fetch_count", "cross_shard_ratio", "fast_track_count", "conservative_track_count", "aggregated_update_count", "aggregation_ratio", "conflict_count", "queue_wait_ms", "txpool_admitted_count", "txpool_rejected_count", "txpool_peak_size", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "empty_block_count", "avg_block_size", "max_block_size", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "blockproducer_empty_cut_count", "block_commit_latency_ms", "consensus_latency_ms", "avg_consensus_latency_ms", "p95_consensus_latency_ms", "consensus_message_count", "avg_consensus_message_count", "consensus_round_count", "view_change_count", "finalized_block_count", "failed_block_count", "routing_decision_count", "cross_shard_tx_count", "local_tx_count", "remote_state_access_count", "avg_touched_shards", "max_touched_shards", "hotspot_key_count", "coaccess_group_count", "avg_routing_overhead_ms", "routing_plugin", "execution_plugin", "execution_tx_count", "blocked_tx_count", "dependency_edge_count", "avg_dependency_edges_per_tx", "avg_execution_latency_ms", "p95_execution_latency_ms", "max_execution_latency_ms", "logical_worker_count", "parallelizable_tx_count", "serial_tx_count", "state_access_plugin", "state_access_count", "local_state_access_count", "remote_state_access_count", "remote_state_access_ratio", "cache_hit_count", "cache_miss_count", "cache_hit_rate", "prefetch_hit_count", "prefetch_miss_count", "prefetch_hit_rate", "avg_state_access_latency_ms", "p95_state_access_latency_ms", "max_state_access_latency_ms", "remote_state_access_latency_ms", "witness_estimated_count", "proof_estimated_count", "estimated_witness_bytes", "estimated_proof_bytes", "commit_plugin", "commit_tx_count", "commit_update_count", "normal_commit_count", "conservative_commit_count", "hotspot_update_count", "raw_update_count", "aggregation_group_count", "constraint_check_count", "constraint_passed_count", "constraint_failed_count", "avg_commit_latency_ms", "p95_commit_latency_ms", "max_commit_latency_ms", "execution_shard_count", "state_storage_unit_count", "cross_state_unit_access_count", "remote_state_fetch_count", "state_locality_ratio", "execution_shard_load_balance", "state_unit_load_balance", "shard_count", "validators_per_shard", "logical_node_count", "validator_node_count", "executor_node_count", "storage_node_count", "supervisor_node_count", "message_count", "network_message_count", "node_event_count", "launcher_mode", "launcher_script_count", "launchable_node_count", "node_address_count", "windows_launcher_available", "linux_launcher_available", "launcher_preview_only", "node_process_entrypoint_available", "node_process_preview_available", "node_process_status_available", "node_process_manifest_available", "node_process_preview_only", "network_adapter_selected", "tcp_preview_enabled", "tcp_listen_node_count", "tcp_send_count", "tcp_receive_count", "typed_message_count", "network_error_count", "consensus_over_network_enabled", "consensus_runtime_selected", "proposal_preview_count", "vote_preview_count", "light_quorum_reached_count", "consensus_network_error_count", "consensus_network_path", "pbft_view", "pbft_sequence", "pbft_preprepare_count", "pbft_prepare_count", "pbft_commit_count", "pbft_quorum_reached_count", "pbft_finalized_block_count", "pbft_consensus_latency_ms", "pbft_preview_enabled", "pbft_quorum_threshold", "pbft_over_network_enabled", "pbft_network_path", "pbft_network_message_count", "pbft_network_error_count", "pbft_preprepare_network_count", "pbft_prepare_network_count", "pbft_commit_network_count", "pbft_finalized_network_count", "pbft_network_quorum_reached_count", "cross_shard_protocol_selected", "cross_shard_message_count", "relay_preview_count", "cross_shard_completed_count", "cross_shard_failed_count", "cross_shard_avg_latency_ms"}
}

func writeBlockLog(path string, rows []map[string]string) error {
	fields := []string{"block_height", "block_id", "parent_hash", "block_hash", "proposer", "proposer_node", "tx_count", "cut_reason", "pool_size_before_cut", "pool_size_after_cut", "block_producer_plugin", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status", "consensus_domain_id", "validator_count", "execution_shard_count", "state_storage_unit_count"}
	out := [][]string{}
	for _, row := range rows {
		values := []string{}
		for _, field := range fields {
			values = append(values, row[field])
		}
		out = append(out, values)
	}
	return writeCSV(path, fields, out)
}

func writeTxResults(path string, txs []TxResult) error {
	fields := []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "consensus_domain_id", "execution_shard_id", "home_state_unit_ids", "accessed_state_unit_ids", "remote_state_unit_count", "remote_fetch_count", "cross_state_unit_access", "state_locality_hit", "read_count", "write_count"}
	rows := [][]string{}
	for _, tx := range txs {
		rows = append(rows, []string{tx.TxID, strconv.Itoa(tx.SubmitTimeMS), strconv.Itoa(tx.AdmitTimeMS), strconv.Itoa(tx.BlockHeight), strconv.Itoa(tx.ExecutionStartMS), strconv.Itoa(tx.ExecutionEndMS), strconv.Itoa(tx.CommitTimeMS), strconv.Itoa(tx.LatencyMS), tx.Status, strconv.Itoa(tx.ShardID), tx.ConsensusDomainID, strconv.Itoa(tx.ExecutionShardID), joinInts(tx.HomeStateUnitIDs), joinInts(tx.AccessedStateUnitIDs), strconv.Itoa(tx.RemoteStateUnitCount), strconv.Itoa(tx.RemoteFetchCount), strconv.FormatBool(tx.CrossStateUnitAccess), strconv.FormatBool(tx.StateLocalityHit), strconv.Itoa(tx.ReadCount), strconv.Itoa(tx.WriteCount)})
	}
	return writeCSV(path, fields, rows)
}

func writeStateCommitLog(path string, commits []StateCommit) error {
	fields := []string{"tx_id", "block_height", "tx_index", "commit_plugin", "commit_path", "state_key", "update_type", "is_hotspot", "aggregated", "aggregation_group_id", "raw_update_count", "aggregated_update_count", "constraint_checked", "constraint_passed", "commit_latency_ms", "reason", "old_value", "delta", "new_value", "commit_time_ms", "status", "state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"}
	rows := [][]string{}
	for _, c := range commits {
		rows = append(rows, []string{
			c.TxID,
			strconv.Itoa(c.BlockHeight),
			strconv.Itoa(c.TxIndex),
			c.CommitPlugin,
			c.CommitPath,
			c.StateKey,
			c.UpdateType,
			strconv.FormatBool(c.IsHotspot),
			strconv.FormatBool(c.Aggregated),
			c.AggregationGroupID,
			strconv.Itoa(c.RawUpdateCount),
			strconv.Itoa(c.AggregatedCount),
			strconv.FormatBool(c.ConstraintChecked),
			strconv.FormatBool(c.ConstraintPassed),
			strconv.Itoa(c.CommitLatencyMS),
			c.Reason,
			strconv.Itoa(c.OldValue),
			strconv.Itoa(c.Delta),
			strconv.Itoa(c.NewValue),
			strconv.Itoa(c.CommitTimeMS),
			c.Status,
			strconv.Itoa(c.StateStorageUnitID),
			strconv.Itoa(c.ExecutionShardID),
			strconv.FormatBool(c.IsRemoteCommit),
			c.PlacementPolicy,
			c.RoutingPlugin,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeTxPoolLog(path string, events []TxPoolEvent) error {
	fields := []string{"event_time_ms", "event_type", "tx_id", "block_height", "pool_size_before", "pool_size_after", "admitted_count", "selected_count", "rejected_count", "queue_wait_ms", "reason"}
	rows := [][]string{}
	for _, event := range events {
		rows = append(rows, []string{
			strconv.Itoa(event.EventTimeMS),
			event.EventType,
			event.TxID,
			strconv.Itoa(event.BlockHeight),
			strconv.Itoa(event.PoolSizeBefore),
			strconv.Itoa(event.PoolSizeAfter),
			strconv.Itoa(event.AdmittedCount),
			strconv.Itoa(event.SelectedCount),
			strconv.Itoa(event.RejectedCount),
			strconv.Itoa(event.QueueWaitMS),
			event.Reason,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeConsensusLog(path string, records []ConsensusRecord) error {
	fields := []string{"block_height", "block_hash", "consensus_plugin", "round_id", "view_id", "sequence_id", "leader_id", "validator_count", "fault_tolerance_f", "prepare_quorum", "commit_quorum", "preprepare_msg_count", "prepare_msg_count", "commit_msg_count", "total_message_count", "consensus_start_time_ms", "consensus_ordered_time_ms", "consensus_finalized_time_ms", "consensus_latency_ms", "finalized", "view_change_count", "reason"}
	rows := [][]string{}
	for _, record := range records {
		rows = append(rows, []string{
			strconv.Itoa(record.BlockHeight),
			record.BlockHash,
			record.PluginID,
			strconv.Itoa(record.RoundID),
			strconv.Itoa(record.ViewID),
			strconv.Itoa(record.SequenceID),
			record.LeaderID,
			strconv.Itoa(record.ValidatorCount),
			strconv.Itoa(record.FaultToleranceF),
			strconv.Itoa(record.PrepareQuorum),
			strconv.Itoa(record.CommitQuorum),
			strconv.Itoa(record.PrePrepareMsgCount),
			strconv.Itoa(record.PrepareMsgCount),
			strconv.Itoa(record.CommitMsgCount),
			strconv.Itoa(record.TotalMessageCount),
			strconv.Itoa(record.ConsensusStartTimeMS),
			strconv.Itoa(record.ConsensusOrderedTimeMS),
			strconv.Itoa(record.ConsensusFinalizedTimeMS),
			strconv.Itoa(record.ConsensusLatencyMS),
			strconv.FormatBool(record.Finalized),
			strconv.Itoa(record.ViewChangeCount),
			record.Reason,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeRoutingLog(path string, records []RoutingRecord) error {
	fields := []string{"tx_id", "block_height", "tx_index", "routing_plugin", "access_key_count", "read_key_count", "write_key_count", "primary_shard", "touched_shards", "touched_shard_count", "cross_shard", "remote_state_access_estimate", "hotspot_key_count", "coaccess_group_id", "routing_overhead_ms", "reason"}
	rows := [][]string{}
	for _, record := range records {
		rows = append(rows, []string{
			record.TxID,
			strconv.Itoa(record.BlockHeight),
			strconv.Itoa(record.TxIndex),
			record.RoutingPlugin,
			strconv.Itoa(record.AccessKeyCount),
			strconv.Itoa(record.ReadKeyCount),
			strconv.Itoa(record.WriteKeyCount),
			strconv.Itoa(record.PrimaryShard),
			joinInts(record.TouchedShards),
			strconv.Itoa(record.TouchedShardCount),
			strconv.FormatBool(record.CrossShard),
			strconv.Itoa(record.RemoteStateAccessEstimate),
			strconv.Itoa(record.HotspotKeyCount),
			record.CoaccessGroupID,
			strconv.Itoa(record.RoutingOverheadMS),
			record.Reason,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeExecutionLog(path string, records []ExecutionRecord) error {
	fields := []string{"tx_id", "block_height", "tx_index", "execution_plugin", "track", "access_key_count", "read_key_count", "write_key_count", "dependency_edge_count", "dependency_risk", "ready_time_ms", "start_time_ms", "end_time_ms", "execution_latency_ms", "blocked", "block_reason", "worker_id", "reason"}
	rows := [][]string{}
	for _, record := range records {
		rows = append(rows, []string{
			record.TxID,
			strconv.Itoa(record.BlockHeight),
			strconv.Itoa(record.TxIndex),
			record.ExecutionPlugin,
			record.Track,
			strconv.Itoa(record.AccessKeyCount),
			strconv.Itoa(record.ReadKeyCount),
			strconv.Itoa(record.WriteKeyCount),
			strconv.Itoa(record.DependencyEdgeCount),
			record.DependencyRisk,
			strconv.Itoa(record.ReadyAtMS),
			strconv.Itoa(record.StartAtMS),
			strconv.Itoa(record.EndAtMS),
			strconv.Itoa(record.ExecutionLatencyMS),
			strconv.FormatBool(record.Blocked),
			record.BlockReason,
			strconv.Itoa(record.WorkerID),
			record.Reason,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeStateAccessLog(path string, records []StateAccessRecord) error {
	fields := []string{"tx_id", "block_height", "tx_index", "state_access_plugin", "access_key", "access_type", "is_read", "is_write", "home_shard", "execution_shard", "is_remote", "cache_hit", "prefetched", "witness_estimated", "proof_estimated", "access_latency_ms", "reason"}
	rows := [][]string{}
	for _, record := range records {
		rows = append(rows, []string{
			record.TxID,
			strconv.Itoa(record.BlockHeight),
			strconv.Itoa(record.TxIndex),
			record.StateAccessPlugin,
			record.AccessKey,
			record.AccessType,
			strconv.FormatBool(record.IsRead),
			strconv.FormatBool(record.IsWrite),
			strconv.Itoa(record.HomeShard),
			strconv.Itoa(record.ExecutionShard),
			strconv.FormatBool(record.IsRemote),
			strconv.FormatBool(record.CacheHit),
			strconv.FormatBool(record.Prefetched),
			strconv.FormatBool(record.WitnessEstimated),
			strconv.FormatBool(record.ProofEstimated),
			strconv.Itoa(record.AccessLatencyMS),
			record.Reason,
		})
	}
	return writeCSV(path, fields, rows)
}

func writeCSV(path string, fields []string, rows [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	if err := writer.Write(fields); err != nil {
		return err
	}
	for _, row := range rows {
		if err := writer.Write(row); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

func buildRoutingMap(txs []PooledTransaction, chain ChainProfile, plugin PluginProfile) map[string]int {
	keys := []string{}
	for _, pooledTx := range txs {
		keys = append(keys, pooledTx.Tx.ReadKeys...)
	}
	keys = unique(keys)
	result := map[string]int{}
	if plugin.ShardingPlugin != "co_access_sharding" {
		for _, key := range keys {
			result[key] = shard(key, chain.ExecutionShardCount)
		}
		return result
	}
	groupShard := 0
	for _, key := range keys {
		if strings.HasPrefix(key, "asset_0") || strings.HasPrefix(key, "asset_1") || strings.HasPrefix(key, "asset_2") || strings.HasPrefix(key, "asset_3") {
			result[key] = 0
			continue
		}
		result[key] = groupShard % max(1, chain.ExecutionShardCount)
		groupShard++
	}
	return result
}

func assignTxShard(tx Transaction, routingMap map[string]int, chain ChainProfile) int {
	if len(tx.ReadKeys) == 0 {
		return shard(tx.ID, chain.ExecutionShardCount)
	}
	return routingMap[tx.ReadKeys[0]]
}

func remoteFetchCount(remoteStateUnits int, plugin PluginProfile) int {
	if plugin.StateAccessPlugin == "access_list_prefetch" {
		return 0
	}
	return remoteStateUnits
}

func accessedStateUnits(tx Transaction, chain ChainProfile) []int {
	keys := append([]string(nil), tx.ReadKeys...)
	for key := range tx.WriteDeltas {
		keys = append(keys, key)
	}
	units := []int{}
	seen := map[int]bool{}
	for _, key := range keys {
		unitID := stateUnit(key, chain)
		if seen[unitID] {
			continue
		}
		seen[unitID] = true
		units = append(units, unitID)
	}
	sort.Ints(units)
	return units
}

func writeStateUnits(tx Transaction, chain ChainProfile) []int {
	units := []int{}
	seen := map[int]bool{}
	for key := range tx.WriteDeltas {
		unitID := stateUnit(key, chain)
		if seen[unitID] {
			continue
		}
		seen[unitID] = true
		units = append(units, unitID)
	}
	sort.Ints(units)
	return units
}

func remoteStateUnitCount(units []int, executionShardID int) int {
	remote := 0
	for _, unitID := range units {
		if unitID != executionShardID {
			remote++
		}
	}
	return remote
}

func classifyTrack(tx Transaction, plugin PluginProfile) string {
	if plugin.ExecutionSchedulerPlugin != "dual_track_execution" {
		return "conservative"
	}
	if len(tx.ReadKeys) <= 2 && tx.ConflictHint != "high" {
		return "fast"
	}
	return "conservative"
}

func runtimeMode(plugin PluginProfile) string {
	if plugin.ProfileID == "v3_2_minimal_single_chain" {
		return "go_backed_minimal_runtime"
	}
	return "go_backed_metatrack_runtime"
}

func unique(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func collectionItem(text, profileID string) string {
	lines := strings.Split(text, "\n")
	start := -1
	for i, line := range lines {
		if strings.Contains(line, "plugin_profile_id: "+profileID) {
			start = i
			break
		}
	}
	if start < 0 {
		return ""
	}
	end := len(lines)
	for i := start + 1; i < len(lines); i++ {
		if strings.HasPrefix(lines[i], "  - plugin_profile_id:") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

func fieldString(text, field, fallback string) string {
	matches := regexp.MustCompile(regexp.QuoteMeta(field) + `:\s*([^#\n\r]+)`).FindStringSubmatch(text)
	if len(matches) != 2 {
		return fallback
	}
	return strings.Trim(strings.TrimSpace(matches[1]), `"'`)
}

func sectionFieldString(text, section, field, fallback string) string {
	return fieldString(sectionText(text, section), field, fallback)
}

func sectionFieldInt(text, section, field string, fallback int) int {
	value, err := strconv.Atoi(sectionFieldString(text, section, field, ""))
	if err != nil {
		return fallback
	}
	return value
}

func sectionFieldBool(text, section, field string, fallback bool) bool {
	value := sectionFieldString(text, section, field, "")
	if value == "" {
		return fallback
	}
	return value == "true"
}

func sectionText(text, section string) string {
	lines := strings.Split(text, "\n")
	start := -1
	for i, line := range lines {
		if strings.TrimSpace(line) == section+":" && !strings.HasPrefix(line, " ") && !strings.HasPrefix(line, "\t") {
			start = i + 1
			break
		}
	}
	if start < 0 {
		return ""
	}
	end := len(lines)
	for i := start; i < len(lines); i++ {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}
		if !strings.HasPrefix(lines[i], " ") && !strings.HasPrefix(lines[i], "\t") {
			end = i
			break
		}
	}
	return strings.Join(lines[start:end], "\n")
}

func fieldInt(text, field string, fallback int) int {
	value, err := strconv.Atoi(fieldString(text, field, ""))
	if err != nil {
		return fallback
	}
	return value
}

func fieldFloat(text, field string, fallback float64) float64 {
	value, err := strconv.ParseFloat(fieldString(text, field, ""), 64)
	if err != nil {
		return fallback
	}
	return value
}

func fieldBool(text, field string, fallback bool) bool {
	value := fieldString(text, field, "")
	if value == "" {
		return fallback
	}
	return value == "true"
}

func shard(key string, shards int) int {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(max(1, shards)))
}

func stateUnit(key string, chain ChainProfile) int {
	return shard(key, chain.StateStorageUnitCount)
}

func consensusDomainID(index int) string {
	return fmt.Sprintf("consensus_%d", index)
}

func joinInts(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ";")
}

func loadBalance(loads map[int]int, bucketCount int) float64 {
	if bucketCount <= 0 {
		return 0
	}
	minLoad := int(^uint(0) >> 1)
	maxLoad := 0
	total := 0
	for index := 0; index < bucketCount; index++ {
		value := loads[index]
		if value < minLoad {
			minLoad = value
		}
		if value > maxLoad {
			maxLoad = value
		}
		total += value
	}
	if total == 0 || maxLoad == 0 {
		return 1
	}
	return round(float64(minLoad) / float64(maxLoad))
}

func avg(values []int) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0
	for _, value := range values {
		total += value
	}
	return float64(total) / float64(len(values))
}

func percentileInt(values []int, pct int) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	index := int(math.Round((float64(pct) / 100) * float64(len(sorted)-1)))
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return float64(sorted[index])
}

func round(value float64) float64 {
	return math.Round(value*1000000) / 1000000
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxInt(values []int) int {
	result := 0
	for _, value := range values {
		if value > result {
			result = value
		}
	}
	return result
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
