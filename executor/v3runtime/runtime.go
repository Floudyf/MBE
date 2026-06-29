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
	ProfileID             string
	NodeIDPrefix          string
	NodeCount             int
	ValidatorCount        int
	ConsensusDomainCount  int
	BlockIntervalMS       int
	MaxTxPerBlock         int
	EmptyBlockEnabled     bool
	ShardCount            int
	ExecutionShardCount   int
	StateStorageUnitCount int
	StatePlacementPolicy  string
	StateBackend          string
	RemoteFetchCostMS     int
	RoutingPlugin         string
	RoutingScope          string
	NetworkPlugin         string
	NetworkBaseDelayMS    int
	KeyCount              int
	MaxPoolSize           int
	DedupEnabled          bool
	BackpressurePolicy    string
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
	TxID               string
	StateKey           string
	OldValue           int
	Delta              int
	NewValue           int
	CommitPlugin       string
	CommitTimeMS       int
	Status             string
	StateStorageUnitID int
	ExecutionShardID   int
	IsRemoteCommit     bool
	PlacementPolicy    string
	RoutingPlugin      string
}

type Summary struct {
	RunID                     string  `json:"run_id"`
	Stage                     string  `json:"stage"`
	BackendType               string  `json:"backend_type"`
	TruthLabel                string  `json:"truth_label"`
	ChainProfileID            string  `json:"chain_profile_id"`
	PluginProfileID           string  `json:"plugin_profile_id"`
	ExperimentProfileID       string  `json:"experiment_profile_id"`
	TxCount                   int     `json:"tx_count"`
	SuccessCount              int     `json:"success_count"`
	FailureCount              int     `json:"failure_count"`
	BlockCount                int     `json:"block_count"`
	ThroughputTPS             float64 `json:"throughput_tps"`
	AvgLatencyMS              float64 `json:"avg_latency_ms"`
	P95LatencyMS              float64 `json:"p95_latency_ms"`
	P99LatencyMS              float64 `json:"p99_latency_ms"`
	RuntimeMode               string  `json:"runtime_mode"`
	RemoteFetchCount          int     `json:"remote_fetch_count"`
	CrossShardRatio           float64 `json:"cross_shard_ratio"`
	FastTrackCount            int     `json:"fast_track_count"`
	ConservativeTrackCount    int     `json:"conservative_track_count"`
	AggregatedUpdateCount     int     `json:"aggregated_update_count"`
	AggregationRatio          float64 `json:"aggregation_ratio"`
	ConflictCount             int     `json:"conflict_count"`
	QueueWaitMS               float64 `json:"queue_wait_ms"`
	TxPoolAdmittedCount       int     `json:"txpool_admitted_count"`
	TxPoolRejectedCount       int     `json:"txpool_rejected_count"`
	TxPoolPeakSize            int     `json:"txpool_peak_size"`
	TxPoolAvgWaitMS           float64 `json:"txpool_avg_wait_ms"`
	TxPoolP95WaitMS           float64 `json:"txpool_p95_wait_ms"`
	EmptyBlockCount           int     `json:"empty_block_count"`
	AvgBlockSize              float64 `json:"avg_block_size"`
	MaxBlockSize              int     `json:"max_block_size"`
	BlockIntervalMS           int     `json:"block_interval_ms"`
	AvgBlockIntervalMS        float64 `json:"avg_block_interval_ms"`
	BlockProducerCountCut     int     `json:"blockproducer_count_cut_count"`
	BlockProducerTimeCut      int     `json:"blockproducer_time_cut_count"`
	BlockProducerDrainCut     int     `json:"blockproducer_drain_cut_count"`
	BlockProducerEmptyCut     int     `json:"blockproducer_empty_cut_count"`
	BlockCommitLatencyMS      float64 `json:"block_commit_latency_ms"`
	ExecutionShardCount       int     `json:"execution_shard_count"`
	StateStorageUnitCount     int     `json:"state_storage_unit_count"`
	CrossStateUnitAccessCount int     `json:"cross_state_unit_access_count"`
	RemoteStateFetchCount     int     `json:"remote_state_fetch_count"`
	StateLocalityRatio        float64 `json:"state_locality_ratio"`
	ExecutionShardLoadBalance float64 `json:"execution_shard_load_balance"`
	StateUnitLoadBalance      float64 `json:"state_unit_load_balance"`
}

type Result struct {
	OutputDir      string
	Summary        Summary
	BlockLog       []map[string]string
	TxResults      []TxResult
	StateCommitLog []StateCommit
	TxPoolLog      []TxPoolEvent
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
	blocks := produceBlocksFromTxPool(txs, chain, txPool, blockProducer)
	state := map[string]int{}
	blockLog := []map[string]string{}
	txResults := []TxResult{}
	stateCommits := []StateCommit{}
	for _, block := range blocks {
		proposer := fmt.Sprintf("%s_%d", chain.NodeIDPrefix, (block.Height-1)/max(1, chain.ValidatorCount))
		proposer = fmt.Sprintf("%s_%d", chain.NodeIDPrefix, (block.Height-1)%max(1, chain.ValidatorCount))
		ordered := block.CutTimeMS + 1
		finalized := ordered + 1
		blockLog = append(blockLog, map[string]string{
			"block_height":             strconv.Itoa(block.Height),
			"block_id":                 block.ID,
			"parent_hash":              block.ParentHash,
			"block_hash":               block.Hash,
			"proposer":                 proposer,
			"proposer_node":            proposer,
			"tx_count":                 strconv.Itoa(len(block.Txs)),
			"cut_reason":               block.CutReason,
			"pool_size_before_cut":     strconv.Itoa(block.PoolSizeBeforeCut),
			"pool_size_after_cut":      strconv.Itoa(block.PoolSizeAfterCut),
			"block_producer_plugin":    block.ProducerPlugin,
			"cut_time_ms":              strconv.Itoa(block.CutTimeMS),
			"ordered_time_ms":          strconv.Itoa(ordered),
			"finalized_time_ms":        strconv.Itoa(finalized),
			"consensus_plugin":         "simple_leader",
			"status":                   "finalized",
			"consensus_domain_id":      consensusDomainID(0),
			"validator_count":          strconv.Itoa(chain.ValidatorCount),
			"execution_shard_count":    strconv.Itoa(chain.ExecutionShardCount),
			"state_storage_unit_count": strconv.Itoa(chain.StateStorageUnitCount),
		})
		cursor := finalized
		routingMap := buildRoutingMap(block.Txs, chain, plugin)
		for _, pooledTx := range block.Txs {
			tx := pooledTx.Tx
			executionShardID := assignTxShard(tx, routingMap, chain)
			accessedUnits := accessedStateUnits(tx, chain)
			homeUnits := writeStateUnits(tx, chain)
			remoteStateUnits := remoteStateUnitCount(accessedUnits, executionShardID)
			track := classifyTrack(tx, plugin)
			start := cursor
			end := start + 1
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
				RemoteFetchCount:     remoteFetchCount(remoteStateUnits, plugin),
				Track:                track,
				Deltas:               deltas,
			}
			txResults = append(txResults, result)
			for key, values := range deltas {
				state[key] = values[2]
				stateCommits = append(stateCommits, StateCommit{
					BlockHeight:        block.Height,
					TxID:               tx.ID,
					StateKey:           key,
					OldValue:           values[0],
					Delta:              values[1],
					NewValue:           values[2],
					CommitPlugin:       "normal_commit",
					CommitTimeMS:       commitTime,
					Status:             "success",
					StateStorageUnitID: stateUnit(key, chain),
					ExecutionShardID:   executionShardID,
					IsRemoteCommit:     stateUnit(key, chain) != executionShardID,
					PlacementPolicy:    chain.StatePlacementPolicy,
					RoutingPlugin:      plugin.ShardingPlugin,
				})
			}
			cursor = end
		}
	}
	summary := buildSummary(runID, experiment, chain, plugin.ProfileID, txResults, len(blocks), runtimeMode(plugin))
	applyMechanismMetrics(&summary, txResults, plugin, chain)
	applyTxPoolMetrics(&summary, txPool)
	applyBlockProducerMetrics(&summary, blockProducer)
	if err := writeArtifacts(input.OutputDir, chainBytes, pluginBytes, experimentBytes, summary, blockLog, txResults, stateCommits, txPool.events, "V3.4.2 Go-backed BlockProducer runtime hardening run"); err != nil {
		return Result{}, err
	}
	return Result{OutputDir: input.OutputDir, Summary: summary, BlockLog: blockLog, TxResults: txResults, StateCommitLog: stateCommits, TxPoolLog: txPool.events, FinalState: state}, nil
}

func parseChainProfile(text string) ChainProfile {
	executionShardCount := sectionFieldInt(text, "execution", "shard_count", sectionFieldInt(text, "sharding", "shard_count", fieldInt(text, "shard_count", 4)))
	stateStorageUnitCount := sectionFieldInt(text, "state", "storage_unit_count", sectionFieldInt(text, "sharding", "shard_count", executionShardCount))
	return ChainProfile{
		ProfileID:             fieldString(text, "profile_id", "chain_x_default"),
		NodeIDPrefix:          fieldString(text, "node_id_prefix", "node"),
		NodeCount:             sectionFieldInt(text, "deployment", "node_count", fieldInt(text, "node_count", 4)),
		ValidatorCount:        sectionFieldInt(text, "consensus", "validator_count", sectionFieldInt(text, "deployment", "validator_count", fieldInt(text, "validator_count", 4))),
		ConsensusDomainCount:  sectionFieldInt(text, "consensus", "domain_count", 1),
		BlockIntervalMS:       fieldInt(text, "block_interval_ms", 100),
		MaxTxPerBlock:         fieldInt(text, "max_tx_per_block", 500),
		EmptyBlockEnabled:     sectionFieldBool(text, "block", "empty_block_enabled", fieldBool(text, "empty_block_enabled", false)),
		ShardCount:            executionShardCount,
		ExecutionShardCount:   executionShardCount,
		StateStorageUnitCount: stateStorageUnitCount,
		StatePlacementPolicy:  sectionFieldString(text, "state", "placement_policy", "hash_state_storage"),
		StateBackend:          sectionFieldString(text, "state", "backend", "memory"),
		RemoteFetchCostMS:     sectionFieldInt(text, "state", "remote_fetch_cost_ms", 1),
		RoutingPlugin:         sectionFieldString(text, "routing", "plugin", sectionFieldString(text, "sharding", "plugin", "hash_sharding")),
		RoutingScope:          sectionFieldString(text, "routing", "routing_scope", "execution_shard"),
		NetworkPlugin:         sectionFieldString(text, "network", "plugin", "fixed_delay"),
		NetworkBaseDelayMS:    sectionFieldInt(text, "network", "base_delay_ms", sectionFieldInt(text, "network", "delay_ms", 0)),
		KeyCount:              fieldInt(text, "key_count", 100000),
		MaxPoolSize:           fieldInt(text, "max_pool_size", 100000),
		DedupEnabled:          fieldBool(text, "dedup_enabled", true),
		BackpressurePolicy:    fieldString(text, "backpressure_policy", "reject"),
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
	}
}

func requireSupportedPlugins(plugin PluginProfile) error {
	baseExpected := map[string]string{
		"TxPoolPlugin":    "fifo_pool",
		"BlockProducer":   "time_or_count_block_producer",
		"ConsensusPlugin": "simple_leader",
		"MetricsPlugin":   "basic_metrics",
	}
	actual := map[string]string{
		"TxPoolPlugin":             plugin.TxPoolPlugin,
		"BlockProducer":            plugin.BlockProducer,
		"ConsensusPlugin":          plugin.ConsensusPlugin,
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
		"ShardingPlugin":           {"hash_sharding": true, "co_access_sharding": true},
		"ExecutionSchedulerPlugin": {"serial_execution": true, "dual_track_execution": true},
		"StateAccessPlugin":        {"direct_fetch": true, "access_list_prefetch": true},
		"CommitPlugin":             {"normal_commit": true, "hot_update_aggregation_commit": true},
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

func writeArtifacts(out string, chainBytes, pluginBytes, experimentBytes []byte, summary Summary, blockLog []map[string]string, txResults []TxResult, commits []StateCommit, txPoolLog []TxPoolEvent, title string) error {
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
	report := "# " + title + "\n\nThis is V3.4.2 role-separated Go-backed single-chain research runtime smoke output with FIFO TxPool and BlockProducer hardening.\n\nIt separates ConsensusDomain, ExecutionShard, StateStorageUnit, StatePlacement, ExecutionRouting, local FIFO TxPool admission/selection behavior, and local logical BlockProducer cut behavior.\n\nStatePlacement phi(key) maps each key to a persistent state storage unit. ExecutionRouting M_t routes a transaction to a logical execution shard. Co-access routing changes execution-side placement/routing; it does not migrate persistent state storage placement.\n\nThis is not Fabric live execution, not MetaTrack final evidence, not a multi-node network emulator, and not final paper-scale performance evidence.\n"
	if err := os.WriteFile(filepath.Join(out, "report.md"), []byte(report), 0o644); err != nil {
		return err
	}
	log := "v3 go-backed runtime start\nruntime_mode=" + summary.RuntimeMode + "\ntruth_label=" + summary.TruthLabel + "\ntxpool=fifo_pool\nblock_producer=time_or_count_block_producer\nfabric_live=false\nmetaflow=false\npbft=false\nhotstuff=false\nraft=false\nmulti_node_network=false\nv3 go-backed runtime done\n"
	return os.WriteFile(filepath.Join(out, "runtime.log"), []byte(log), 0o644)
}

func writeSummaryCSV(path string, s Summary) error {
	return writeCSV(path, summaryFields(), [][]string{{
		s.RunID, s.Stage, s.BackendType, s.TruthLabel, s.ChainProfileID, s.PluginProfileID, s.ExperimentProfileID,
		strconv.Itoa(s.TxCount), strconv.Itoa(s.SuccessCount), strconv.Itoa(s.FailureCount), strconv.Itoa(s.BlockCount),
		fmt.Sprint(s.ThroughputTPS), fmt.Sprint(s.AvgLatencyMS), fmt.Sprint(s.P95LatencyMS), fmt.Sprint(s.P99LatencyMS), s.RuntimeMode,
		strconv.Itoa(s.RemoteFetchCount), fmt.Sprint(s.CrossShardRatio), strconv.Itoa(s.FastTrackCount), strconv.Itoa(s.ConservativeTrackCount), strconv.Itoa(s.AggregatedUpdateCount), fmt.Sprint(s.AggregationRatio), strconv.Itoa(s.ConflictCount), fmt.Sprint(s.QueueWaitMS), strconv.Itoa(s.TxPoolAdmittedCount), strconv.Itoa(s.TxPoolRejectedCount), strconv.Itoa(s.TxPoolPeakSize), fmt.Sprint(s.TxPoolAvgWaitMS), fmt.Sprint(s.TxPoolP95WaitMS), strconv.Itoa(s.EmptyBlockCount), fmt.Sprint(s.AvgBlockSize), strconv.Itoa(s.MaxBlockSize), strconv.Itoa(s.BlockIntervalMS), fmt.Sprint(s.AvgBlockIntervalMS), strconv.Itoa(s.BlockProducerCountCut), strconv.Itoa(s.BlockProducerTimeCut), strconv.Itoa(s.BlockProducerDrainCut), strconv.Itoa(s.BlockProducerEmptyCut), fmt.Sprint(s.BlockCommitLatencyMS),
		strconv.Itoa(s.ExecutionShardCount), strconv.Itoa(s.StateStorageUnitCount), strconv.Itoa(s.CrossStateUnitAccessCount), strconv.Itoa(s.RemoteStateFetchCount), fmt.Sprint(s.StateLocalityRatio), fmt.Sprint(s.ExecutionShardLoadBalance), fmt.Sprint(s.StateUnitLoadBalance),
	}})
}

func summaryFields() []string {
	return []string{"run_id", "stage", "backend_type", "truth_label", "chain_profile_id", "plugin_profile_id", "experiment_profile_id", "tx_count", "success_count", "failure_count", "block_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "runtime_mode", "remote_fetch_count", "cross_shard_ratio", "fast_track_count", "conservative_track_count", "aggregated_update_count", "aggregation_ratio", "conflict_count", "queue_wait_ms", "txpool_admitted_count", "txpool_rejected_count", "txpool_peak_size", "txpool_avg_wait_ms", "txpool_p95_wait_ms", "empty_block_count", "avg_block_size", "max_block_size", "block_interval_ms", "avg_block_interval_ms", "blockproducer_count_cut_count", "blockproducer_time_cut_count", "blockproducer_drain_cut_count", "blockproducer_empty_cut_count", "block_commit_latency_ms", "execution_shard_count", "state_storage_unit_count", "cross_state_unit_access_count", "remote_state_fetch_count", "state_locality_ratio", "execution_shard_load_balance", "state_unit_load_balance"}
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
	fields := []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status", "state_storage_unit_id", "execution_shard_id", "is_remote_commit", "placement_policy", "routing_plugin"}
	rows := [][]string{}
	for _, c := range commits {
		rows = append(rows, []string{strconv.Itoa(c.BlockHeight), c.TxID, c.StateKey, strconv.Itoa(c.OldValue), strconv.Itoa(c.Delta), strconv.Itoa(c.NewValue), c.CommitPlugin, strconv.Itoa(c.CommitTimeMS), c.Status, strconv.Itoa(c.StateStorageUnitID), strconv.Itoa(c.ExecutionShardID), strconv.FormatBool(c.IsRemoteCommit), c.PlacementPolicy, c.RoutingPlugin})
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
