package v3runtime

import (
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
	ProfileID       string
	NodeIDPrefix    string
	NodeCount       int
	ValidatorCount  int
	BlockIntervalMS int
	MaxTxPerBlock   int
	ShardCount      int
	KeyCount        int
	MaxPoolSize     int
	DedupEnabled    bool
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
	Height    int
	ID        string
	Txs       []Transaction
	CutTimeMS int
}

type TxResult struct {
	TxID             string
	SubmitTimeMS     int
	AdmitTimeMS      int
	BlockHeight      int
	ExecutionStartMS int
	ExecutionEndMS   int
	CommitTimeMS     int
	LatencyMS        int
	Status           string
	ShardID          int
	ReadCount        int
	WriteCount       int
	RemoteFetchCount int
	Track            string
	Deltas           map[string][3]int
}

type StateCommit struct {
	BlockHeight  int
	TxID         string
	StateKey     string
	OldValue     int
	Delta        int
	NewValue     int
	CommitPlugin string
	CommitTimeMS int
	Status       string
}

type Summary struct {
	RunID                  string  `json:"run_id"`
	Stage                  string  `json:"stage"`
	BackendType            string  `json:"backend_type"`
	TruthLabel             string  `json:"truth_label"`
	ChainProfileID         string  `json:"chain_profile_id"`
	PluginProfileID        string  `json:"plugin_profile_id"`
	ExperimentProfileID    string  `json:"experiment_profile_id"`
	TxCount                int     `json:"tx_count"`
	SuccessCount           int     `json:"success_count"`
	FailureCount           int     `json:"failure_count"`
	BlockCount             int     `json:"block_count"`
	ThroughputTPS          float64 `json:"throughput_tps"`
	AvgLatencyMS           float64 `json:"avg_latency_ms"`
	P95LatencyMS           float64 `json:"p95_latency_ms"`
	P99LatencyMS           float64 `json:"p99_latency_ms"`
	RuntimeMode            string  `json:"runtime_mode"`
	RemoteFetchCount       int     `json:"remote_fetch_count"`
	CrossShardRatio        float64 `json:"cross_shard_ratio"`
	FastTrackCount         int     `json:"fast_track_count"`
	ConservativeTrackCount int     `json:"conservative_track_count"`
	AggregatedUpdateCount  int     `json:"aggregated_update_count"`
	AggregationRatio       float64 `json:"aggregation_ratio"`
	ConflictCount          int     `json:"conflict_count"`
	QueueWaitMS            float64 `json:"queue_wait_ms"`
	BlockCommitLatencyMS   float64 `json:"block_commit_latency_ms"`
}

type Result struct {
	OutputDir      string
	Summary        Summary
	BlockLog       []map[string]string
	TxResults      []TxResult
	StateCommitLog []StateCommit
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
	blocks := cutBlocks(txs, chain)
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
			"block_height":      strconv.Itoa(block.Height),
			"block_id":          block.ID,
			"proposer_node":     proposer,
			"tx_count":          strconv.Itoa(len(block.Txs)),
			"cut_time_ms":       strconv.Itoa(block.CutTimeMS),
			"ordered_time_ms":   strconv.Itoa(ordered),
			"finalized_time_ms": strconv.Itoa(finalized),
			"consensus_plugin":  "simple_leader",
			"status":            "finalized",
		})
		cursor := finalized
		routingMap := buildRoutingMap(block.Txs, chain, plugin)
		for _, tx := range block.Txs {
			shardID := assignTxShard(tx, routingMap, chain)
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
				TxID:             tx.ID,
				SubmitTimeMS:     tx.SubmitTimeMS,
				AdmitTimeMS:      tx.SubmitTimeMS,
				BlockHeight:      block.Height,
				ExecutionStartMS: start,
				ExecutionEndMS:   end,
				CommitTimeMS:     commitTime,
				LatencyMS:        commitTime - tx.SubmitTimeMS,
				Status:           "success",
				ShardID:          shardID,
				ReadCount:        len(tx.ReadKeys),
				WriteCount:       len(tx.WriteDeltas),
				RemoteFetchCount: remoteFetchCount(tx, shardID, routingMap, chain, plugin),
				Track:            track,
				Deltas:           deltas,
			}
			txResults = append(txResults, result)
			for key, values := range deltas {
				state[key] = values[2]
				stateCommits = append(stateCommits, StateCommit{
					BlockHeight:  block.Height,
					TxID:         tx.ID,
					StateKey:     key,
					OldValue:     values[0],
					Delta:        values[1],
					NewValue:     values[2],
					CommitPlugin: "normal_commit",
					CommitTimeMS: commitTime,
					Status:       "success",
				})
			}
			cursor = end
		}
	}
	summary := buildSummary(runID, experiment, chain.ProfileID, plugin.ProfileID, txResults, len(blocks), runtimeMode(plugin))
	applyMechanismMetrics(&summary, txResults, plugin)
	if err := writeArtifacts(input.OutputDir, chainBytes, pluginBytes, experimentBytes, summary, blockLog, txResults, stateCommits, "V3.3 Go-backed minimal runtime parity run"); err != nil {
		return Result{}, err
	}
	return Result{OutputDir: input.OutputDir, Summary: summary, BlockLog: blockLog, TxResults: txResults, StateCommitLog: stateCommits, FinalState: state}, nil
}

func parseChainProfile(text string) ChainProfile {
	return ChainProfile{
		ProfileID:       fieldString(text, "profile_id", "chain_x_default"),
		NodeIDPrefix:    fieldString(text, "node_id_prefix", "node"),
		NodeCount:       fieldInt(text, "node_count", 4),
		ValidatorCount:  fieldInt(text, "validator_count", 4),
		BlockIntervalMS: fieldInt(text, "block_interval_ms", 100),
		MaxTxPerBlock:   fieldInt(text, "max_tx_per_block", 500),
		ShardCount:      fieldInt(text, "shard_count", 4),
		KeyCount:        fieldInt(text, "key_count", 100000),
		MaxPoolSize:     fieldInt(text, "max_pool_size", 100000),
		DedupEnabled:    fieldBool(text, "dedup_enabled", true),
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

func cutBlocks(txs []Transaction, chain ChainProfile) []Block {
	blocks := []Block{}
	maxTx := max(1, chain.MaxTxPerBlock)
	for offset := 0; offset < len(txs); offset += maxTx {
		end := offset + maxTx
		if end > len(txs) {
			end = len(txs)
		}
		height := len(blocks) + 1
		batch := append([]Transaction(nil), txs[offset:end]...)
		lastSubmit := batch[len(batch)-1].SubmitTimeMS
		cutTime := max(height*chain.BlockIntervalMS, lastSubmit)
		blocks = append(blocks, Block{Height: height, ID: fmt.Sprintf("block_%06d", height), Txs: batch, CutTimeMS: cutTime})
	}
	return blocks
}

func buildSummary(runID string, exp ExperimentProfile, chainProfileID, pluginProfileID string, txs []TxResult, blockCount int, runtimeMode string) Summary {
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
		RunID:               runID,
		Stage:               exp.Stage,
		BackendType:         exp.BackendType,
		TruthLabel:          exp.TruthLabel,
		ChainProfileID:      chainProfileID,
		PluginProfileID:     pluginProfileID,
		ExperimentProfileID: exp.ProfileID,
		TxCount:             len(txs),
		SuccessCount:        len(txs),
		FailureCount:        0,
		BlockCount:          blockCount,
		ThroughputTPS:       round(float64(len(txs)) / duration),
		AvgLatencyMS:        round(avg(latencies)),
		P95LatencyMS:        percentileInt(latencies, 95),
		P99LatencyMS:        percentileInt(latencies, 99),
		RuntimeMode:         runtimeMode,
	}
}

func applyMechanismMetrics(summary *Summary, txs []TxResult, plugin PluginProfile) {
	remote := 0
	crossShard := 0
	fast := 0
	conservative := 0
	conflicts := 0
	aggregated := 0
	hotCounts := map[string]int{}
	for _, tx := range txs {
		remote += tx.RemoteFetchCount
		if tx.RemoteFetchCount > 0 {
			crossShard++
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
	if len(txs) > 0 {
		summary.CrossShardRatio = round(float64(crossShard) / float64(len(txs)))
		summary.AggregationRatio = round(float64(aggregated) / float64(len(txs)))
	}
	summary.FastTrackCount = fast
	summary.ConservativeTrackCount = conservative
	summary.AggregatedUpdateCount = aggregated
	summary.ConflictCount = conflicts
	summary.QueueWaitMS = 0
	summary.BlockCommitLatencyMS = summary.AvgLatencyMS
}

func writeArtifacts(out string, chainBytes, pluginBytes, experimentBytes []byte, summary Summary, blockLog []map[string]string, txResults []TxResult, commits []StateCommit, title string) error {
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
	report := "# " + title + "\n\nThis is V3.3 Go-backed minimal runtime parity, not Fabric live execution, not MetaTrack final evidence, and not final paper-scale performance evidence.\n"
	if err := os.WriteFile(filepath.Join(out, "report.md"), []byte(report), 0o644); err != nil {
		return err
	}
	log := "v3 go-backed runtime start\nruntime_mode=" + summary.RuntimeMode + "\ntruth_label=" + summary.TruthLabel + "\nfabric_live=false\nmetaflow=false\nv3 go-backed runtime done\n"
	return os.WriteFile(filepath.Join(out, "runtime.log"), []byte(log), 0o644)
}

func writeSummaryCSV(path string, s Summary) error {
	return writeCSV(path, summaryFields(), [][]string{{
		s.RunID, s.Stage, s.BackendType, s.TruthLabel, s.ChainProfileID, s.PluginProfileID, s.ExperimentProfileID,
		strconv.Itoa(s.TxCount), strconv.Itoa(s.SuccessCount), strconv.Itoa(s.FailureCount), strconv.Itoa(s.BlockCount),
		fmt.Sprint(s.ThroughputTPS), fmt.Sprint(s.AvgLatencyMS), fmt.Sprint(s.P95LatencyMS), fmt.Sprint(s.P99LatencyMS), s.RuntimeMode,
		strconv.Itoa(s.RemoteFetchCount), fmt.Sprint(s.CrossShardRatio), strconv.Itoa(s.FastTrackCount), strconv.Itoa(s.ConservativeTrackCount), strconv.Itoa(s.AggregatedUpdateCount), fmt.Sprint(s.AggregationRatio), strconv.Itoa(s.ConflictCount), fmt.Sprint(s.QueueWaitMS), fmt.Sprint(s.BlockCommitLatencyMS),
	}})
}

func summaryFields() []string {
	return []string{"run_id", "stage", "backend_type", "truth_label", "chain_profile_id", "plugin_profile_id", "experiment_profile_id", "tx_count", "success_count", "failure_count", "block_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "runtime_mode", "remote_fetch_count", "cross_shard_ratio", "fast_track_count", "conservative_track_count", "aggregated_update_count", "aggregation_ratio", "conflict_count", "queue_wait_ms", "block_commit_latency_ms"}
}

func writeBlockLog(path string, rows []map[string]string) error {
	fields := []string{"block_height", "block_id", "proposer_node", "tx_count", "cut_time_ms", "ordered_time_ms", "finalized_time_ms", "consensus_plugin", "status"}
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
	fields := []string{"tx_id", "submit_time_ms", "admit_time_ms", "block_height", "execution_start_ms", "execution_end_ms", "commit_time_ms", "latency_ms", "status", "shard_id", "read_count", "write_count", "remote_fetch_count"}
	rows := [][]string{}
	for _, tx := range txs {
		rows = append(rows, []string{tx.TxID, strconv.Itoa(tx.SubmitTimeMS), strconv.Itoa(tx.AdmitTimeMS), strconv.Itoa(tx.BlockHeight), strconv.Itoa(tx.ExecutionStartMS), strconv.Itoa(tx.ExecutionEndMS), strconv.Itoa(tx.CommitTimeMS), strconv.Itoa(tx.LatencyMS), tx.Status, strconv.Itoa(tx.ShardID), strconv.Itoa(tx.ReadCount), strconv.Itoa(tx.WriteCount), strconv.Itoa(tx.RemoteFetchCount)})
	}
	return writeCSV(path, fields, rows)
}

func writeStateCommitLog(path string, commits []StateCommit) error {
	fields := []string{"block_height", "tx_id", "state_key", "old_value", "delta", "new_value", "commit_plugin", "commit_time_ms", "status"}
	rows := [][]string{}
	for _, c := range commits {
		rows = append(rows, []string{strconv.Itoa(c.BlockHeight), c.TxID, c.StateKey, strconv.Itoa(c.OldValue), strconv.Itoa(c.Delta), strconv.Itoa(c.NewValue), c.CommitPlugin, strconv.Itoa(c.CommitTimeMS), c.Status})
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

func buildRoutingMap(txs []Transaction, chain ChainProfile, plugin PluginProfile) map[string]int {
	keys := []string{}
	for _, tx := range txs {
		keys = append(keys, tx.ReadKeys...)
	}
	keys = unique(keys)
	result := map[string]int{}
	if plugin.ShardingPlugin != "co_access_sharding" {
		for _, key := range keys {
			result[key] = shard(key, chain.ShardCount)
		}
		return result
	}
	groupShard := 0
	for _, key := range keys {
		if strings.HasPrefix(key, "asset_0") || strings.HasPrefix(key, "asset_1") || strings.HasPrefix(key, "asset_2") || strings.HasPrefix(key, "asset_3") {
			result[key] = 0
			continue
		}
		result[key] = groupShard % max(1, chain.ShardCount)
		groupShard++
	}
	return result
}

func assignTxShard(tx Transaction, routingMap map[string]int, chain ChainProfile) int {
	if len(tx.ReadKeys) == 0 {
		return shard(tx.ID, chain.ShardCount)
	}
	return routingMap[tx.ReadKeys[0]]
}

func remoteFetchCount(tx Transaction, txShard int, routingMap map[string]int, chain ChainProfile, plugin PluginProfile) int {
	if plugin.StateAccessPlugin == "access_list_prefetch" {
		return 0
	}
	remote := 0
	for _, key := range tx.ReadKeys {
		shardID, ok := routingMap[key]
		if !ok {
			shardID = shard(key, chain.ShardCount)
		}
		if shardID != txShard {
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}
