package core

import (
	"encoding/csv"
	"fmt"
	"math"
	"metaverse-chainlab/executor/commit"
	"metaverse-chainlab/executor/execution_sharding"
	"metaverse-chainlab/executor/routing"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Summary contains the V0 replay metrics written to summary.csv.
type Summary struct {
	TxCount, SuccessCount, FailedCount, RemoteFetchCount, RoutingCrossShardTxCount, RoutingRemoteKeyCount, CoAccessGroupCount                                                                                                                                                   int
	ThroughputTPS, AvgLatencyMS, P95LatencyMS, P99LatencyMS, CrossShardRatio, WallClockRuntimeMS, RoutingCrossShardTxRatio, RoutingTimeMS, VirtualTimeMS                                                                                                                        float64
	RoutingPolicy                                                                                                                                                                                                                                                               string
	FastTrackTxCount, ConservativeTrackTxCount, FastTrackExecutedCount, ConservativeTrackExecutedCount, BlockedOrDeferredTxCount, SchedulerIdleCount                                                                                                                            int
	FastTrackTxRatio, ConservativeTrackTxRatio                                                                                                                                                                                                                                  float64
	DualTrackEnabled                                                                                                                                                                                                                                                            bool
	FastTrackMaxAccessSize                                                                                                                                                                                                                                                      int
	TrackPolicy                                                                                                                                                                                                                                                                 string
	AggregationCandidateTxCount, AggregatedTxCount, AggregatedCommitCount, ConservativeCommitCount, AggregationSavedCommitCount, AggregationGroupCount, AggregationHotKeyCount, AggregationConstraintFailureCount, AggregationMissingDeltaCount, AggregationNonCommutativeCount int
	HotUpdateAggregationEnabled                                                                                                                                                                                                                                                 bool
	AggregationPolicy                                                                                                                                                                                                                                                           string
}

// Replay streams a V0 trace, records per-transaction virtual-clock latency, and writes metrics.
func Replay(config, trace, out string) (Summary, error) {
	replayConfig, err := LoadReplayConfig(config)
	if err != nil {
		return Summary{}, err
	}
	modules := DefaultModuleSet(replayConfig)
	dualConfig := execution_sharding.DualTrackConfig{Enabled: replayConfig.DualTrackEnabled, FastTrackMaxAccessSize: replayConfig.FastTrackMaxAccessSize, ConservativeOnConflictHint: replayConfig.ConservativeOnConflictHint, ConservativeOnMissingAccessSet: replayConfig.ConservativeOnMissingAccessSet, SchedulerPolicy: replayConfig.SchedulerPolicy}
	aggConfig := commit.Config{Enabled: replayConfig.HotUpdateAggregationEnabled, MinHotCount: replayConfig.AggregationMinHotCount, MaxGroupSize: replayConfig.AggregationMaxGroupSize, RequireFastTrack: replayConfig.AggregationRequireFastTrack, ConservativeOnConstraintFailure: replayConfig.ConservativeOnConstraintFailure, Policy: replayConfig.AggregationPolicy}
	aggUpdates := []commit.Update{}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return Summary{}, err
	}
	if err := writeConfigSnapshot(config, out); err != nil {
		return Summary{}, err
	}

	start := time.Now()
	logLines := []string{
		"replay start",
		"input trace path: " + trace,
		"output directory: " + out,
	}

	latencyFile, err := os.Create(filepath.Join(out, "latency.csv"))
	if err != nil {
		return Summary{}, err
	}
	latencyWriter := csv.NewWriter(latencyFile)
	if err := latencyWriter.Write([]string{"tx_id", "tx_type", "arrival_time_ms", "start_time_ms", "commit_done_time_ms", "latency_ms", "status", "chain_latency_ms"}); err != nil {
		latencyFile.Close()
		return Summary{}, err
	}

	summary := Summary{}
	latencies := make([]float64, 0)
	crossShardCount := 0
	var firstArrival, lastCommit float64
	hasTransactions := false
	txs, errs := StreamTransactions(trace)
	for tx := range txs {
		summary.TxCount++
		arrival := tx.Timestamp
		blockIndex := (summary.TxCount - 1) / replayConfig.BlockSize
		startTime := math.Max(arrival, float64(blockIndex)*replayConfig.BlockIntervalMS)
		keys := uniqueStateKeys(tx)
		// V1.2 builds the default M_t for the current streaming batch. M_t is
		// execution-side state routing and leaves phi persistent placement intact.
		route := modules.Routing.BuildRouting([]routing.Transaction{{ID: tx.TxID, AccessKeys: keys}}, modules.StateSharding)
		summary.RoutingPolicy = route.Metrics.RoutingPolicy
		summary.RoutingCrossShardTxCount += route.Metrics.CrossShardTxCount
		summary.RoutingRemoteKeyCount += route.Metrics.RemoteKeyCount
		summary.CoAccessGroupCount += route.Metrics.CoAccessGroupCount
		summary.RoutingTimeMS += route.Metrics.RoutingTimeMS
		// psi_t is calculated even though serial execution preserves V0 timing.
		assigned := modules.ExecutionSharding.Assign(execution_sharding.Transaction{ID: tx.TxID, AccessKeys: keys}, execution_sharding.Context{StateToExecution: route.StateToExecution})
		if s, ok := route.TxToExecution[tx.TxID]; ok {
			assigned = s
		}
		decision := execution_sharding.ClassifyDualTrack(execution_sharding.DualTrackTransaction{ID: tx.TxID, AccessKeys: keys, ExecutionShard: assigned}, dualConfig)
		primary := ""
		if tx.PrimaryKey != nil {
			primary = *tx.PrimaryKey
		}
		aggUpdates = append(aggUpdates, commit.Update{ID: tx.TxID, PrimaryKey: primary, Fast: decision.Track == execution_sharding.Fast, Commutative: tx.Commutative, Delta: tx.DeltaValue})
		if decision.Track == execution_sharding.Fast {
			summary.FastTrackTxCount++
			summary.FastTrackExecutedCount++
		} else {
			summary.ConservativeTrackTxCount++
			summary.ConservativeTrackExecutedCount++
		}
		summary.DualTrackEnabled = dualConfig.Enabled
		summary.FastTrackMaxAccessSize = dualConfig.FastTrackMaxAccessSize
		summary.TrackPolicy = dualConfig.SchedulerPolicy
		crossShard, remoteFetches := stateAccessMetrics(keys, modules)
		virtualLatency := tx.ChainLatencyMS + replayConfig.FinalityDelayMS + float64(remoteFetches)*replayConfig.RemoteFetchLatencyMS
		commitDone := startTime + virtualLatency
		latency := commitDone - arrival
		if !hasTransactions || arrival < firstArrival {
			firstArrival = arrival
		}
		if !hasTransactions || commitDone > lastCommit {
			lastCommit = commitDone
		}
		hasTransactions = true

		status := "success"
		if tx.Status == "failed" {
			status = "failed"
			summary.FailedCount++
		} else {
			summary.SuccessCount++
			latencies = append(latencies, latency)
		}

		if crossShard {
			crossShardCount++
			summary.RemoteFetchCount += remoteFetches
		}
		if err := latencyWriter.Write([]string{
			tx.TxID, tx.TxType, fmt.Sprint(arrival), fmt.Sprint(startTime), fmt.Sprint(commitDone),
			fmt.Sprint(latency), status, fmt.Sprint(tx.ChainLatencyMS),
		}); err != nil {
			latencyFile.Close()
			return summary, err
		}
	}
	for err := range errs {
		if err != nil {
			latencyFile.Close()
			return summary, err
		}
	}
	agg := commit.Aggregate(aggUpdates, aggConfig)
	am := agg.Metrics
	summary.HotUpdateAggregationEnabled, summary.AggregationPolicy = aggConfig.Enabled, aggConfig.Policy
	summary.AggregationCandidateTxCount, summary.AggregatedTxCount, summary.AggregatedCommitCount = am.CandidateTxCount, am.AggregatedTxCount, am.AggregatedCommitCount
	summary.ConservativeCommitCount, summary.AggregationSavedCommitCount, summary.AggregationGroupCount = am.ConservativeCommitCount, am.SavedCommitCount, am.GroupCount
	summary.AggregationHotKeyCount, summary.AggregationConstraintFailureCount, summary.AggregationMissingDeltaCount, summary.AggregationNonCommutativeCount = am.HotKeyCount, am.ConstraintFailureCount, am.MissingDeltaCount, am.NonCommutativeCount
	latencyWriter.Flush()
	if err := latencyWriter.Error(); err != nil {
		latencyFile.Close()
		return summary, err
	}
	if err := latencyFile.Close(); err != nil {
		return summary, err
	}

	summary.AvgLatencyMS, summary.P95LatencyMS, summary.P99LatencyMS = latencyMetrics(latencies)
	if summary.TxCount > 0 {
		summary.CrossShardRatio = float64(crossShardCount) / float64(summary.TxCount)
		summary.RoutingCrossShardTxRatio = float64(summary.RoutingCrossShardTxCount) / float64(summary.TxCount)
		summary.VirtualTimeMS = lastCommit - firstArrival
		if summary.VirtualTimeMS > 0 {
			summary.ThroughputTPS = float64(summary.TxCount) * 1000 / summary.VirtualTimeMS
		} else {
			// A zero virtual interval still represents a completed finite replay.
			summary.ThroughputTPS = float64(summary.TxCount)
		}
		summary.FastTrackTxRatio = float64(summary.FastTrackTxCount) / float64(summary.TxCount)
		summary.ConservativeTrackTxRatio = float64(summary.ConservativeTrackTxCount) / float64(summary.TxCount)
	}
	summary.WallClockRuntimeMS = float64(time.Since(start).Microseconds()) / 1000

	if err := writeSummary(filepath.Join(out, "summary.csv"), summary); err != nil {
		return summary, err
	}
	logLines = append(logLines,
		fmt.Sprintf("tx_count: %d", summary.TxCount),
		fmt.Sprintf("success_count: %d", summary.SuccessCount),
		fmt.Sprintf("failed_count: %d", summary.FailedCount),
		fmt.Sprintf("throughput_tps: %g", summary.ThroughputTPS),
		fmt.Sprintf("cross_shard_ratio: %g", summary.CrossShardRatio),
		fmt.Sprintf("remote_fetch_count: %d", summary.RemoteFetchCount),
		fmt.Sprintf("routing_policy: %s", summary.RoutingPolicy),
		fmt.Sprintf("routing_cross_shard_tx_count: %d", summary.RoutingCrossShardTxCount),
		fmt.Sprintf("dual_track_enabled: %t", summary.DualTrackEnabled),
		fmt.Sprintf("fast_track_tx_count: %d", summary.FastTrackTxCount),
		fmt.Sprintf("conservative_track_tx_count: %d", summary.ConservativeTrackTxCount),
		fmt.Sprintf("hot_update_aggregation_enabled: %t", summary.HotUpdateAggregationEnabled),
		fmt.Sprintf("aggregation_policy: %s", summary.AggregationPolicy),
		fmt.Sprintf("aggregated_commit_count: %d", summary.AggregatedCommitCount),
		"replay done",
	)
	return summary, os.WriteFile(filepath.Join(out, "runtime.log"), []byte(strings.Join(logLines, "\n")+"\n"), 0o644)
}

func writeConfigSnapshot(config, out string) error {
	contents, err := os.ReadFile(config)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(out, "config.yaml"), contents, 0o644)
}

func stateAccessMetrics(keys []string, modules ModuleSet) (bool, int) {
	if len(keys) < 2 {
		return false, 0
	}
	localShard := modules.StateSharding.LocateState(keys[0])
	shards := map[int]struct{}{localShard: {}}
	remoteFetches := 0
	for _, key := range keys[1:] {
		shard := modules.StateSharding.LocateState(key)
		shards[shard] = struct{}{}
		if shard != localShard {
			remoteFetches++
		}
	}
	if len(shards) < 2 {
		return false, 0
	}
	return true, remoteFetches
}

func uniqueStateKeys(tx Transaction) []string {
	seen := make(map[string]struct{})
	keys := make([]string, 0, len(tx.AccessList)+len(tx.ReadSet)+len(tx.WriteSet))
	for _, set := range [][]string{tx.AccessList, tx.ReadSet, tx.WriteSet} {
		for _, key := range set {
			if _, exists := seen[key]; !exists {
				seen[key] = struct{}{}
				keys = append(keys, key)
			}
		}
	}
	return keys
}

func latencyMetrics(latencies []float64) (float64, float64, float64) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sorted := append([]float64(nil), latencies...)
	sort.Float64s(sorted)
	average := 0.0
	for _, latency := range sorted {
		average += latency
	}
	average /= float64(len(sorted))
	return average, percentile(sorted, 0.95), percentile(sorted, 0.99)
}

func percentile(sorted []float64, quantile float64) float64 {
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	return sorted[index]
}

func writeSummary(path string, summary Summary) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	writer.WriteAll([][]string{{
		"tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "cross_shard_ratio", "remote_fetch_count", "routing_policy", "routing_cross_shard_tx_count", "routing_cross_shard_tx_ratio", "routing_remote_key_count", "co_access_group_count", "routing_time_ms", "dual_track_enabled", "fast_track_max_access_size", "track_policy", "fast_track_tx_count", "conservative_track_tx_count", "fast_track_tx_ratio", "conservative_track_tx_ratio", "fast_track_executed_count", "conservative_track_executed_count", "blocked_or_deferred_tx_count", "scheduler_idle_count", "hot_update_aggregation_enabled", "aggregation_policy", "aggregation_candidate_tx_count", "aggregated_tx_count", "aggregated_commit_count", "conservative_commit_count", "aggregation_saved_commit_count", "aggregation_group_count", "aggregation_hot_key_count", "aggregation_constraint_failure_count", "aggregation_missing_delta_count", "aggregation_non_commutative_count", "virtual_time_ms", "wall_clock_runtime_ms",
	}, {
		fmt.Sprint(summary.TxCount), fmt.Sprint(summary.SuccessCount), fmt.Sprint(summary.FailedCount), fmt.Sprint(summary.ThroughputTPS), fmt.Sprint(summary.AvgLatencyMS), fmt.Sprint(summary.P95LatencyMS), fmt.Sprint(summary.P99LatencyMS), fmt.Sprint(summary.CrossShardRatio), fmt.Sprint(summary.RemoteFetchCount), summary.RoutingPolicy, fmt.Sprint(summary.RoutingCrossShardTxCount), fmt.Sprint(summary.RoutingCrossShardTxRatio), fmt.Sprint(summary.RoutingRemoteKeyCount), fmt.Sprint(summary.CoAccessGroupCount), fmt.Sprint(summary.RoutingTimeMS), fmt.Sprint(summary.DualTrackEnabled), fmt.Sprint(summary.FastTrackMaxAccessSize), summary.TrackPolicy, fmt.Sprint(summary.FastTrackTxCount), fmt.Sprint(summary.ConservativeTrackTxCount), fmt.Sprint(summary.FastTrackTxRatio), fmt.Sprint(summary.ConservativeTrackTxRatio), fmt.Sprint(summary.FastTrackExecutedCount), fmt.Sprint(summary.ConservativeTrackExecutedCount), fmt.Sprint(summary.BlockedOrDeferredTxCount), fmt.Sprint(summary.SchedulerIdleCount), fmt.Sprint(summary.HotUpdateAggregationEnabled), summary.AggregationPolicy, fmt.Sprint(summary.AggregationCandidateTxCount), fmt.Sprint(summary.AggregatedTxCount), fmt.Sprint(summary.AggregatedCommitCount), fmt.Sprint(summary.ConservativeCommitCount), fmt.Sprint(summary.AggregationSavedCommitCount), fmt.Sprint(summary.AggregationGroupCount), fmt.Sprint(summary.AggregationHotKeyCount), fmt.Sprint(summary.AggregationConstraintFailureCount), fmt.Sprint(summary.AggregationMissingDeltaCount), fmt.Sprint(summary.AggregationNonCommutativeCount), fmt.Sprint(summary.VirtualTimeMS), fmt.Sprint(summary.WallClockRuntimeMS),
	}})
	return writer.Error()
}
