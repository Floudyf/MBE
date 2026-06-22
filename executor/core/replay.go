package core

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const defaultShardCount = 4

// Summary contains the V0 replay metrics written to summary.csv.
type Summary struct {
	TxCount, SuccessCount, FailedCount, RemoteFetchCount int
	ThroughputTPS, AvgLatencyMS, P95LatencyMS, P99LatencyMS, CrossShardRatio, WallClockRuntimeMS float64
}

// Replay streams a V0 trace, records per-transaction virtual-clock latency, and writes metrics.
func Replay(config, trace, out string) (Summary, error) {
	shardCount, err := shardCountFromConfig(config)
	if err != nil {
		return Summary{}, err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
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
		commitDone := arrival + tx.ChainLatencyMS
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
			latencies = append(latencies, tx.ChainLatencyMS)
		}

		if crossShard, remoteFetches := stateAccessMetrics(tx, shardCount); crossShard {
			crossShardCount++
			summary.RemoteFetchCount += remoteFetches
		}
		if err := latencyWriter.Write([]string{
			tx.TxID, tx.TxType, fmt.Sprint(arrival), fmt.Sprint(arrival), fmt.Sprint(commitDone),
			fmt.Sprint(tx.ChainLatencyMS), status, fmt.Sprint(tx.ChainLatencyMS),
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
		virtualSpanMS := lastCommit - firstArrival
		if virtualSpanMS > 0 {
			summary.ThroughputTPS = float64(summary.TxCount) * 1000 / virtualSpanMS
		} else {
			// A zero virtual interval still represents a completed finite replay.
			summary.ThroughputTPS = float64(summary.TxCount)
		}
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
		"replay done",
	)
	return summary, os.WriteFile(filepath.Join(out, "runtime.log"), []byte(strings.Join(logLines, "\n")+"\n"), 0o644)
}

func shardCountFromConfig(path string) (int, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	matches := regexp.MustCompile(`shard_count:\s*(\d+)`).FindStringSubmatch(string(contents))
	if len(matches) != 2 {
		return defaultShardCount, nil
	}
	count, err := strconv.Atoi(matches[1])
	if err != nil || count <= 0 {
		return defaultShardCount, nil
	}
	return count, nil
}

func stateAccessMetrics(tx Transaction, shardCount int) (bool, int) {
	keys := uniqueStateKeys(tx)
	if len(keys) < 2 {
		return false, 0
	}
	localShard := stateShard(keys[0], shardCount)
	shards := map[int]struct{}{localShard: {}}
	remoteFetches := 0
	for _, key := range keys[1:] {
		shard := stateShard(key, shardCount)
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

func stateShard(key string, shardCount int) int {
	hash := fnv.New32a()
	_, _ = hash.Write([]byte(key))
	return int(hash.Sum32() % uint32(shardCount))
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
		"tx_count", "success_count", "failed_count", "throughput_tps", "avg_latency_ms", "p95_latency_ms", "p99_latency_ms", "cross_shard_ratio", "remote_fetch_count", "wall_clock_runtime_ms",
	}, {
		fmt.Sprint(summary.TxCount), fmt.Sprint(summary.SuccessCount), fmt.Sprint(summary.FailedCount), fmt.Sprint(summary.ThroughputTPS), fmt.Sprint(summary.AvgLatencyMS), fmt.Sprint(summary.P95LatencyMS), fmt.Sprint(summary.P99LatencyMS), fmt.Sprint(summary.CrossShardRatio), fmt.Sprint(summary.RemoteFetchCount), fmt.Sprint(summary.WallClockRuntimeMS),
	}})
	return writer.Error()
}
