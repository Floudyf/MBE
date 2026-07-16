package v5

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"metaverse-chainlab/executor/realism/tx"
)

const maxWorkloadRecordBytes = 1024 * 1024

type SyntheticIterator struct {
	plugin  builtinWorkload
	plan    WorkloadPlan
	shards  int
	index   int
	summary WorkloadReplaySummary
}

func NewSyntheticIterator(plugin builtinWorkload, plan WorkloadPlan, shards int) *SyntheticIterator {
	expected := requestedCrossShardCount(plan.TxCount, plan.CrossShardRatio)
	return &SyntheticIterator{plugin: plugin, plan: plan, shards: shards, summary: WorkloadReplaySummary{ExpectedCount: plan.TxCount, ExpectedCrossShardCount: expected, ExpectedCrossShardRatio: plan.CrossShardRatio, ReplayMode: "max_throughput", NoFallback: true, NonceContinuity: true}}
}

func (it *SyntheticIterator) Next(context.Context) (WorkloadRecord, error) {
	if it.index >= it.plan.TxCount {
		return WorkloadRecord{}, io.EOF
	}
	index := it.index
	it.index++
	cross := crossShardAt(index, it.plan.TxCount, it.plan.CrossShardRatio, it.plan.Seed)
	item := it.plugin.BuildWorkloadItem(WorkloadInput{Index: index, Shards: it.shards, Seed: it.plan.Seed, TimeoutEvery: it.plan.TimeoutEvery, CrossShard: cross})
	it.summary.ReadCount++
	return WorkloadRecord{Index: index, LogicalID: fmt.Sprintf("synthetic-%d", index), Payload: item.Payload, StateKeys: item.StateKeys, CrossShard: cross, Value: 1}, nil
}

func (it *SyntheticIterator) Close() error                   { return nil }
func (it *SyntheticIterator) Summary() WorkloadReplaySummary { return it.summary }

type canonicalWireRecord struct {
	SchemaVersion     string   `json:"schema_version"`
	DatasetID         string   `json:"dataset_id"`
	SourceRowIndex    int      `json:"source_row_index"`
	SourceEventID     string   `json:"source_event_id"`
	SourceTxHash      string   `json:"source_tx_hash"`
	TimestampMS       int64    `json:"timestamp_ms"`
	SenderID          string   `json:"sender_id"`
	ReceiverID        string   `json:"receiver_id"`
	OperationType     string   `json:"operation_type"`
	RuntimeValue      int64    `json:"runtime_value"`
	StateKeys         []string `json:"state_keys"`
	RoutingSourceKey  string   `json:"routing_source_key"`
	RoutingTargetKey  string   `json:"routing_target_key"`
	MaterializedIndex int      `json:"materialized_index"`
	LogicalEventID    string   `json:"logical_event_id"`
}

type CanonicalTraceIterator struct {
	plan       WorkloadPlan
	shards     int
	file       *os.File
	gzip       *gzip.Reader
	scanner    *bufio.Scanner
	index      int
	closed     bool
	identities map[string]string
	nonces     map[string]uint64
	hash       string
	summary    WorkloadReplaySummary
}

func NewCanonicalTraceIterator(plan WorkloadPlan, shards int, dataDir string) (*CanonicalTraceIterator, error) {
	if plan.PluginID != "canonical_trace_replay" || plan.SourceType != "dataset" {
		return nil, fmt.Errorf("canonical trace iterator requires dataset canonical_trace_replay plan")
	}
	if plan.NoFallback == false {
		return nil, fmt.Errorf("dataset workload requires no_fallback=true")
	}
	path, err := workloadPath(dataDir, plan.MaterializedRelativePath)
	if err != nil {
		return nil, err
	}
	hash, err := sha256Path(path)
	if err != nil {
		return nil, err
	}
	if !strings.EqualFold(hash, plan.MaterializedSHA256) {
		return nil, fmt.Errorf("materialized hash mismatch")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	gz, err := gzip.NewReader(file)
	if err != nil {
		file.Close()
		return nil, err
	}
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 64*1024), maxWorkloadRecordBytes)
	expected := plan.ActualTxCount
	if expected == 0 {
		expected = plan.TxCount
	}
	return &CanonicalTraceIterator{
		plan: plan, shards: shards, file: file, gzip: gz, scanner: scanner,
		identities: map[string]string{}, nonces: map[string]uint64{}, hash: hash,
		summary: WorkloadReplaySummary{DatasetID: plan.DatasetID, VariantID: plan.VariantID, TruthLabel: plan.TruthLabel, SourceSHA256: plan.SourceSHA256, MaterializedSHA256: plan.MaterializedSHA256, ExpectedCount: expected, ExpectedCrossShardCount: plan.ExpectedCrossShardCount, ExpectedCrossShardRatio: plan.ExpectedCrossShardRatio, ReplayMode: plan.ReplayMode, NoFallback: true, NonceContinuity: true, ShardLoadDistribution: map[string]int{}, IdentityMappingVersion: firstNonEmpty(plan.IdentityMappingVersion, "mbe_dataset_identity_v1")},
	}, nil
}

func (it *CanonicalTraceIterator) Next(context.Context) (WorkloadRecord, error) {
	if !it.scanner.Scan() {
		if err := it.scanner.Err(); err != nil {
			return WorkloadRecord{}, err
		}
		if it.index != it.summary.ExpectedCount {
			return WorkloadRecord{}, fmt.Errorf("early EOF: read %d expected %d", it.index, it.summary.ExpectedCount)
		}
		return WorkloadRecord{}, io.EOF
	}
	var wire canonicalWireRecord
	if err := json.Unmarshal(it.scanner.Bytes(), &wire); err != nil {
		return WorkloadRecord{}, fmt.Errorf("malformed canonical workload JSON: %w", err)
	}
	if wire.SchemaVersion != "mbe_workload_record_v1" || wire.DatasetID != it.plan.DatasetID || wire.SenderID == "" || wire.OperationType == "" || len(wire.StateKeys) < 1 || wire.RoutingSourceKey == "" {
		return WorkloadRecord{}, fmt.Errorf("canonical workload schema error")
	}
	if it.index >= it.summary.ExpectedCount {
		return WorkloadRecord{}, fmt.Errorf("canonical workload has excess records")
	}
	sourceShard := stableShard([]string{strings.ToLower(wire.RoutingSourceKey)}, it.shards)
	targetShard := sourceShard
	if wire.RoutingTargetKey != "" {
		targetShard = stableShard([]string{strings.ToLower(wire.RoutingTargetKey)}, it.shards)
	}
	cross := wire.RoutingTargetKey != "" && sourceShard != targetShard
	target := ""
	payload := "dataset_event:" + wire.OperationType
	if cross {
		target = fmt.Sprintf("s%d", targetShard)
		payload = "v5_cross:" + target + ":" + payload
		it.summary.ActualCrossShardCount++
	}
	it.summary.ShardLoadDistribution[fmt.Sprintf("s%d", sourceShard)]++
	it.summary.ReadCount++
	it.index++
	return WorkloadRecord{Index: it.index - 1, LogicalID: firstNonEmpty(wire.LogicalEventID, wire.SourceEventID), SenderID: strings.ToLower(wire.SenderID), ReceiverID: strings.ToLower(wire.ReceiverID), OperationType: wire.OperationType, RoutingSourceKey: wire.RoutingSourceKey, RoutingTargetKey: wire.RoutingTargetKey, Payload: payload, StateKeys: wire.StateKeys, CrossShard: cross, SourceShard: fmt.Sprintf("s%d", sourceShard), TargetShard: target, SourceEventID: wire.SourceEventID, TimestampMS: wire.TimestampMS, Value: maxInt64(1, wire.RuntimeValue)}, nil
}

func (it *CanonicalTraceIterator) Close() error {
	if it.closed {
		return nil
	}
	it.closed = true
	err1 := it.gzip.Close()
	err2 := it.file.Close()
	return errors.Join(err1, err2)
}

func (it *CanonicalTraceIterator) Summary() WorkloadReplaySummary {
	out := it.summary
	out.IdentityCount = len(it.identities)
	out.MappingDigest = mappingDigest(it.identities)
	if out.ReadCount > 0 {
		out.ActualCrossShardRatio = float64(out.ActualCrossShardCount) / float64(out.ReadCount)
		if out.ExpectedCrossShardCount == 0 && out.ExpectedCrossShardRatio == 0 {
			out.ExpectedCrossShardCount = out.ActualCrossShardCount
			out.ExpectedCrossShardRatio = out.ActualCrossShardRatio
		}
	}
	if len(out.ShardLoadDistribution) > 0 {
		maxLoad := 0
		total := 0
		for _, value := range out.ShardLoadDistribution {
			total += value
			if value > maxLoad {
				maxLoad = value
			}
		}
		avg := float64(total) / float64(len(out.ShardLoadDistribution))
		if avg > 0 {
			out.MaxAverageShardLoadRatio = float64(maxLoad) / avg
		}
	}
	return out
}

func (it *CanonicalTraceIterator) SignedTransaction(record WorkloadRecord) (tx.SignedTransaction, error) {
	domain := strings.Join([]string{it.plan.DatasetID, it.plan.SourceSHA256, fmt.Sprint(it.plan.Seed), firstNonEmpty(it.plan.IdentityMappingVersion, "mbe_dataset_identity_v1")}, "|")
	privateSeed := domain + "|" + record.SenderID
	publicKey, privateKey := tx.DeterministicKeyPair(privateSeed)
	sender := tx.AddressFromPublicKey(publicKey)
	it.identities[record.SenderID] = sender
	nonce := it.nonces[record.SenderID]
	it.nonces[record.SenderID] = nonce + 1
	item := tx.SignedTransaction{Sender: sender, Receiver: "receiver_" + record.ReceiverID, Nonce: nonce, Value: record.Value, StateKeys: record.StateKeys, Payload: record.Payload, Timestamp: record.TimestampMS, SourceKind: "canonical_trace_replay", TraceSourceID: record.SourceEventID}
	if err := tx.Sign(&item, privateKey); err != nil {
		return item, err
	}
	if err := tx.Verify(item); err != nil {
		return item, err
	}
	it.summary.SignaturePassCount++
	return item, nil
}

func workloadPath(dataDir, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(filepath.ToSlash(relative), "../") {
		return "", fmt.Errorf("unsafe materialized workload path")
	}
	cwd, _ := os.Getwd()
	candidates := []string{
		filepath.Join(dataDir, ".cache", "workloads"),
		filepath.Join(dataDir, "..", "..", "workloads"),
		filepath.Join(cwd, ".cache", "workloads"),
		filepath.Join(cwd, "..", ".cache", "workloads"),
	}
	for _, root := range candidates {
		root = filepath.Clean(root)
		path := filepath.Clean(filepath.Join(root, relative))
		if !strings.HasPrefix(path, root) {
			continue
		}
		if _, err := os.Stat(path); err == nil {
			return path, nil
		}
	}
	return "", fmt.Errorf("materialized workload file is not available")
}

func sha256Path(path string) (string, error) {
	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}
	return hex.EncodeToString(hash.Sum(nil)), nil
}

func stableShard(keys []string, shards int) int {
	if shards <= 0 {
		return 0
	}
	return stableKey(keys) % shards
}

func mappingDigest(items map[string]string) string {
	keys := make([]string, 0, len(items))
	for key := range items {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	for _, key := range keys {
		hash.Write([]byte(key + "=" + items[key] + "\n"))
	}
	return base64.StdEncoding.EncodeToString(hash.Sum(nil))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func maxInt64(a, b int64) int64 {
	if b > a {
		return b
	}
	return a
}
