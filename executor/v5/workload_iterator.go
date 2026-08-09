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
	digest := sha256.Sum256([]byte(fmt.Sprintf("synthetic:v2:%s:%d:%d:%d:%0.12f:%d", plan.PluginID, plan.TxCount, plan.Seed, plan.TimeoutEvery, plan.CrossShardRatio, shards)))
	return &SyntheticIterator{plugin: plugin, plan: plan, shards: shards, summary: WorkloadReplaySummary{MaterializedSHA256: hex.EncodeToString(digest[:]), ExpectedCount: plan.TxCount, ExpectedCrossShardCount: expected, ExpectedCrossShardRatio: plan.CrossShardRatio, ReplayMode: "max_throughput", NoFallback: true, NonceContinuity: true}}
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
	return WorkloadRecord{Index: index, LogicalID: fmt.Sprintf("synthetic-%d", index), Payload: item.Payload, StateKeys: item.StateKeys, AccessList: item.AccessList, CrossShard: cross, Value: 1}, nil
}

func (it *SyntheticIterator) Close() error                   { return nil }
func (it *SyntheticIterator) Summary() WorkloadReplaySummary { return it.summary }

type canonicalWireRecord struct {
	SchemaVersion     string                        `json:"schema_version"`
	DatasetID         string                        `json:"dataset_id"`
	SourceRowIndex    int                           `json:"source_row_index"`
	SourceEventID     string                        `json:"source_event_id"`
	SourceTxHash      string                        `json:"source_tx_hash"`
	TimestampMS       int64                         `json:"timestamp_ms"`
	SenderID          string                        `json:"sender_id"`
	ReceiverID        string                        `json:"receiver_id"`
	OperationType     string                        `json:"operation_type"`
	RuntimeValue      int64                         `json:"runtime_value"`
	StateKeys         []string                      `json:"state_keys"`
	RoutingSourceKey  string                        `json:"routing_source_key"`
	RoutingTargetKey  string                        `json:"routing_target_key"`
	AccessListSchema  string                        `json:"access_list_schema"`
	AccessListSource  string                        `json:"access_list_source"`
	AccessTemplate    []canonicalWireAccessTemplate `json:"access_template"`
	AccessList        []tx.AccessItem               `json:"access_list"`
	AccessListDigest  string                        `json:"access_list_digest"`
	Category          string                        `json:"category,omitempty"`
	Contract          string                        `json:"contract,omitempty"`
	MaterializedIndex int                           `json:"materialized_index"`
	LogicalEventID    string                        `json:"logical_event_id"`
}

type canonicalWireAccessTemplate struct {
	Role      string `json:"role"`
	Mode      string `json:"mode"`
	Semantics string `json:"semantics"`
	Delta     int64  `json:"delta,omitempty"`
}

type CanonicalTraceIterator struct {
	plan       WorkloadPlan
	shards     int
	sharding   ShardingPlugin
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
	return NewCanonicalTraceIteratorWithSharding(plan, shards, dataDir, nil)
}

func NewCanonicalTraceIteratorWithSharding(plan WorkloadPlan, shards int, dataDir string, sharding ShardingPlugin) (*CanonicalTraceIterator, error) {
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
		plan: plan, shards: shards, sharding: sharding, file: file, gzip: gz, scanner: scanner,
		identities: map[string]string{}, nonces: map[string]uint64{}, hash: hash,
		summary: WorkloadReplaySummary{DatasetID: plan.DatasetID, VariantID: plan.VariantID, TruthLabel: plan.TruthLabel, SourceSHA256: plan.SourceSHA256, SourceFileSHA256: plan.SourceFileSHA256, SelectionMode: plan.SelectionMode, VariantParameters: cloneAnyMap(plan.VariantParameters), MaterializedSHA256: plan.MaterializedSHA256, ExpectedCount: expected, ExpectedCrossShardCount: plan.ExpectedCrossShardCount, ExpectedCrossShardRatio: plan.ExpectedCrossShardRatio, ReplayMode: plan.ReplayMode, NoFallback: true, NonceContinuity: true, ShardLoadDistribution: map[string]int{}, IdentityMappingVersion: firstNonEmpty(plan.IdentityMappingVersion, "mbe_dataset_identity_v1")},
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
	if wire.DatasetID != it.plan.DatasetID || wire.SenderID == "" || wire.OperationType == "" || len(wire.StateKeys) < 1 || wire.RoutingSourceKey == "" {
		return WorkloadRecord{}, fmt.Errorf("canonical workload schema error")
	}
	if it.index >= it.summary.ExpectedCount {
		return WorkloadRecord{}, fmt.Errorf("canonical workload has excess records")
	}
	senderID := strings.ToLower(wire.SenderID)
	accessList, accessSchema, accessSource, accessDigest, err := resolveCanonicalAccessList(it.plan, wire)
	if err != nil {
		return WorkloadRecord{}, err
	}
	sourceShard := canonicalRuntimeSourceShardWithSharding(it.plan, senderID, it.shards, it.sharding)
	if wire.SchemaVersion == "mbe_workload_record_v3" && accessSchema != "dcl_sale_access_template_v1" {
		sourceShard = shardIndexFor(it.sharding, []string{strings.ToLower(wire.RoutingSourceKey)}, it.shards)
	}
	targetShard := sourceShard
	if wire.RoutingTargetKey != "" {
		targetShard = shardIndexFor(it.sharding, []string{strings.ToLower(wire.RoutingTargetKey)}, it.shards)
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
	return WorkloadRecord{Index: it.index - 1, LogicalID: firstNonEmpty(wire.LogicalEventID, wire.SourceEventID), SenderID: senderID, ReceiverID: strings.ToLower(wire.ReceiverID), OperationType: wire.OperationType, RoutingSourceKey: wire.RoutingSourceKey, RoutingTargetKey: wire.RoutingTargetKey, Payload: payload, StateKeys: wire.StateKeys, AccessList: accessList, AccessListSchema: accessSchema, AccessListSource: accessSource, AccessListDigest: accessDigest, CrossShard: cross, SourceShard: fmt.Sprintf("s%d", sourceShard), TargetShard: target, SourceEventID: wire.SourceEventID, TimestampMS: wire.TimestampMS, Value: maxInt64(1, wire.RuntimeValue)}, nil
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
	privateSeed := canonicalPrivateSeed(it.plan, record.SenderID)
	publicKey, privateKey := tx.DeterministicKeyPair(privateSeed)
	sender := tx.AddressFromPublicKey(publicKey)
	it.identities[record.SenderID] = sender
	nonce := it.nonces[record.SenderID]
	it.nonces[record.SenderID] = nonce + 1
	receiver := "receiver_" + record.ReceiverID
	if len(record.AccessList) == 0 {
		return tx.SignedTransaction{}, fmt.Errorf("empty resolved access list for source_event_id=%s", record.SourceEventID)
	}
	if digest := CanonicalAccessListDigest(record.AccessList); digest != record.AccessListDigest {
		return tx.SignedTransaction{}, fmt.Errorf("access list digest mismatch for source_event_id=%s", record.SourceEventID)
	}
	if record.AccessListSchema == "dcl_sale_access_template_v1" && (!accessListHasKey(record.AccessList, "balance:"+sender) || !accessListHasKey(record.AccessList, "nonce:"+sender) || !accessListHasKey(record.AccessList, "balance:"+receiver) || !accessListHasKey(record.AccessList, "nonce:"+receiver)) {
		return tx.SignedTransaction{}, fmt.Errorf("resolved access list does not match runtime sender/receiver for source_event_id=%s", record.SourceEventID)
	}
	item := tx.SignedTransaction{LogicalTxID: firstNonEmpty(record.LogicalID, record.SourceEventID), Sender: sender, Receiver: receiver, Nonce: nonce, Value: record.Value, StateKeys: record.StateKeys, AccessList: append([]tx.AccessItem(nil), record.AccessList...), AccessListDigest: record.AccessListDigest, AccessListSchema: record.AccessListSchema, AccessListSource: record.AccessListSource, Payload: record.Payload, Timestamp: record.TimestampMS, SourceKind: "canonical_trace_replay", TraceSourceID: record.SourceEventID}
	if record.RoutePlanDigest != "" {
		routing := tx.ExecutionRoutingMetadata{SenderID: sender, ReceiverID: receiver, RoutingEpoch: record.RoutingEpoch, RoutingOrdinal: record.RoutingOrdinal, ExecutionShard: record.ExecutionShard, RoutingReason: record.RoutingReason, RoutePlanDigest: record.RoutePlanDigest, RouteBatchSequence: record.RouteBatchSequence, RouteBatchTransactionCount: record.RouteBatchTransactionCount, RouteBatchShardTransactionCount: record.RouteBatchShardTransactionCount, PredictedRemoteReads: record.PredictedRemoteReads, PredictedRemoteWrites: record.PredictedRemoteWrites, StateVersions: append([]tx.StateVersionDependency(nil), record.StateVersions...)}
		digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
		if err != nil {
			return item, err
		}
		routing.RouteEntryDigest = digest
		item.ExecutionRouting = &routing
	}
	if err := tx.Sign(&item, privateKey); err != nil {
		return item, err
	}
	if err := tx.Verify(item); err != nil {
		return item, err
	}
	it.summary.SignaturePassCount++
	return item, nil
}

func canonicalIdentityDomain(plan WorkloadPlan) string {
	return strings.Join([]string{plan.DatasetID, plan.SourceSHA256, fmt.Sprint(plan.Seed), firstNonEmpty(plan.IdentityMappingVersion, "mbe_dataset_identity_v1")}, "|")
}

func canonicalPrivateSeed(plan WorkloadPlan, senderID string) string {
	return canonicalIdentityDomain(plan) + "|" + senderID
}

func canonicalRuntimeSenderAddress(plan WorkloadPlan, senderID string) string {
	publicKey, _ := tx.DeterministicKeyPair(canonicalPrivateSeed(plan, senderID))
	return tx.AddressFromPublicKey(publicKey)
}

func canonicalRuntimeSourceShard(plan WorkloadPlan, senderID string, shards int) int {
	return canonicalRuntimeSourceShardWithSharding(plan, senderID, shards, nil)
}

func canonicalRuntimeSourceShardWithSharding(plan WorkloadPlan, senderID string, shards int, sharding ShardingPlugin) int {
	if shards <= 0 {
		return 0
	}
	return shardIndexFor(sharding, []string{"nonce:" + canonicalRuntimeSenderAddress(plan, senderID)}, shards)
}

func canonicalRuntimeAccessList(plan WorkloadPlan, record WorkloadRecord) []tx.AccessItem {
	return append([]tx.AccessItem(nil), record.AccessList...)
}

func resolveCanonicalAccessList(plan WorkloadPlan, wire canonicalWireRecord) ([]tx.AccessItem, string, string, string, error) {
	if wire.SchemaVersion == "mbe_workload_record_v2" {
		items, err := resolveAccessTemplate(plan, wire)
		if err != nil {
			return nil, "", "", "", err
		}
		digest := CanonicalAccessListDigest(items)
		return items, wire.AccessListSchema, wire.AccessListSource, digest, nil
	}
	if wire.SchemaVersion == "mbe_workload_record_v3" {
		items, err := resolveDirectAccessList(wire)
		if err != nil {
			return nil, "", "", "", err
		}
		digest := CanonicalAccessListDigest(items)
		if !strings.EqualFold(digest, wire.AccessListDigest) {
			return nil, "", "", "", fmt.Errorf("direct access list digest mismatch source_row_index=%d source_event_id=%s", wire.SourceRowIndex, wire.SourceEventID)
		}
		return items, wire.AccessListSchema, wire.AccessListSource, digest, nil
	}
	if wire.SchemaVersion == "mbe_workload_record_v1" {
		items, err := canonicalLegacyAccessList(wire)
		if err != nil {
			return nil, "", "", "", err
		}
		digest := CanonicalAccessListDigest(items)
		return items, "legacy_access_inference_v1", "legacy_state_keys", digest, nil
	}
	return nil, "", "", "", fmt.Errorf("canonical workload schema error source_row_index=%d source_event_id=%s schema=%s", wire.SourceRowIndex, wire.SourceEventID, wire.SchemaVersion)
}

func resolveDirectAccessList(wire canonicalWireRecord) ([]tx.AccessItem, error) {
	if strings.TrimSpace(wire.AccessListSchema) == "" || strings.TrimSpace(wire.AccessListSource) == "" || len(wire.AccessList) == 0 {
		return nil, fmt.Errorf("canonical direct access list error source_row_index=%d source_event_id=%s", wire.SourceRowIndex, wire.SourceEventID)
	}
	byKey := map[string]tx.AccessItem{}
	for _, item := range wire.AccessList {
		item.Key = strings.TrimSpace(item.Key)
		item.UpdateSemantics = strings.TrimSpace(item.UpdateSemantics)
		if item.Key == "" || item.UpdateSemantics == "" {
			return nil, fmt.Errorf("canonical direct access list item error source_row_index=%d source_event_id=%s", wire.SourceRowIndex, wire.SourceEventID)
		}
		if _, err := parseAccessMode(string(item.Mode)); err != nil {
			return nil, fmt.Errorf("canonical direct access list item error source_row_index=%d source_event_id=%s: %w", wire.SourceRowIndex, wire.SourceEventID, err)
		}
		if _, exists := byKey[item.Key]; exists {
			return nil, fmt.Errorf("duplicate direct access key source_row_index=%d source_event_id=%s key=%s", wire.SourceRowIndex, wire.SourceEventID, item.Key)
		}
		byKey[item.Key] = item
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	stateKeys := append([]string(nil), wire.StateKeys...)
	sort.Strings(stateKeys)
	if len(keys) != len(stateKeys) {
		return nil, fmt.Errorf("direct access/state key count mismatch source_row_index=%d source_event_id=%s", wire.SourceRowIndex, wire.SourceEventID)
	}
	out := make([]tx.AccessItem, 0, len(keys))
	for index, key := range keys {
		if stateKeys[index] != key {
			return nil, fmt.Errorf("direct access/state key mismatch source_row_index=%d source_event_id=%s", wire.SourceRowIndex, wire.SourceEventID)
		}
		out = append(out, byKey[key])
	}
	return out, nil
}

func resolveAccessTemplate(plan WorkloadPlan, wire canonicalWireRecord) ([]tx.AccessItem, error) {
	if wire.AccessListSchema != "dcl_sale_access_template_v1" || wire.AccessListSource != "semantics_derived" {
		return nil, fmt.Errorf("canonical access template error source_row_index=%d source_event_id=%s schema=%s role=<schema>", wire.SourceRowIndex, wire.SourceEventID, wire.AccessListSchema)
	}
	if strings.TrimSpace(wire.Category) == "" || strings.TrimSpace(wire.Contract) == "" {
		return nil, fmt.Errorf("canonical access template error source_row_index=%d source_event_id=%s schema=%s role=<category_contract>", wire.SourceRowIndex, wire.SourceEventID, wire.AccessListSchema)
	}
	byKey := map[string]tx.AccessItem{}
	for _, template := range wire.AccessTemplate {
		key, mode, err := resolveTemplateRole(plan, wire, template)
		if err != nil {
			return nil, err
		}
		if existing, ok := byKey[key]; ok {
			return nil, fmt.Errorf("duplicate resolved access key source_row_index=%d source_event_id=%s role=%s schema=%s key=%s existing_mode=%s new_mode=%s", wire.SourceRowIndex, wire.SourceEventID, template.Role, wire.AccessListSchema, key, existing.Mode, mode)
		}
		byKey[key] = tx.AccessItem{Key: key, Mode: mode, UpdateSemantics: template.Semantics, Delta: template.Delta}
	}
	required := []string{"balance:" + canonicalRuntimeSenderAddress(plan, strings.ToLower(wire.SenderID)), "nonce:" + canonicalRuntimeSenderAddress(plan, strings.ToLower(wire.SenderID)), "balance:receiver_" + strings.ToLower(wire.ReceiverID), "market:" + strings.ToLower(wire.Contract), "category:" + strings.ToLower(wire.Category)}
	for _, key := range required {
		if _, ok := byKey[key]; !ok {
			return nil, fmt.Errorf("missing resolved access key source_row_index=%d source_event_id=%s schema=%s key=%s", wire.SourceRowIndex, wire.SourceEventID, wire.AccessListSchema, key)
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tx.AccessItem, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out, nil
}

func resolveTemplateRole(plan WorkloadPlan, wire canonicalWireRecord, template canonicalWireAccessTemplate) (string, tx.AccessMode, error) {
	mode, err := parseAccessMode(template.Mode)
	if err != nil {
		return "", "", fmt.Errorf("canonical access template error source_row_index=%d source_event_id=%s role=%s schema=%s: %w", wire.SourceRowIndex, wire.SourceEventID, template.Role, wire.AccessListSchema, err)
	}
	senderID := strings.ToLower(wire.SenderID)
	receiverID := strings.ToLower(wire.ReceiverID)
	switch template.Role {
	case "sender_balance":
		return "balance:" + canonicalRuntimeSenderAddress(plan, senderID), mode, nil
	case "sender_nonce":
		return "nonce:" + canonicalRuntimeSenderAddress(plan, senderID), mode, nil
	case "receiver_balance":
		return "balance:receiver_" + receiverID, mode, nil
	case "receiver_nonce":
		return "nonce:receiver_" + receiverID, mode, nil
	case "market_contract":
		if mode != tx.AccessCommutativeDelta || template.Delta != 1 {
			return "", "", fmt.Errorf("canonical access template error source_row_index=%d source_event_id=%s role=%s schema=%s: market delta must be commutative_delta(+1)", wire.SourceRowIndex, wire.SourceEventID, template.Role, wire.AccessListSchema)
		}
		return "market:" + strings.ToLower(wire.Contract), mode, nil
	case "category_metadata":
		return "category:" + strings.ToLower(wire.Category), mode, nil
	default:
		return "", "", fmt.Errorf("canonical access template error source_row_index=%d source_event_id=%s role=%s schema=%s: unknown role", wire.SourceRowIndex, wire.SourceEventID, template.Role, wire.AccessListSchema)
	}
}

func parseAccessMode(value string) (tx.AccessMode, error) {
	switch tx.AccessMode(value) {
	case tx.AccessRead, tx.AccessWrite, tx.AccessReadWrite, tx.AccessCommutativeDelta:
		return tx.AccessMode(value), nil
	default:
		return "", fmt.Errorf("invalid access mode %q", value)
	}
}

func canonicalLegacyAccessList(wire canonicalWireRecord) ([]tx.AccessItem, error) {
	type accessSpec struct {
		mode      tx.AccessMode
		semantics string
	}
	byKey := map[string]accessSpec{}
	add := func(key string, mode tx.AccessMode, semantics string) {
		key = strings.TrimSpace(key)
		if key == "" {
			return
		}
		current, ok := byKey[key]
		if !ok || accessModeRank(mode) > accessModeRank(current.mode) {
			byKey[key] = accessSpec{mode: mode, semantics: semantics}
			return
		}
		if current.semantics == "dataset_state_key" && semantics != "" {
			current.semantics = semantics
			byKey[key] = current
		}
	}
	for _, key := range wire.StateKeys {
		add(key, tx.AccessReadWrite, "dataset_state_key")
	}
	add(wire.RoutingSourceKey, tx.AccessReadWrite, "routing_source_state")
	add(wire.RoutingTargetKey, tx.AccessReadWrite, "routing_target_state")
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tx.AccessItem, 0, len(keys))
	for _, key := range keys {
		spec := byKey[key]
		out = append(out, tx.AccessItem{Key: key, Mode: spec.mode, UpdateSemantics: spec.semantics})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("canonical legacy access inference error source_row_index=%d source_event_id=%s schema=%s", wire.SourceRowIndex, wire.SourceEventID, wire.SchemaVersion)
	}
	return out, nil
}

func CanonicalAccessListDigest(items []tx.AccessItem) string {
	normalized := append([]tx.AccessItem(nil), items...)
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Key != normalized[j].Key {
			return normalized[i].Key < normalized[j].Key
		}
		if normalized[i].Mode != normalized[j].Mode {
			return normalized[i].Mode < normalized[j].Mode
		}
		if normalized[i].UpdateSemantics != normalized[j].UpdateSemantics {
			return normalized[i].UpdateSemantics < normalized[j].UpdateSemantics
		}
		return normalized[i].Delta < normalized[j].Delta
	})
	payload, _ := json.Marshal(normalized)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func accessListHasKey(items []tx.AccessItem, key string) bool {
	for _, item := range items {
		if item.Key == key {
			return true
		}
	}
	return false
}

func accessModeRank(mode tx.AccessMode) int {
	switch mode {
	case tx.AccessRead:
		return 1
	case tx.AccessWrite:
		return 2
	case tx.AccessReadWrite:
		return 3
	case tx.AccessCommutativeDelta:
		return 4
	default:
		return 0
	}
}

func workloadPath(dataDir, relative string) (string, error) {
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(filepath.ToSlash(relative), "../") {
		return "", fmt.Errorf("unsafe materialized workload path")
	}
	cwd, _ := os.Getwd()
	candidates := []string{}
	if envRoot := strings.TrimSpace(os.Getenv("MBE_WORKLOAD_CACHE_ROOT")); envRoot != "" {
		candidates = append(candidates, envRoot)
	}
	candidates = append(candidates,
		filepath.Join(dataDir, ".cache", "workloads"),
		filepath.Join(dataDir, "..", "..", "workloads"),
		filepath.Join(cwd, ".cache", "workloads"),
		filepath.Join(cwd, "..", ".cache", "workloads"),
	)
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

func shardIndexFor(sharding ShardingPlugin, keys []string, shards int) int {
	if shards <= 0 {
		return 0
	}
	if sharding != nil {
		shardIDs := make([]string, 0, shards)
		for index := 0; index < shards; index++ {
			shardIDs = append(shardIDs, fmt.Sprintf("s%d", index))
		}
		selected := sharding.ShardFor(keys, shardIDs)
		for index, shard := range shardIDs {
			if shard == selected {
				return index
			}
		}
	}
	return stableShard(keys, shards)
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

func cloneAnyMap(input map[string]any) map[string]any {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]any, len(input))
	for key, value := range input {
		out[key] = value
	}
	return out
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
