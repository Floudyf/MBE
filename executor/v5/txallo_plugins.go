package v5

import (
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

const (
	txalloShardingID          = "txallo_account_sharding"
	txalloRoutingID           = "txallo_routing"
	txalloStatelessRoutingID  = "stateless_txallo_routing"
	txalloNoRelayCrossShardID = "txallo_no_relay"
	txalloTruthBoundary       = "txallo_icde2023_algorithm1_2_deterministic_multilevel_louvain_g_snapshot_v2"
)

type HistoricalAllocationBootstrapInput struct {
	Plan     WorkloadPlan
	DataDir  string
	ShardIDs []string
}
type HistoricalAllocationBootstrapper interface {
	BootstrapHistoricalAllocation(context.Context, HistoricalAllocationBootstrapInput) error
	HistoricalAllocationEvidence() map[string]any
}
type txalloAccountMappingProvider interface {
	ShardingPlugin
	ShardForAccount(string, []string) string
	TxAlloMappingSnapshot() map[string]string
	TxAlloEvidenceSnapshot() map[string]any
}
type BatchRoutingArtifactCapability interface {
	BatchRoutingArtifactFamily() string
}
type AccountPlacementRoutingCapability interface {
	RoutingPlugin
	ApplyAccountPlacement(WorkloadRecord, TransactionPlacement) WorkloadRecord
}

// Keep MetaTrack evidence attached to MetaTrack only. Merely implementing
// BatchRoutingPlugin must never cause another algorithm to emit MetaTrack files.
func (p *metaTrackRouting) BatchRoutingArtifactFamily() string { return "metatrack" }

type txalloAccountSharding struct {
	basicPlugin
	mu                     sync.RWMutex
	allocator              *txalloAllocator
	shards                 []string
	aliases                map[string]string // runtime account/address -> logical TxAllo account
	evidence               map[string]any
	provisionalAccounts    map[string]bool
	provisionalLookupCount int
}

func (p *txalloAccountSharding) configuredFloat(key string, fallback float64) float64 {
	if v, ok := p.config[key]; ok {
		if f := floatValue(v); f > 0 {
			return f
		}
	}
	return fallback
}
func (p *txalloAccountSharding) configuredInt(key string, fallback int) int {
	if v := intValue(p.config[key]); v > 0 {
		return v
	}
	return fallback
}
func txalloAccountFromStateKey(key string) string {
	key = strings.TrimSpace(key)
	for _, prefix := range []string{"balance:", "nonce:"} {
		if strings.HasPrefix(key, prefix) {
			return strings.TrimPrefix(key, prefix)
		}
	}
	return ""
}
func (p *txalloAccountSharding) fallbackShard(account string, shards []string) string {
	if len(shards) == 0 {
		return ""
	}
	return shards[stableKey([]string{"txallo_account:" + account})%len(shards)]
}
func (p *txalloAccountSharding) ShardForAccount(account string, shards []string) string {
	account = strings.ToLower(strings.TrimSpace(account))
	if account == "" {
		return p.fallbackShard(account, shards)
	}
	p.mu.RLock()
	logical := account
	if x := p.aliases[account]; x != "" {
		logical = x
	}
	if p.allocator != nil {
		if s := p.allocator.Mapping[logical]; s != "" {
			p.mu.RUnlock()
			return s
		}
	}
	p.mu.RUnlock()
	p.mu.Lock()
	if p.provisionalAccounts == nil {
		p.provisionalAccounts = map[string]bool{}
	}
	p.provisionalAccounts[logical] = true
	p.provisionalLookupCount++
	p.mu.Unlock()
	return p.fallbackShard(account, shards)
}
func (p *txalloAccountSharding) ShardFor(keys, shards []string) string {
	accounts := []string{}
	for _, key := range keys {
		if a := txalloAccountFromStateKey(key); a != "" {
			accounts = append(accounts, a)
		}
	}
	accounts = txalloUniqueSortedStrings(accounts)
	if len(accounts) > 0 {
		return p.ShardForAccount(accounts[0], shards)
	}
	if len(shards) == 0 {
		return ""
	}
	return shards[stableKey(keys)%len(shards)]
}
func (p *txalloAccountSharding) TxAlloMappingSnapshot() map[string]string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.allocator == nil {
		return map[string]string{}
	}
	return copyMapping(p.allocator.Mapping)
}
func (p *txalloAccountSharding) TxAlloEvidenceSnapshot() map[string]any {
	p.mu.RLock()
	defer p.mu.RUnlock()
	out := map[string]any{}
	for k, v := range p.evidence {
		out[k] = v
	}
	out["provisional_account_count"] = len(p.provisionalAccounts)
	out["provisional_lookup_count"] = p.provisionalLookupCount
	return out
}
func (p *txalloAccountSharding) HistoricalAllocationEvidence() map[string]any {
	return p.TxAlloEvidenceSnapshot()
}

func readFirstCanonicalSourceRow(path string) (int, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return 0, err
	}
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), maxWorkloadRecordBytes)
	if !sc.Scan() {
		if sc.Err() != nil {
			return 0, sc.Err()
		}
		return 0, io.EOF
	}
	var row canonicalWireRecord
	if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
		return 0, err
	}
	return row.SourceRowIndex, nil
}
func readTxAlloCanonicalHistory(path string, cutoff, limit int) ([]canonicalWireRecord, error) {
	if cutoff <= 0 || limit <= 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return nil, err
	}
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), maxWorkloadRecordBytes)
	ring := make([]canonicalWireRecord, 0, limit)
	for sc.Scan() {
		var row canonicalWireRecord
		if err := json.Unmarshal(sc.Bytes(), &row); err != nil {
			return nil, err
		}
		if row.SourceRowIndex >= cutoff {
			break
		}
		if strings.TrimSpace(row.SenderID) == "" {
			continue
		}
		if len(ring) < limit {
			ring = append(ring, row)
		} else {
			copy(ring, ring[1:])
			ring[len(ring)-1] = row
		}
	}
	if err := sc.Err(); err != nil {
		return nil, err
	}
	return ring, nil
}
func txalloHistoryFromCanonical(rows []canonicalWireRecord) []txalloHistoryTx {
	out := make([]txalloHistoryTx, 0, len(rows))
	for _, r := range rows {
		accounts := txalloUniqueSortedStrings([]string{strings.ToLower(r.SenderID), strings.ToLower(r.ReceiverID)})
		if len(accounts) > 0 {
			out = append(out, txalloHistoryTx{Accounts: accounts})
		}
	}
	return out
}

type txalloHistorySidecarRow struct {
	SchemaVersion  string   `json:"schema_version"`
	SourceOrder    int      `json:"source_order"`
	TransactionID  string   `json:"transaction_id"`
	Timestamp      string   `json:"timestamp"`
	BlockNum       int      `json:"block_num"`
	GlobalSequence int64    `json:"global_sequence"`
	SenderID       string   `json:"sender_id"`
	ReceiverID     string   `json:"receiver_id"`
	Accounts       []string `json:"accounts"`
}
type txalloGCachePayload struct {
	SchemaVersion string            `json:"schema_version"`
	CacheKey      string            `json:"cache_key"`
	Mapping       map[string]string `json:"mapping"`
	Evidence      map[string]any    `json:"evidence"`
}

func txalloRunHistoryPath(dataDir, relative string) (string, error) {
	relative = filepath.ToSlash(strings.TrimSpace(relative))
	if relative == "" || filepath.IsAbs(relative) || strings.Contains(relative, "../") || strings.HasPrefix(relative, "/") {
		return "", fmt.Errorf("unsafe TxAllo history relative path")
	}
	// mbe-client receives outDir=<child>/client. The compiler stores reviewed
	// TxAllo history under <child>/workload. Also accept dataDir itself for
	// focused tests/older launchers.
	candidates := []string{
		filepath.Join(dataDir, filepath.FromSlash(relative)),
		filepath.Join(dataDir, "..", filepath.FromSlash(relative)),
	}
	for _, candidate := range candidates {
		clean := filepath.Clean(candidate)
		if info, err := os.Stat(clean); err == nil && !info.IsDir() {
			return clean, nil
		}
	}
	return "", fmt.Errorf("TxAllo history sidecar is not available")
}
func readTxAlloHistorySidecar(path string, expected int) ([]txalloHistoryTx, []txalloHistorySidecarRow, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, nil, e
	}
	defer f.Close()
	gz, e := gzip.NewReader(f)
	if e != nil {
		return nil, nil, e
	}
	defer gz.Close()
	sc := bufio.NewScanner(gz)
	sc.Buffer(make([]byte, 64*1024), maxWorkloadRecordBytes)
	history := make([]txalloHistoryTx, 0, expected)
	rows := make([]txalloHistorySidecarRow, 0, expected)
	last := -1
	for sc.Scan() {
		var r txalloHistorySidecarRow
		if e := json.Unmarshal(sc.Bytes(), &r); e != nil {
			return nil, nil, e
		}
		if r.SchemaVersion != "mbe_txallo_history_record_v1" {
			return nil, nil, fmt.Errorf("unsupported TxAllo history schema")
		}
		if r.SourceOrder <= last {
			return nil, nil, fmt.Errorf("TxAllo history source order is not strictly increasing")
		}
		last = r.SourceOrder
		a := txalloUniqueSortedStrings(r.Accounts)
		if len(a) == 0 {
			return nil, nil, fmt.Errorf("TxAllo history row has no accounts")
		}
		history = append(history, txalloHistoryTx{Accounts: a})
		rows = append(rows, r)
	}
	if e := sc.Err(); e != nil {
		return nil, nil, e
	}
	if expected > 0 && len(history) != expected {
		return nil, nil, fmt.Errorf("TxAllo history count=%d want=%d", len(history), expected)
	}
	return history, rows, nil
}
func txalloHistoryAudit(plan WorkloadPlan) (map[string]any, error) {
	raw, ok := plan.AuditMetadata["txallo_history"]
	if !ok {
		return nil, fmt.Errorf("TxAllo history audit metadata missing")
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("TxAllo history audit metadata invalid")
	}
	if strings.TrimSpace(fmt.Sprint(m["selection_policy"])) != "preceding_ratio_v1" {
		return nil, fmt.Errorf("TxAllo history policy mismatch")
	}
	if intValue(m["future_evaluation_transactions_used"]) != 0 {
		return nil, fmt.Errorf("TxAllo future-data leakage")
	}
	return m, nil
}
func txalloGCacheKey(historySHA string, shards []string, eta, lambda, epsilon float64) string {
	return stableJSONDigest(map[string]any{"algorithm": "txallo_paper_v20_ratio_g_v21", "history_sha256": historySHA, "shards": shards, "eta": eta, "configured_lambda": lambda, "configured_epsilon": epsilon})
}
func txalloLoadGCache(path, key string, shards []string) (txalloGCachePayload, bool) {
	var p txalloGCachePayload
	b, e := os.ReadFile(path)
	if e != nil {
		return p, false
	}
	if json.Unmarshal(b, &p) != nil || p.SchemaVersion != "mbe_txallo_g_cache_v1" || p.CacheKey != key || len(p.Mapping) == 0 {
		return txalloGCachePayload{}, false
	}
	valid := map[string]bool{}
	for _, s := range shards {
		valid[s] = true
	}
	for _, s := range p.Mapping {
		if !valid[s] {
			return txalloGCachePayload{}, false
		}
	}
	n := intValue(p.Evidence["graph_account_count"])
	if n <= 0 || len(p.Mapping) != n {
		return txalloGCachePayload{}, false
	}
	return p, true
}
func txalloWriteGCache(path string, p txalloGCachePayload) error {
	if e := os.MkdirAll(filepath.Dir(path), 0o755); e != nil {
		return e
	}
	b, e := json.MarshalIndent(p, "", "  ")
	if e != nil {
		return e
	}
	tmp := fmt.Sprintf("%s.tmp-%d", path, time.Now().UnixNano())
	if e = os.WriteFile(tmp, append(b, '\n'), 0o644); e != nil {
		return e
	}
	if e = os.Rename(tmp, path); e != nil {
		_ = os.Remove(tmp)
		return e
	}
	return nil
}

func (p *txalloAccountSharding) BootstrapHistoricalAllocation(ctx context.Context, input HistoricalAllocationBootstrapInput) error {
	started := time.Now()
	shards := append([]string(nil), input.ShardIDs...)
	sort.Strings(shards)
	if len(shards) == 0 {
		return fmt.Errorf("TxAllo requires at least one shard")
	}
	mode := strings.TrimSpace(fmt.Sprint(p.config["allocation_mode"]))
	if mode == "" {
		mode = "paper_g_ratio_snapshot"
	}
	if mode != "paper_g_ratio_snapshot" {
		return fmt.Errorf("TxAllo formal mode requires paper_g_ratio_snapshot, got %q", mode)
	}
	meta, err := txalloHistoryAudit(input.Plan)
	if err != nil {
		return err
	}
	rel := strings.TrimSpace(fmt.Sprint(meta["history_relative_path"]))
	count := intValue(meta["selected_history_count"])
	if rel == "" || count <= 0 {
		return fmt.Errorf("TxAllo selected pre-evaluation history is empty")
	}
	path, err := txalloRunHistoryPath(input.DataDir, rel)
	if err != nil {
		return err
	}
	history, rows, err := readTxAlloHistorySidecar(path, count)
	if err != nil {
		return fmt.Errorf("TxAllo history sidecar: %w", err)
	}
	if len(history) == 0 {
		return fmt.Errorf("TxAllo requires non-empty pre-evaluation history")
	}
	eta := p.configuredFloat("eta", 2)
	lambda := p.configuredFloat("lambda", 0)
	epsilon := p.configuredFloat("epsilon", 0)
	hsha := strings.TrimSpace(fmt.Sprint(meta["selected_history_sha256"]))
	key := txalloGCacheKey(hsha, shards, eta, lambda, epsilon)
	cdir := strings.TrimSpace(fmt.Sprint(meta["g_cache_dir"]))
	cpath := ""
	if cdir != "" {
		cpath = filepath.Join(cdir, key+".json")
	}
	aliases := map[string]string{}
	for _, r := range rows {
		ls := strings.ToLower(strings.TrimSpace(r.SenderID))
		if ls != "" {
			aliases[strings.ToLower(canonicalRuntimeSenderAddress(input.Plan, ls))] = ls
		}
		lr := strings.ToLower(strings.TrimSpace(r.ReceiverID))
		if lr != "" {
			aliases["receiver_"+lr] = lr
		}
	}
	alloc := newTxAlloAllocator(shards, eta, lambda, epsilon)
	hit := false
	ev := map[string]any{}
	if cpath != "" {
		if cached, ok := txalloLoadGCache(cpath, key, shards); ok {
			hit = true
			alloc.Mapping = copyMapping(cached.Mapping)
			alloc.Eta = floatValue(cached.Evidence["eta"])
			alloc.Lambda = floatValue(cached.Evidence["lambda"])
			alloc.Epsilon = floatValue(cached.Evidence["epsilon"])
			for k, v := range cached.Evidence {
				ev[k] = v
			}
		}
	}
	if !hit {
		alloc.RunG(history)
		if len(alloc.Graph.Nodes) <= 0 || len(alloc.Mapping) <= 0 {
			return fmt.Errorf("TxAllo G produced empty graph/mapping")
		}
		if len(alloc.Mapping) != len(alloc.Graph.Nodes) {
			return fmt.Errorf("TxAllo G mapping incomplete")
		}
		obj := alloc.Metrics.Objective
		ev = map[string]any{"graph_account_count": len(alloc.Graph.Nodes), "graph_edge_count": len(alloc.Graph.Edges), "eta": alloc.Eta, "lambda": alloc.Lambda, "epsilon": alloc.Epsilon, "mapping_digest": stableJSONDigest(alloc.Mapping), "mapped_account_count": len(alloc.Mapping), "modeled_throughput": obj.Throughput, "modeled_cross_shard_ratio": obj.CrossShardRatio, "modeled_workload_stddev": obj.WorkloadStdDev, "louvain_level_count": alloc.Metrics.LouvainLevels}
		if cpath != "" {
			_ = txalloWriteGCache(cpath, txalloGCachePayload{SchemaVersion: "mbe_txallo_g_cache_v1", CacheKey: key, Mapping: copyMapping(alloc.Mapping), Evidence: ev})
		}
	}
	graphCount := intValue(ev["graph_account_count"])
	if graphCount <= 0 || len(alloc.Mapping) <= 0 || len(alloc.Mapping) != graphCount {
		return fmt.Errorf("TxAllo mapping is not operationally valid")
	}
	for k, v := range map[string]any{"truth_boundary": txalloTruthBoundary, "allocation_mode": mode, "history_source": "manifest_preanchor_ratio_sidecar", "history_policy": "preceding_ratio_v1", "history_ratio": floatValue(meta["history_ratio"]), "history_pool_count": intValue(meta["history_pool_count"]), "history_transaction_count": len(history), "history_window_start_source_order": intValue(meta["history_window_start_source_order"]), "history_window_end_source_order": intValue(meta["history_window_end_source_order"]), "history_selected_sha256": hsha, "g_cache_hit": hit, "g_cache_key": key, "g_txallo_run_count": 1, "g_txallo_executed_this_run": !hit, "a_txallo_run_count": 0, "dynamic_a_txallo_runtime_enabled": false, "mapping_nonempty": len(alloc.Mapping) > 0, "mapping_structurally_complete": len(alloc.Mapping) == graphCount, "mapping_operationally_valid": len(alloc.Mapping) > 0 && len(alloc.Mapping) == graphCount, "future_evaluation_transactions_used": 0, "bootstrap_ms": time.Since(started).Milliseconds()} {
		ev[k] = v
	}
	p.mu.Lock()
	p.allocator = alloc
	p.shards = shards
	p.aliases = aliases
	p.evidence = ev
	if p.provisionalAccounts == nil {
		p.provisionalAccounts = map[string]bool{}
	}
	p.mu.Unlock()
	return nil
}

type txalloRouting struct {
	basicPlugin
	stateless bool
}

func (p txalloRouting) BatchRoutingArtifactFamily() string { return "txallo" }

// MBE_TXALLO_REPRO_V202: dataset cross-shard labels are input annotations, not
// physical placement truth. Strip any legacy wrapper and rebuild it solely from
// the frozen TxAllo account allocation.
func txalloBusinessPayload(payload string) string {
	payload = strings.TrimSpace(payload)
	if !strings.HasPrefix(payload, "v5_cross:") {
		return payload
	}
	remainder := strings.TrimPrefix(payload, "v5_cross:")
	if colon := strings.Index(remainder, ":"); colon >= 0 {
		if business := remainder[colon+1:]; business != "" {
			return business
		}
	}
	return "v5_safe"
}

func (p txalloRouting) ApplyAccountPlacement(record WorkloadRecord, placement TransactionPlacement) WorkloadRecord {
	basePayload := txalloBusinessPayload(record.Payload)
	record.SourceShard = placement.HomeShard
	record.TargetShard = placement.TargetShard
	record.CrossShard = placement.TargetShard != "" && placement.TargetShard != placement.HomeShard
	record.Payload = basePayload
	if record.CrossShard {
		record.Payload = "v5_cross:" + placement.TargetShard + ":" + basePayload
	}
	return record
}
func (p txalloRouting) mapping(input BatchRoutingInput) (txalloAccountMappingProvider, bool) {
	m, ok := input.Sharding.(txalloAccountMappingProvider)
	return m, ok
}
func (p txalloRouting) PlanBatch(input BatchRoutingInput) BatchRoutingPlan {
	plan := BatchRoutingPlan{BatchIndex: input.BatchIndex, ShardingPluginID: shardingPluginID(input.Sharding), PlacementPolicy: "txallo_account_mapping_from_committed_history_v1", TransactionPolicy: "sender_account_home_execution_v1", ShardLoadBefore: map[string]int{}, ShardLoadAfter: map[string]int{}}
	for _, s := range input.ShardIDs {
		plan.ShardLoadBefore[s] = 0
		plan.ShardLoadAfter[s] = 0
	}
	mapping, ok := p.mapping(input)
	for _, record := range input.Records {
		sender := strings.ToLower(strings.TrimSpace(record.SenderID))
		receiver := strings.ToLower(strings.TrimSpace(record.ReceiverID))
		home, target := "", ""
		if ok {
			home = mapping.ShardForAccount(sender, input.ShardIDs)
			if receiver != "" {
				target = mapping.ShardForAccount(receiver, input.ShardIDs)
			}
		}
		if home == "" {
			home = shardFor(input.Sharding, []string{"txallo:" + sender}, input.ShardIDs)
		}
		if target == "" {
			target = home
		}
		remote := 0
		if target != home {
			remote = 1
		}
		plan.TransactionPlacements = append(plan.TransactionPlacements, TransactionPlacement{LogicalID: firstNonEmpty(record.LogicalID, fmt.Sprintf("tx-%d", record.Index)), TxIndex: record.Index, HomeShard: home, ExecutionShard: home, TargetShard: target, Reason: "txallo_frozen_account_mapping", RemoteAccessCount: remote})
		plan.ShardLoadAfter[home]++
		plan.RemoteAccessEstimate += remote
	}
	sort.Slice(plan.TransactionPlacements, func(i, j int) bool {
		return plan.TransactionPlacements[i].TxIndex < plan.TransactionPlacements[j].TxIndex
	})
	plan.RoutingOverhead = plan.RemoteAccessEstimate
	plan.PlanDigest = routingPlanDigest(plan)
	return plan
}
func (p txalloRouting) Route(input RoutingInput) RoutingDecision {
	d := hashRouting{p.basicPlugin}.Route(input)
	d.Reason = "txallo_fallback_route_without_batch_record_identity"
	return d
}
func (p txalloRouting) StatelessDirectExecution() bool     { return p.stateless }
func (p txalloRouting) BindExecutionRoutingMetadata() bool { return p.stateless }
func (p txalloRouting) BindBatchProjectionMetadata() bool  { return false }
func (p txalloRouting) BatchExecutionPlanAlgorithmID() string {
	return "txallo_frozen_mapping_route_plan_v1"
}
func (p txalloRouting) SignedBatchExecutionPlan() bool  { return false }
func (p txalloRouting) NativeVersionedStateReady() bool { return false }
func (p txalloRouting) StatelessVersionAdmission() bool { return p.stateless }

// Stateless-TxAllo uses remote exact-version state transport and must never
// re-execute the business transaction through MBE Relay/Finalize.
type txalloNoRelayCrossShard struct{ basicPlugin }

func (p txalloNoRelayCrossShard) IsCrossShard(tx.SignedTransaction) bool { return false }
func (p txalloNoRelayCrossShard) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "TxAlloStatelessNoRelay", Success: true}
}
func (p txalloNoRelayCrossShard) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "TxAlloStatelessNoRelay", Success: true}
}
func (p txalloNoRelayCrossShard) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "TxAlloStatelessNoRelay", Success: true}
}
func (p txalloNoRelayCrossShard) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "TxAlloStatelessNoRelay", Success: false, Error: reason}
}
func (p txalloNoRelayCrossShard) BuildRelay(input CrossShardRelayInput) Relay {
	return Relay{Tx: input.Tx, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}
func (p txalloNoRelayCrossShard) BuildFinalize(input CrossShardFinalizeInput) Finalize {
	return Finalize{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

func registerTxAlloPlugins(register func(string, string, Factory)) {
	register("sharding", txalloShardingID, func(c map[string]any) (Plugin, error) {
		return &txalloAccountSharding{basicPlugin: makeBasic("sharding", txalloShardingID, c), aliases: map[string]string{}, evidence: map[string]any{}}, nil
	})
	register("routing", txalloRoutingID, func(c map[string]any) (Plugin, error) {
		return txalloRouting{basicPlugin: makeBasic("routing", txalloRoutingID, c), stateless: false}, nil
	})
	register("routing", txalloStatelessRoutingID, func(c map[string]any) (Plugin, error) {
		return txalloRouting{basicPlugin: makeBasic("routing", txalloStatelessRoutingID, c), stateless: true}, nil
	})
	register("cross_shard", txalloNoRelayCrossShardID, func(c map[string]any) (Plugin, error) {
		return txalloNoRelayCrossShard{basicPlugin: makeBasic("cross_shard", txalloNoRelayCrossShardID, c)}, nil
	})
}
func validateTxAlloPluginCombination(p RuntimePlugins) error {
	sid, rid := "", ""
	if p.Sharding != nil {
		sid = p.Sharding.ID()
	}
	if p.Routing != nil {
		rid = p.Routing.ID()
	}
	selected := sid == txalloShardingID || rid == txalloRoutingID || rid == txalloStatelessRoutingID
	if !selected {
		return nil
	}
	if sid != txalloShardingID {
		return fmt.Errorf("TxAllo requires sharding:%s", txalloShardingID)
	}
	if rid != txalloRoutingID && rid != txalloStatelessRoutingID {
		return fmt.Errorf("TxAllo requires routing:%s or %s", txalloRoutingID, txalloStatelessRoutingID)
	}
	if p.Execution == nil || p.Execution.ID() != "serial_execution_baseline" {
		return fmt.Errorf("TxAllo placement baseline requires shared serial execution")
	}
	if p.Scheduler == nil || p.Scheduler.ID() != "fifo_serial_scheduler" {
		return fmt.Errorf("TxAllo placement baseline requires shared FIFO scheduler")
	}
	if p.BlockExecutor == nil || p.BlockExecutor.ID() != "serial_block_executor" {
		return fmt.Errorf("TxAllo placement baseline requires shared serial block executor")
	}
	if p.Consensus == nil || p.Consensus.ID() != "pbft_style_consensus" {
		return fmt.Errorf("TxAllo requires shared PBFT")
	}
	if p.CrossShard == nil {
		return fmt.Errorf("TxAllo requires an explicit cross_shard plugin")
	}
	if rid == txalloStatelessRoutingID && p.CrossShard.ID() != txalloNoRelayCrossShardID {
		return fmt.Errorf("Stateless-TxAllo requires cross_shard:%s", txalloNoRelayCrossShardID)
	}
	if rid == txalloRoutingID && p.CrossShard.ID() != "relay_certificate_protocol" {
		return fmt.Errorf("stateful TxAllo requires cross_shard:relay_certificate_protocol")
	}
	return nil
}

func txalloMappingRows(mapping map[string]string) [][]string {
	keys := make([]string, 0, len(mapping))
	for k := range mapping {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	rows := make([][]string, 0, len(keys))
	for _, k := range keys {
		rows = append(rows, []string{k, mapping[k]})
	}
	return rows
}
func txalloEvidenceDigest(e map[string]any) string {
	raw, _ := json.Marshal(e)
	h := sha256.Sum256(raw)
	return hex.EncodeToString(h[:])
}
