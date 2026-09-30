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
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	txalloShardingID         = "txallo_account_sharding"
	txalloRoutingID          = "txallo_routing"
	txalloStatelessRoutingID = "stateless_txallo_routing"
	txalloTruthBoundary      = "txallo_icde2023_algorithm1_2_account_graph_frozen_pre_evaluation_history_v1"
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
	mu        sync.RWMutex
	allocator *txalloAllocator
	shards    []string
	aliases   map[string]string // runtime account/address -> logical TxAllo account
	evidence  map[string]any
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
	defer p.mu.RUnlock()
	logical := account
	if x := p.aliases[account]; x != "" {
		logical = x
	}
	if p.allocator != nil {
		if s := p.allocator.Mapping[logical]; s != "" {
			return s
		}
	}
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

func (p *txalloAccountSharding) BootstrapHistoricalAllocation(ctx context.Context, input HistoricalAllocationBootstrapInput) error {
	started := time.Now()
	shards := append([]string(nil), input.ShardIDs...)
	sort.Strings(shards)
	if len(shards) == 0 {
		return fmt.Errorf("TxAllo requires at least one shard")
	}
	eta := p.configuredFloat("eta", 2)
	lambda := p.configuredFloat("lambda", 0)
	epsilon := p.configuredFloat("epsilon", 0)
	historyLimit := p.configuredInt("history_records", 5000)
	adaptiveChunk := p.configuredInt("adaptive_chunk_records", 500)
	history := []txalloHistoryTx{}
	aliases := map[string]string{}
	cutoff := -1
	source := "synthetic_cold_start_no_future_history"
	if input.Plan.SourceType == "dataset" && input.Plan.MaterializedRelativePath != "" && input.Plan.CanonicalRelativePath != "" {
		evalPath, err := workloadPath(input.DataDir, input.Plan.MaterializedRelativePath)
		if err != nil {
			return err
		}
		cutoff, err = readFirstCanonicalSourceRow(evalPath)
		if err != nil {
			return fmt.Errorf("TxAllo evaluation cutoff: %w", err)
		}
		canonicalPath, err := workloadPath(input.DataDir, input.Plan.CanonicalRelativePath)
		if err != nil {
			return err
		}
		rows, err := readTxAlloCanonicalHistory(canonicalPath, cutoff, historyLimit)
		if err != nil {
			return fmt.Errorf("TxAllo history: %w", err)
		}
		history = txalloHistoryFromCanonical(rows)
		source = "canonical_pre_evaluation_history"
		for _, r := range rows {
			logicalSender := strings.ToLower(r.SenderID)
			if logicalSender != "" {
				aliases[strings.ToLower(canonicalRuntimeSenderAddress(input.Plan, logicalSender))] = logicalSender
			}
			logicalReceiver := strings.ToLower(r.ReceiverID)
			if logicalReceiver != "" {
				aliases["receiver_"+logicalReceiver] = logicalReceiver
			}
		}
	}
	alloc := newTxAlloAllocator(shards, eta, lambda, epsilon)
	gCount, aCount := 0, 0
	if len(history) > 0 {
		firstEnd := len(history)
		if adaptiveChunk > 0 && len(history) > adaptiveChunk {
			firstEnd = len(history) % adaptiveChunk
			if firstEnd == 0 {
				firstEnd = adaptiveChunk
			}
		}
		alloc.RunG(history[:firstEnd])
		gCount++
		for start := firstEnd; start < len(history); start += adaptiveChunk {
			if err := ctx.Err(); err != nil {
				return err
			}
			end := start + adaptiveChunk
			if end > len(history) {
				end = len(history)
			}
			alloc.RunA(history[start:end])
			aCount++
		}
	} else {
		// No committed pre-window history exists (for example a prefix starting at row 0
		// or a synthetic workload). Keep the mapping empty and route unseen accounts by
		// deterministic hash. Never train on the evaluation batch itself.
		alloc.params()
	}
	mapping := copyMapping(alloc.Mapping)
	digest := stableJSONDigest(mapping)
	objective := txalloEvaluate(alloc.Graph, mapping, shards, alloc.Eta, alloc.Lambda)
	p.mu.Lock()
	p.allocator = alloc
	p.shards = shards
	p.aliases = aliases
	p.evidence = map[string]any{"truth_boundary": txalloTruthBoundary, "history_source": source, "history_cutoff_source_row_index": cutoff, "history_transaction_count": len(history), "history_limit": historyLimit, "adaptive_chunk_records": adaptiveChunk, "g_txallo_run_count": gCount, "a_txallo_run_count": aCount, "graph_account_count": len(alloc.Graph.Nodes), "graph_edge_count": len(alloc.Graph.Edges), "eta": alloc.Eta, "lambda": alloc.Lambda, "epsilon": alloc.Epsilon, "mapping_digest": digest, "mapped_account_count": len(mapping), "modeled_throughput": objective.Throughput, "modeled_cross_shard_ratio": objective.CrossShardRatio, "modeled_workload_stddev": objective.WorkloadStdDev, "bootstrap_ms": time.Since(started).Milliseconds(), "future_evaluation_transactions_used": 0}
	p.mu.Unlock()
	return nil
}

type txalloRouting struct {
	basicPlugin
	stateless bool
}

func (p txalloRouting) BatchRoutingArtifactFamily() string { return "txallo" }
func (p txalloRouting) ApplyAccountPlacement(record WorkloadRecord, placement TransactionPlacement) WorkloadRecord {
	record.SourceShard = placement.HomeShard
	record.TargetShard = placement.TargetShard
	record.CrossShard = placement.TargetShard != "" && placement.TargetShard != placement.HomeShard
	if record.CrossShard && !strings.HasPrefix(record.Payload, "v5_cross:") {
		record.Payload = "v5_cross:" + placement.TargetShard + ":" + record.Payload
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
