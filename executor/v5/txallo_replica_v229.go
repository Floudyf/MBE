package v5

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_TXALLO_PAPER_REPLICA_V229 implements only the state substrate assumed by
// TxAllo Section VII. TxAllo Algorithm 1/2, Louvain, eta/lambda/epsilon, PBFT,
// source-block cadence and history selection are intentionally untouched.
const (
	txalloPaperReplicaOrigin      = "txallo_paper_replicated_state_v229"
	txalloReplicaCommitFeedName   = "txallo_replica_commit.jsonl"
	txalloReplicaCommitFeedSchema = "mbe_txallo_replica_commit_v229"
	txalloMappingEpochHistoryName = "txallo_mapping_epochs.jsonl"
	txalloReplicaPollInterval     = 20 * time.Millisecond
)

type txalloReplicaCommitRowV229 struct {
	SchemaVersion         string `json:"schema_version"`
	Token                 string `json:"token"`
	Key                   string `json:"key"`
	RoutingOrdinal        uint64 `json:"routing_ordinal"`
	Value                 string `json:"value"`
	ValueDigest           string `json:"value_digest"`
	SourceTxID            string `json:"source_tx_id"`
	SourceShard           string `json:"source_shard"`
	MaterializedShard     string `json:"materialized_shard"`
	SourceBlockHash       string `json:"source_block_hash"`
	SourceHeight          uint64 `json:"source_height"`
	MaterializedBlockHash string `json:"materialized_block_hash"`
	MaterializedHeight    uint64 `json:"materialized_height"`
	Role                  string `json:"role"`
	Commutative           bool   `json:"commutative"`
	TimestampMS           int64  `json:"timestamp_ms"`
}

type txalloReplicaNodeStateV229 struct {
	mu     sync.Mutex
	loaded bool
	rows   map[string]txalloReplicaCommitRowV229
}

var txalloReplicaRuntimeStatesV229 sync.Map // map[*NodeRuntime]*txalloReplicaNodeStateV229

func (p *txalloAccountSharding) TxAlloStatefulReplicaEnabled() bool {
	if p == nil {
		return false
	}
	return boolFromAny(p.config["stateful_paper_replicated_state"])
}

func (r *NodeRuntime) markTxAlloReplicaPostCommitFailureV229(err error) {
	if r == nil || err == nil {
		return
	}
	r.mu.Lock()
	r.incrementRuntimeMetricLocked("txallo_replica_post_commit_failure_count")
	r.fatalExecutionError = "TxAllo paper replicated-state post-commit failure: " + err.Error()
	r.lastProgressAt = time.Now().UnixMilli()
	r.mu.Unlock()
}

func (r *NodeRuntime) txalloStatefulReplicaEnabled() bool {
	if r == nil || r.plugins.Routing == nil || r.plugins.Routing.ID() != txalloRoutingID {
		return false
	}
	p, ok := r.plugins.Sharding.(*txalloAccountSharding)
	return ok && p.TxAlloStatefulReplicaEnabled()
}

func txalloReplicaTokenV229(key string, ordinal uint64) string {
	return stableTextDigest(strings.Join([]string{"txallo-replica-v229", strings.TrimSpace(key), fmt.Sprint(ordinal)}, "|"))
}

func txalloStatefulReplicaDependenciesForRecord(record WorkloadRecord, ordinal uint64, lastWriter map[string]uint64) []tx.StateVersionDependency {
	type summary struct{ read, write bool }
	byKey := map[string]summary{}
	for _, access := range normalizedAccessItems(record) {
		if strings.TrimSpace(access.Key) == "" || access.Mode == tx.AccessCommutativeDelta {
			continue
		}
		current := byKey[access.Key]
		current.read = current.read || isReadMode(access.Mode)
		current.write = current.write || isWriteMode(access.Mode)
		byKey[access.Key] = current
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tx.StateVersionDependency, 0, len(keys))
	for _, key := range keys {
		current := byKey[key]
		dep := tx.StateVersionDependency{Key: key, RequiredVersion: lastWriter[key]}
		if current.write {
			dep.ProducedVersion = ordinal
		}
		out = append(out, dep)
	}
	for _, key := range keys {
		if byKey[key].write {
			lastWriter[key] = ordinal
		}
	}
	return out
}

func (r *NodeRuntime) txalloReplicaStateV229() *txalloReplicaNodeStateV229 {
	if r == nil {
		return nil
	}
	if existing, ok := txalloReplicaRuntimeStatesV229.Load(r); ok {
		return existing.(*txalloReplicaNodeStateV229)
	}
	created := &txalloReplicaNodeStateV229{rows: map[string]txalloReplicaCommitRowV229{}}
	actual, _ := txalloReplicaRuntimeStatesV229.LoadOrStore(r, created)
	return actual.(*txalloReplicaNodeStateV229)
}

func (r *NodeRuntime) loadTxAlloReplicaStateV229() error {
	if !r.txalloStatefulReplicaEnabled() {
		return nil
	}
	stateV := r.txalloReplicaStateV229()
	stateV.mu.Lock()
	defer stateV.mu.Unlock()
	if stateV.loaded {
		return nil
	}
	stateV.rows = map[string]txalloReplicaCommitRowV229{}
	path := filepath.Join(r.node.DataDir, txalloReplicaCommitFeedName)
	f, err := txalloOpenFileStable(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		empty, createErr := txalloOpenAppendFileStable(path, 0o644)
		if createErr != nil {
			return createErr
		}
		if closeErr := empty.Close(); closeErr != nil {
			return closeErr
		}
		stateV.loaded = true
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	s.Buffer(buf, 4*1024*1024)
	for s.Scan() {
		if len(strings.TrimSpace(s.Text())) == 0 {
			continue
		}
		var row txalloReplicaCommitRowV229
		if err := json.Unmarshal(s.Bytes(), &row); err != nil {
			return fmt.Errorf("TxAllo replica feed decode: %w", err)
		}
		if row.SchemaVersion != txalloReplicaCommitFeedSchema || row.Token == "" || row.Key == "" || row.RoutingOrdinal == 0 {
			return fmt.Errorf("TxAllo replica feed contains invalid row")
		}
		if row.ValueDigest != stateValueDigest(row.Value) {
			return fmt.Errorf("TxAllo replica feed value digest mismatch token=%s", row.Token)
		}
		if old, exists := stateV.rows[row.Token]; exists {
			if old.Key != row.Key || old.RoutingOrdinal != row.RoutingOrdinal || old.SourceTxID != row.SourceTxID || old.SourceShard != row.SourceShard || old.Commutative != row.Commutative || (!row.Commutative && (old.ValueDigest != row.ValueDigest || old.Value != row.Value)) {
				return fmt.Errorf("TxAllo replica feed conflict token=%s", row.Token)
			}
			continue
		}
		stateV.rows[row.Token] = row
		if !row.Commutative {
			r.mu.Lock()
			if r.stateVersionMaterialized == nil {
				r.stateVersionMaterialized = map[string]uint64{}
			}
			r.publishStateVersionLocked(row.Key, row.RoutingOrdinal, row.Value)
			if row.RoutingOrdinal > r.stateVersionMaterialized[row.Key] {
				r.stateVersionMaterialized[row.Key] = row.RoutingOrdinal
			}
			r.mu.Unlock()
		}
	}
	if err := s.Err(); err != nil {
		return err
	}
	stateV.loaded = true
	return nil
}

func (r *NodeRuntime) cleanupTxAlloReplicaStateV229() {
	if r != nil {
		txalloReplicaRuntimeStatesV229.Delete(r)
	}
}

func (r *NodeRuntime) txalloReplicaAppliedV229(key string, ordinal uint64) bool {
	if ordinal == 0 {
		return true
	}
	stateV := r.txalloReplicaStateV229()
	if stateV == nil {
		return false
	}
	stateV.mu.Lock()
	defer stateV.mu.Unlock()
	_, ok := stateV.rows[txalloReplicaTokenV229(key, ordinal)]
	return ok
}

func (r *NodeRuntime) txalloReplicaMaterializedVersionV229(key string) uint64 {
	if r == nil || key == "" {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stateVersionMaterialized[key]
}

func (r *NodeRuntime) txalloReplicaExactValueV229(key string, version uint64) (string, bool) {
	if r == nil || key == "" || version == 0 {
		return "", false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	values := r.stateVersionValues[key]
	if values == nil {
		return "", false
	}
	value, ok := values[version]
	return value, ok
}

func (r *NodeRuntime) txalloReplicaExternalDependencyReadyV229(dep tx.StateVersionDependency) (bool, error) {
	key, required := dep.Key, dep.RequiredVersion
	if required == 0 {
		return true, nil
	}
	materialized := r.txalloReplicaMaterializedVersionV229(key)
	if materialized < required {
		return false, nil
	}
	if !r.txalloReplicaAppliedV229(key, required) {
		return false, nil
	}
	if materialized == required {
		return true, nil
	}
	// A writer must never fork from a predecessor that has already been superseded.
	// A read-only dependency may still consume an older exact committed value from
	// the local replicated version history; no remote fetch is introduced.
	if dep.ProducedVersion != 0 {
		return false, fmt.Errorf("TxAllo exact writer predecessor superseded at replicated execution shard key=%s required=%d materialized=%d", key, required, materialized)
	}
	_, ok := r.txalloReplicaExactValueV229(key, required)
	return ok, nil
}

func (r *NodeRuntime) recordTxAlloReplicaCommitV229(row txalloReplicaCommitRowV229) error {
	if !r.txalloStatefulReplicaEnabled() {
		return nil
	}
	if row.Key == "" || row.RoutingOrdinal == 0 {
		return fmt.Errorf("TxAllo replica durable row missing key/ordinal")
	}
	row.SchemaVersion = txalloReplicaCommitFeedSchema
	row.Token = txalloReplicaTokenV229(row.Key, row.RoutingOrdinal)
	if row.ValueDigest == "" {
		row.ValueDigest = stateValueDigest(row.Value)
	}
	if row.ValueDigest != stateValueDigest(row.Value) {
		return fmt.Errorf("TxAllo replica durable row value digest mismatch key=%s ordinal=%d", row.Key, row.RoutingOrdinal)
	}
	row.MaterializedShard = r.node.ShardID
	row.TimestampMS = time.Now().UnixMilli()
	stateV := r.txalloReplicaStateV229()
	stateV.mu.Lock()
	defer stateV.mu.Unlock()
	if old, exists := stateV.rows[row.Token]; exists {
		if old.Key != row.Key || old.RoutingOrdinal != row.RoutingOrdinal || old.SourceTxID != row.SourceTxID || old.SourceShard != row.SourceShard || old.Commutative != row.Commutative || (!row.Commutative && (old.ValueDigest != row.ValueDigest || old.Value != row.Value)) {
			return fmt.Errorf("TxAllo durable replica conflict key=%s ordinal=%d", row.Key, row.RoutingOrdinal)
		}
		return nil
	}
	payload, err := json.Marshal(row)
	if err != nil {
		return err
	}
	path := filepath.Join(r.node.DataDir, txalloReplicaCommitFeedName)
	f, err := txalloOpenAppendFileStable(path, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(payload, '\n')); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	stateV.rows[row.Token] = row
	return nil
}

func txalloReplicaTargetCommitV229(item tx.SignedTransaction, shardID string) bool {
	if !strings.HasPrefix(item.Payload, "v5_cross:") {
		return false
	}
	return crossTargetShard(item.Payload) == shardID
}

func txalloAccessWriteV229(access tx.AccessItem) bool {
	return isWriteMode(access.Mode)
}

func txalloAccessCommutativeV229(access tx.AccessItem) bool {
	return access.Mode == tx.AccessCommutativeDelta || access.UpdateSemantics == "commutative_delta"
}

func (r *NodeRuntime) publishDurableTxAlloExactV229(key string, version uint64, value string) error {
	if version == 0 || key == "" {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if values := r.stateVersionValues[key]; values != nil {
		if old, ok := values[version]; ok && old != value {
			r.incrementRuntimeMetricLocked("txallo_replica_exact_version_conflict_count")
			return fmt.Errorf("TxAllo exact replica conflict key=%s version=%d", key, version)
		}
	}
	r.publishStateVersionLocked(key, version, value)
	if r.stateVersionMaterialized == nil {
		r.stateVersionMaterialized = map[string]uint64{}
	}
	if version > r.stateVersionMaterialized[key] {
		r.stateVersionMaterialized[key] = version
	}
	return nil
}

func (r *NodeRuntime) markTxAlloReplicaDurableV229(block realblock.Block, txDeltas []execution.TxDelta) error {
	if !r.txalloStatefulReplicaEnabled() {
		return nil
	}
	deltaByID := map[string]execution.TxDelta{}
	for _, delta := range txDeltas {
		deltaByID[delta.TxID] = delta
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			continue
		}
		delta, ok := deltaByID[item.TxID]
		if !ok || !delta.Success {
			continue
		}
		ordinal := item.ExecutionRouting.RoutingOrdinal
		for _, access := range item.AccessList {
			if strings.TrimSpace(access.Key) == "" || !txalloAccessWriteV229(access) {
				continue
			}
			value, wrote := writeSetLogicalValue(delta.WriteSet, access.Key)
			if !wrote {
				value = r.db.Get(access.Key)
			}
			commutative := txalloAccessCommutativeV229(access)
			if !commutative {
				dep, exists := stateVersionDependencyForKey(item, access.Key)
				if !exists || dep.ProducedVersion != ordinal {
					return fmt.Errorf("TxAllo stateful replica missing signed produced version key=%s tx=%s", access.Key, item.TxID)
				}
				if err := r.publishDurableTxAlloExactV229(access.Key, dep.ProducedVersion, value); err != nil {
					return err
				}
			}
			if err := r.recordTxAlloReplicaCommitV229(txalloReplicaCommitRowV229{
				Key: access.Key, RoutingOrdinal: ordinal, Value: value, ValueDigest: stateValueDigest(value),
				SourceTxID: item.TxID, SourceShard: r.node.ShardID,
				SourceBlockHash: block.BlockHash, SourceHeight: block.Height,
				MaterializedBlockHash: block.BlockHash, MaterializedHeight: block.Height,
				Role: "source_local", Commutative: commutative,
			}); err != nil {
				return err
			}
		}
	}
	for _, item := range block.SystemStateDeltas {
		if item.ApplyOrigin != txalloPaperReplicaOrigin || item.Key == "" || item.RoutingOrdinal == 0 {
			continue
		}
		commutative := item.UpdateSemantics == "commutative_delta"
		value := item.Value
		if commutative {
			// A commutative SystemStateDelta stores a delta, not the resulting value.
			// The durable feed only uses its token for the epoch barrier, so record
			// the target shard's post-commit value as diagnostic evidence.
			value = r.db.Get(item.Key)
		}
		if !commutative && item.ProducedVersion > 0 {
			if err := r.publishDurableTxAlloExactV229(item.Key, item.ProducedVersion, value); err != nil {
				return err
			}
		}
		sourceTxID := item.TxID
		if sourceTxID == "" && len(item.TxIDs) > 0 {
			sourceTxID = item.TxIDs[0]
		}
		if err := r.recordTxAlloReplicaCommitV229(txalloReplicaCommitRowV229{
			Key: item.Key, RoutingOrdinal: item.RoutingOrdinal, Value: value, ValueDigest: stateValueDigest(value),
			SourceTxID: sourceTxID, SourceShard: item.ExecutionShard,
			SourceBlockHash: item.SourceBlockHash, SourceHeight: item.SourceHeight,
			MaterializedBlockHash: block.BlockHash, MaterializedHeight: block.Height,
			Role: "replica_system", Commutative: commutative,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *NodeRuntime) replicateTxAlloCommittedBlockV229(ctx context.Context, block realblock.Block, txDeltas []execution.TxDelta) error {
	if !r.txalloStatefulReplicaEnabled() || len(r.shardIDs()) < 2 || !r.isCurrentLeader() {
		return nil
	}
	deltaByID := map[string]execution.TxDelta{}
	for _, delta := range txDeltas {
		deltaByID[delta.TxID] = delta
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			continue
		}
		delta, ok := deltaByID[item.TxID]
		if !ok || !delta.Success {
			continue
		}
		ordinal := item.ExecutionRouting.RoutingOrdinal
		for _, access := range item.AccessList {
			if strings.TrimSpace(access.Key) == "" || !txalloAccessWriteV229(access) {
				continue
			}
			value, wrote := writeSetLogicalValue(delta.WriteSet, access.Key)
			if !wrote {
				value = r.db.Get(access.Key)
			}
			kv := state.StateKV{
				Key: qualifyStateKey(r.node.ShardID, access.Key), Value: value,
				TxIDs: []string{item.TxID}, UpdateSemantics: access.UpdateSemantics,
				ApplyOrigin: txalloPaperReplicaOrigin, DeltaKind: "txallo_paper_replica",
				RoutingOrdinal: ordinal,
			}
			if txalloAccessCommutativeV229(access) {
				kv.UpdateSemantics = "commutative_delta"
				kv.Delta = access.Delta
			} else {
				dep, exists := stateVersionDependencyForKey(item, access.Key)
				if !exists || dep.ProducedVersion != ordinal {
					return fmt.Errorf("TxAllo replica dissemination missing signed version key=%s tx=%s", access.Key, item.TxID)
				}
				kv.PreviousVersion = dep.RequiredVersion
				kv.ProducedVersion = dep.ProducedVersion
			}
			for _, targetShard := range r.shardIDs() {
				if targetShard == r.node.ShardID {
					continue
				}
				acks, _, err := r.applyRemoteStateDelta(ctx, block, kv, access.Key, targetShard, txDeltas)
				if err != nil {
					return fmt.Errorf("TxAllo replica dissemination %s to %s: %w", access.Key, targetShard, err)
				}
				r.addRuntimeMetric("txallo_replica_enqueue_ack_count", int64(len(acks)))
			}
			r.addRuntimeMetric("txallo_replica_write_fanout_count", int64(len(r.shardIDs())-1))
		}
	}
	return nil
}

func (r *NodeRuntime) txalloReplicaAdmissionEnabledV229(block realblock.Block) bool {
	if !r.txalloStatefulReplicaEnabled() || len(block.TxList) == 0 || len(r.shardIDs()) < 2 {
		return false
	}
	for _, item := range block.TxList {
		if item.ExecutionRouting != nil && len(item.ExecutionRouting.StateVersions) > 0 && item.ExecutionRouting.ExecutionShard == r.node.ShardID {
			return true
		}
	}
	return false
}

func (r *NodeRuntime) admitTxAlloReplicaCandidateV229(block realblock.Block) (realblock.Block, []tx.SignedTransaction, error) {
	if !r.txalloReplicaAdmissionEnabledV229(block) {
		return block, nil, nil
	}
	producer := map[string]int{}
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			continue
		}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.Key != "" && dep.ProducedVersion > 0 {
				producer[txalloReplicaTokenV229(dep.Key, dep.ProducedVersion)] = index
			}
		}
	}

	selected := make([]bool, len(block.TxList))
	baseRequired := map[string]uint64{}
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			selected[index] = true
			continue
		}
		ready := true
		pendingBase := map[string]uint64{}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.Key == "" || dep.RequiredVersion == 0 {
				continue
			}
			token := txalloReplicaTokenV229(dep.Key, dep.RequiredVersion)
			if owner, ok := producer[token]; ok {
				if owner >= index || !selected[owner] {
					ready = false
					break
				}
				continue
			}
			externalReady, err := r.txalloReplicaExternalDependencyReadyV229(dep)
			if err != nil {
				return realblock.Block{}, nil, err
			}
			if !externalReady {
				ready = false
				break
			}
			if previous, exists := baseRequired[dep.Key]; exists && previous != dep.RequiredVersion {
				// One serial block has one starting snapshot. Distinct external exact
				// versions of the same key are split across blocks rather than silently
				// collapsing one reader onto another reader's value.
				ready = false
				break
			}
			pendingBase[dep.Key] = dep.RequiredVersion
		}
		if ready {
			selected[index] = true
			for key, version := range pendingBase {
				baseRequired[key] = version
			}
		}
	}

	admittedItems := make([]tx.SignedTransaction, 0, len(block.TxList))
	deferred := make([]tx.SignedTransaction, 0, len(block.TxList))
	for index, item := range block.TxList {
		if selected[index] {
			admittedItems = append(admittedItems, item)
		} else {
			deferred = append(deferred, item)
		}
	}
	r.addRuntimeMetric("txallo_replica_admission_candidate_tx_count", int64(len(block.TxList)))
	r.addRuntimeMetric("txallo_replica_admission_deferred_tx_count", int64(len(deferred)))
	if len(admittedItems) == 0 {
		return realblock.Block{}, deferred, nil
	}
	admitted, err := r.proposer.BuildFromReserved(admittedItems, time.UnixMilli(block.Timestamp))
	return admitted, deferred, err
}

func (r *NodeRuntime) prepareTxAlloReplicaProjectionV229(block realblock.Block, stateBefore map[string]string) (map[string]string, bool, error) {
	if !r.txalloStatefulReplicaEnabled() || len(block.TxList) == 0 {
		return stateBefore, false, nil
	}
	next := make(map[string]string, len(stateBefore))
	for key, value := range stateBefore {
		next[key] = value
	}
	producer := map[string]int{}
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			continue
		}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.Key != "" && dep.ProducedVersion > 0 {
				producer[txalloReplicaTokenV229(dep.Key, dep.ProducedVersion)] = index
			}
		}
	}
	projected := false
	baseRequired := map[string]uint64{}
	for index, item := range block.TxList {
		if item.ExecutionRouting == nil || item.ExecutionRouting.ExecutionShard != r.node.ShardID || txalloReplicaTargetCommitV229(item, r.node.ShardID) {
			continue
		}
		for _, dep := range item.ExecutionRouting.StateVersions {
			if dep.Key == "" || dep.RequiredVersion == 0 {
				continue
			}
			if owner, ok := producer[txalloReplicaTokenV229(dep.Key, dep.RequiredVersion)]; ok && owner < index {
				continue
			}
			if previous, exists := baseRequired[dep.Key]; exists && previous != dep.RequiredVersion {
				return nil, false, fmt.Errorf("TxAllo replica block requires multiple external exact versions key=%s first=%d next=%d", dep.Key, previous, dep.RequiredVersion)
			}
			baseRequired[dep.Key] = dep.RequiredVersion
			materialized := r.txalloReplicaMaterializedVersionV229(dep.Key)
			if materialized < dep.RequiredVersion || !r.txalloReplicaAppliedV229(dep.Key, dep.RequiredVersion) {
				return nil, false, fmt.Errorf("TxAllo replica predecessor not durable at execution key=%s required=%d materialized=%d", dep.Key, dep.RequiredVersion, materialized)
			}
			if materialized == dep.RequiredVersion {
				continue
			}
			if dep.ProducedVersion != 0 {
				return nil, false, fmt.Errorf("TxAllo exact writer predecessor superseded before execution key=%s required=%d materialized=%d", dep.Key, dep.RequiredVersion, materialized)
			}
			value, ok := r.txalloReplicaExactValueV229(dep.Key, dep.RequiredVersion)
			if !ok {
				return nil, false, fmt.Errorf("TxAllo replica exact historical value unavailable key=%s version=%d", dep.Key, dep.RequiredVersion)
			}
			next[r.node.ShardID+"::"+dep.Key] = value
			projected = true
		}
	}
	if projected {
		r.addRuntimeMetric("txallo_replica_exact_local_projection_count", 1)
	}
	return next, projected, nil
}

func txalloExpectedReplicaTokensV229(records []WorkloadRecord) []string {
	set := map[string]bool{}
	for _, record := range records {
		ordinal := uint64(record.Index + 1)
		for _, access := range normalizedAccessItems(record) {
			if strings.TrimSpace(access.Key) == "" || !txalloAccessWriteV229(access) {
				continue
			}
			set[txalloReplicaTokenV229(access.Key, ordinal)] = true
		}
	}
	out := make([]string, 0, len(set))
	for token := range set {
		out = append(out, token)
	}
	sort.Strings(out)
	return out
}

type txalloReplicaFeedReaderV229 struct {
	offsets map[string]int64
	partial map[string][]byte
	rows    map[string]map[string]txalloReplicaCommitRowV229
}

func newTxAlloReplicaFeedReaderV229() *txalloReplicaFeedReaderV229 {
	return &txalloReplicaFeedReaderV229{offsets: map[string]int64{}, partial: map[string][]byte{}, rows: map[string]map[string]txalloReplicaCommitRowV229{}}
}

func (reader *txalloReplicaFeedReaderV229) poll(nodes []NodePlan) error {
	for _, node := range nodes {
		path := filepath.Join(node.DataDir, txalloReplicaCommitFeedName)
		f, err := txalloOpenFileStable(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return err
		}
		offset := reader.offsets[path]
		if info.Size() < offset {
			offset = 0
			reader.partial[path] = nil
			reader.rows[node.NodeID] = map[string]txalloReplicaCommitRowV229{}
		}
		if _, err := f.Seek(offset, io.SeekStart); err != nil {
			_ = f.Close()
			return err
		}
		chunk, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return err
		}
		reader.offsets[path] = offset + int64(len(chunk))
		buf := append(append([]byte(nil), reader.partial[path]...), chunk...)
		last := -1
		for i := len(buf) - 1; i >= 0; i-- {
			if buf[i] == '\n' {
				last = i
				break
			}
		}
		if last < 0 {
			reader.partial[path] = buf
			continue
		}
		reader.partial[path] = append([]byte(nil), buf[last+1:]...)
		for _, line := range strings.Split(string(buf[:last]), "\n") {
			if strings.TrimSpace(line) == "" {
				continue
			}
			var row txalloReplicaCommitRowV229
			if err := json.Unmarshal([]byte(line), &row); err != nil {
				return fmt.Errorf("TxAllo replica ACK feed decode node=%s: %w", node.NodeID, err)
			}
			if row.SchemaVersion != txalloReplicaCommitFeedSchema || row.Token == "" {
				return fmt.Errorf("TxAllo replica ACK feed invalid node=%s", node.NodeID)
			}
			if reader.rows[node.NodeID] == nil {
				reader.rows[node.NodeID] = map[string]txalloReplicaCommitRowV229{}
			}
			reader.rows[node.NodeID][row.Token] = row
		}
	}
	return nil
}

func txalloWaitReplicaConvergenceV229(ctx context.Context, nodes []NodePlan, expected []string, reader *txalloReplicaFeedReaderV229) error {
	if reader == nil {
		return fmt.Errorf("TxAllo replica convergence reader is nil")
	}
	if len(nodes) == 0 {
		return fmt.Errorf("TxAllo replica convergence has no nodes")
	}
	for {
		if err := reader.poll(nodes); err != nil {
			return err
		}
		complete := true
		for _, token := range expected {
			var reference *txalloReplicaCommitRowV229
			for _, node := range nodes {
				row, ok := reader.rows[node.NodeID][token]
				if !ok {
					complete = false
					break
				}
				if reference == nil {
					copyRow := row
					reference = &copyRow
					continue
				}
				if row.Key != reference.Key || row.RoutingOrdinal != reference.RoutingOrdinal || row.SourceTxID != reference.SourceTxID || row.SourceShard != reference.SourceShard || row.Commutative != reference.Commutative {
					return fmt.Errorf("TxAllo replica convergence identity mismatch token=%s", token)
				}
				if !row.Commutative && row.ValueDigest != reference.ValueDigest {
					return fmt.Errorf("TxAllo replica convergence exact-value mismatch token=%s", token)
				}
			}
			if !complete {
				break
			}
		}
		if complete {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("TxAllo replica convergence barrier: %w", ctx.Err())
		case <-time.After(txalloReplicaPollInterval):
		}
	}
}
