package v5

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/storage"
	"metaverse-chainlab/executor/realism/tx"
)

const (
	optmeStatefulRoutingID          = "optme_global_routing"
	optmePartitionStateStorageID    = "optme_partition_state_store"
	optmeGlobalCrossShardID         = "optme_global_no_relay"
	optmeGlobalOrderingDomainID     = "optme-global"
	optmeProjectionVersion          = "optme_block_start_projection_v22"
	optmePartitionMaterializeOrigin = "optme_v22_global_block_materialization"
	optmeProjectionAccessPrefix     = "optme_v22_block_start:"

	// CrossShardFinalityOptMEGlobalCommit is intentionally method-specific.
	// OptME transactions are globally ordered by one unchanged PBFT domain; the
	// frontend topology.shards value denotes logical state partitions, not
	// independent transaction-ordering chains.
	CrossShardFinalityOptMEGlobalCommit = "optme_global_ordering_durable_commit"
)

// optmeGlobalRouting preserves deterministic hash placement only as logical
// workload/state-home identity. client.go maps every transaction to the single
// optme-global PBFT ordering domain compiled for the OptME method family.
type optmeGlobalRouting struct{ basicPlugin }

func (p optmeGlobalRouting) Route(input RoutingInput) RoutingDecision {
	d := hashRouting{p.basicPlugin}.Route(input)
	d.Reason = "optme_global_ordering_logical_route"
	return d
}
func (p optmeGlobalRouting) CrossShardFinalityMode() string {
	return CrossShardFinalityOptMEGlobalCommit
}

// Stateless-OptME uses the same global ordering finality as Stateful OptME.
// It still implements RoutingRuntimeCapabilities for signed projection metadata,
// but v22 explicitly opts out of transaction-level version admission.
func (p statelessOptmeRouting) CrossShardFinalityMode() string {
	return CrossShardFinalityOptMEGlobalCommit
}

// optmePartitionStateStore separates the single PBFT ordering identity from the
// logical persistent Home partition identity. The underlying storage/WAL format
// is unchanged; only the namespace source changes from consensus domain to
// effective execution shard.
type optmePartitionStateStore struct{ builtinStateStorage }

func (p optmePartitionStateStore) UseExecutionShardStorageIdentity() bool { return true }
func (p optmePartitionStateStore) Durable() bool                          { return p.builtinStateStorage.Durable() }
func (p optmePartitionStateStore) Open(input StateStorageInput) (*state.DB, *storage.BlockStore, error) {
	return p.builtinStateStorage.Open(input)
}

// optmeGlobalCrossShard disables the legacy SourceLock/Relay/Finalize lifecycle
// for the paper baseline. Cross-partition accesses are state projection inside
// one globally ordered block, not transactions hopping between independent
// ordering domains.
type optmeGlobalCrossShard struct{ basicPlugin }

func (p optmeGlobalCrossShard) IsCrossShard(tx.SignedTransaction) bool { return false }
func (p optmeGlobalCrossShard) SourceLock(input CrossShardRelayInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.Tx.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "OptMEGlobalOrder", Success: true}
}
func (p optmeGlobalCrossShard) TargetCommit(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "OptMEGlobalCommit", Success: true}
}
func (p optmeGlobalCrossShard) HandleFinalize(input CrossShardFinalizeInput) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "OptMEGlobalCommit", Success: true}
}
func (p optmeGlobalCrossShard) TimeoutRefund(input CrossShardFinalizeInput, reason string) CrossShardEvent {
	return CrossShardEvent{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard, Stage: "OptMEGlobalExecutionFailure", Success: false, Error: reason}
}
func (p optmeGlobalCrossShard) BuildRelay(input CrossShardRelayInput) Relay {
	return Relay{Tx: input.Tx, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}
func (p optmeGlobalCrossShard) BuildFinalize(input CrossShardFinalizeInput) Finalize {
	return Finalize{TxID: input.TxID, LogicalTxID: input.LogicalTxID, SourceShard: input.SourceShard, TargetShard: input.TargetShard}
}

func registerOptMEV22Plugins(register func(string, string, Factory)) {
	register("routing", optmeStatefulRoutingID, func(c map[string]any) (Plugin, error) {
		return optmeGlobalRouting{makeBasic("routing", optmeStatefulRoutingID, c)}, nil
	})
	register("state_storage", optmePartitionStateStorageID, func(c map[string]any) (Plugin, error) {
		return optmePartitionStateStore{builtinStateStorage{makeBasic("state_storage", optmePartitionStateStorageID, c)}}, nil
	})
	register("cross_shard", optmeGlobalCrossShardID, func(c map[string]any) (Plugin, error) {
		return optmeGlobalCrossShard{makeBasic("cross_shard", optmeGlobalCrossShardID, c)}, nil
	})
}

func optmeGlobalOrderingEnabled(plugins RuntimePlugins) bool {
	if plugins.BlockExecutor == nil {
		return false
	}
	switch plugins.BlockExecutor.ID() {
	case optmeStatefulExecutorID, optmeStatelessExecutorID:
		return true
	default:
		return false
	}
}

func (r *NodeRuntime) optmeStatelessProjectionEnabled() bool {
	return r != nil && r.plugins.Routing != nil && r.plugins.Routing.ID() == optmeStatelessRoutingID &&
		r.plugins.BlockExecutor != nil && r.plugins.BlockExecutor.ID() == optmeStatelessExecutorID &&
		r.plugins.StateStorage != nil && r.plugins.StateStorage.ID() == optmePartitionStateStorageID
}

func optmeWithoutStateVersions(item tx.SignedTransaction) tx.SignedTransaction {
	copyItem := item
	if item.ExecutionRouting == nil {
		return copyItem
	}
	routing := *item.ExecutionRouting
	routing.StateVersions = nil
	copyItem.ExecutionRouting = &routing
	return copyItem
}

// validateOptMEV22NoTransactionStateVersions makes the v22 boundary fail-closed.
// A mixed-version client must never be allowed to reintroduce the superseded
// workload/source-order RequiredVersion/ProducedVersion chain into OptME.
func validateOptMEV22NoTransactionStateVersions(block realblock.Block) error {
	for _, item := range block.TxList {
		if item.ExecutionRouting != nil && len(item.ExecutionRouting.StateVersions) > 0 {
			return fmt.Errorf("optme v22 forbids transaction-level StateVersions: tx=%s count=%d", item.TxID, len(item.ExecutionRouting.StateVersions))
		}
	}
	return nil
}

// primeOptMEBlockStartProjectionSnapshot freezes this node's owned partition at
// the exact pre-execution state of block H. stateFetchSnapshot() keys include
// requester partition, so prime every logical requester. This closes the race in
// which a late remote fetch could otherwise observe H after the Home replica had
// already materialized H locally.
func (r *NodeRuntime) primeOptMEBlockStartProjectionSnapshot(block realblock.Block, stateBefore map[string]string) {
	if !r.optmeStatelessProjectionEnabled() || block.BlockHash == "" {
		return
	}
	local := r.stateAccessPartitionID()
	if local == "" {
		return
	}
	snapshot := copyStringMap(stateBefore)
	root := state.RootOfSnapshot(snapshot)
	requesters := r.stateAccessShardIDs()
	if len(requesters) == 0 {
		requesters = []string{local}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stateFetchSnapshots == nil {
		r.stateFetchSnapshots = map[string]map[string]string{}
	}
	if r.stateFetchSnapshotRoots == nil {
		r.stateFetchSnapshotRoots = map[string]string{}
	}
	for _, requester := range requesters {
		cacheKey := stateFetchSnapshotKey(StateFetchRequest{BlockHash: block.BlockHash, HomeShard: local, ExecutionShard: requester})
		if existing := r.stateFetchSnapshots[cacheKey]; existing != nil {
			continue
		}
		r.stateFetchSnapshots[cacheKey] = snapshot
		r.stateFetchSnapshotRoots[cacheKey] = root
		r.stateFetchSnapshotOrder = append(r.stateFetchSnapshotOrder, cacheKey)
	}
	for len(r.stateFetchSnapshotOrder) > stateFetchSnapshotCacheLimit {
		oldest := r.stateFetchSnapshotOrder[0]
		r.stateFetchSnapshotOrder = r.stateFetchSnapshotOrder[1:]
		delete(r.stateFetchSnapshots, oldest)
		delete(r.stateFetchSnapshotRoots, oldest)
	}
}

type optmeProjectionTask struct {
	key       string
	item      tx.SignedTransaction
	access    tx.AccessItem
	homeShard string
}

type optmeProjectionFetchResult struct {
	task     optmeProjectionTask
	response StateFetchResponse
	latency  time.Duration
	err      error
}

func optmeProjectionAccessKind(predecessorHeight uint64) string {
	return optmeProjectionAccessPrefix + strconv.FormatUint(predecessorHeight, 10)
}

func optmeProjectionRequiredHeight(accessKind string) (uint64, bool) {
	if !strings.HasPrefix(accessKind, optmeProjectionAccessPrefix) {
		return 0, false
	}
	raw := strings.TrimPrefix(accessKind, optmeProjectionAccessPrefix)
	value, err := strconv.ParseUint(raw, 10, 64)
	return value, err == nil
}

// handleOptMEProjectionFetchRequest serves only a block-start snapshot whose
// state height is exactly H-1 for block H. A lagging Home answers not-ready;
// a Home that has advanced beyond H-1 may serve only the snapshot that was
// frozen under the requesting block hash before H materialization began.
func (r *NodeRuntime) handleOptMEProjectionFetchRequest(ctx context.Context, requester string, request StateFetchRequest) (bool, error) {
	requiredHeight, ok := optmeProjectionRequiredHeight(request.AccessKind)
	if !ok {
		return false, nil
	}
	qualifiedKey := request.HomeShard + "::" + request.Key
	respond := func(success bool, value, root, reason string) error {
		response := StateFetchResponse{
			RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash,
			Key: request.Key, QualifiedKey: qualifiedKey, Value: value,
			HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard,
			StateRoot: root, StateVersion: requiredHeight, Versioned: false,
			Success: success, Error: reason,
		}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		return r.enqueueStateFetchResponse(ctx, requester, response)
	}
	if request.HomeShard == "" || request.HomeShard != r.stateAccessPartitionID() {
		return true, respond(false, "", "", "optme_projection_wrong_home")
	}
	cacheKey := stateFetchSnapshotKey(request)
	r.mu.Lock()
	committedHeight := r.committedHeight
	cached := r.stateFetchSnapshots[cacheKey]
	cachedRoot := r.stateFetchSnapshotRoots[cacheKey]
	committing := r.committing != nil && r.committing[request.BlockHash]
	r.mu.Unlock()
	if cached != nil && cachedRoot != "" {
		return true, respond(true, cached[qualifiedKey], cachedRoot, "")
	}
	if committedHeight > requiredHeight {
		// The Home has already crossed H-1. Without the frozen snapshot this
		// historical boundary is irrecoverable from the live DB.
		return true, respond(false, "", "", "optme_projection_snapshot_unavailable")
	}
	// Never sample the live DB on demand. Even when committedHeight == H-1, a
	// concurrent H commit could begin immediately after this check. The H commit
	// path must first capture stateBefore and prime the immutable snapshot cache;
	// until that cache appears the requester retries causally. `committing` is
	// intentionally observed only for diagnostics here: there is a small safe
	// window after committing=true and before prime() stores the snapshot.
	_ = committing
	return true, respond(false, "", "", "optme_projection_home_not_ready")
}

// fetchOptMEProjectionState retries only causal Home-readiness states. It has
// no transaction-version semantics: RequiredVersion carries the predecessor
// block height solely as a snapshot-height guard while Versioned stays false.
func (r *NodeRuntime) fetchOptMEProjectionState(ctx context.Context, block realblock.Block, task optmeProjectionTask) (StateFetchResponse, time.Duration, error) {
	targetNode := r.stateAccessLeaderID(task.homeShard)
	if targetNode == "" {
		return StateFetchResponse{}, 0, fmt.Errorf("optme v22 projection Home leader missing for %s", task.homeShard)
	}
	predecessorHeight := uint64(0)
	if block.Height > 0 {
		predecessorHeight = block.Height - 1
	}
	requestID := stableTextDigest(strings.Join([]string{"optme-v22-projection", r.node.NodeID, block.BlockHash, task.key, task.homeShard, r.stateAccessPartitionID()}, "|"))
	waiter := make(chan StateFetchResponse, 1)
	r.mu.Lock()
	if r.stateFetchWaiters == nil {
		r.stateFetchWaiters = map[string]chan StateFetchResponse{}
	}
	r.stateFetchWaiters[requestID] = waiter
	r.mu.Unlock()
	defer func() { r.mu.Lock(); delete(r.stateFetchWaiters, requestID); r.mu.Unlock() }()
	request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{
		RequestID: requestID, TxID: task.item.TxID, BlockHash: block.BlockHash,
		Key: task.key, HomeShard: task.homeShard, ExecutionShard: r.stateAccessPartitionID(),
		AccessKind: optmeProjectionAccessKind(predecessorHeight), RequiredVersion: predecessorHeight, Versioned: false,
	})
	request.AccessKind = optmeProjectionAccessKind(predecessorHeight)
	request.RequiredVersion = predecessorHeight
	request.Versioned = false
	started := time.Now()
	for {
		envelope, err := p2p.NewEnvelope(stateFetchRequestMessage, r.node.NodeID, targetNode, r.node.ShardID, block.Height, r.currentPBFTView(), block.Height, request)
		if err != nil {
			return StateFetchResponse{}, time.Since(started), err
		}
		if err := r.sendStateAccessToNode(ctx, targetNode, envelope); err != nil {
			return StateFetchResponse{}, time.Since(started), err
		}
		select {
		case response := <-waiter:
			if response.Success {
				return response, time.Since(started), nil
			}
			if response.Error == "optme_projection_home_not_ready" {
				select {
				case <-ctx.Done():
					return StateFetchResponse{}, time.Since(started), ctx.Err()
				case <-time.After(5 * time.Millisecond):
					continue
				}
			}
			// snapshot_unavailable is permanent for this block: the Home has
			// already advanced past H-1 without a frozen H snapshot. Retrying
			// cannot recreate historical state, so fail closed immediately.
			return response, time.Since(started), fmt.Errorf("optme v22 projection fetch failed: %s", response.Error)
		case <-ctx.Done():
			return StateFetchResponse{}, time.Since(started), ctx.Err()
		}
	}
}

type optmeStatelessProjectionResult struct {
	Enabled          bool
	Version          string
	Digest           string
	UniqueKeyCount   int
	RemoteFetchCount int
	HomeRoots        map[string]string
	BuildMS          int64
}

type optmeProjectionDigestRow struct {
	Key         string `json:"key"`
	HomeShard   string `json:"home_shard"`
	StateRoot   string `json:"state_root"`
	ValueDigest string `json:"value_digest"`
}

func optmeProjectionTasks(block realblock.Block, shards []string, home func(string, []string) string) ([]optmeProjectionTask, error) {
	byKey := map[string]optmeProjectionTask{}
	for _, item := range block.TxList {
		clean := optmeWithoutStateVersions(item)
		for _, access := range item.AccessList {
			key := strings.TrimSpace(access.Key)
			if key == "" || strings.Contains(key, "::") {
				continue
			}
			if _, exists := byKey[key]; exists {
				continue
			}
			homeShard := home(key, shards)
			if homeShard == "" {
				return nil, fmt.Errorf("optme v22 projection has no Home partition for key %s", key)
			}
			byKey[key] = optmeProjectionTask{key: key, item: clean, access: access, homeShard: homeShard}
		}
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]optmeProjectionTask, 0, len(keys))
	for _, key := range keys {
		out = append(out, byKey[key])
	}
	return out, nil
}

// prepareOptMEStatelessProjection constructs one immutable block-start view.
// It never uses transaction source ordinals or RequiredVersion/ProducedVersion.
// Every key is fetched at most once per block; all keys from the same Home must
// report one identical snapshot root or the block fails closed.
func (r *NodeRuntime) prepareOptMEStatelessProjection(ctx context.Context, block realblock.Block, stateBefore map[string]string) (map[string]string, optmeStatelessProjectionResult, error) {
	result := optmeStatelessProjectionResult{Enabled: true, Version: optmeProjectionVersion, HomeRoots: map[string]string{}}
	if !r.optmeStatelessProjectionEnabled() {
		return stateBefore, optmeStatelessProjectionResult{}, nil
	}
	started := time.Now()
	if err := validateOptMEV22NoTransactionStateVersions(block); err != nil {
		return nil, result, err
	}
	shards := r.stateAccessShardIDs()
	if len(shards) == 0 {
		return nil, result, fmt.Errorf("optme v22 stateless projection has no logical state partitions")
	}
	tasks, err := optmeProjectionTasks(block, shards, r.stateHomeShardForKey)
	if err != nil {
		return nil, result, err
	}
	result.UniqueKeyCount = len(tasks)
	projection := map[string]string{}
	rows := make([]optmeProjectionDigestRow, 0, len(tasks))
	local := r.stateAccessPartitionID()
	localRoot := state.RootOfSnapshot(stateBefore)
	for _, task := range tasks {
		if task.homeShard != local {
			continue
		}
		value := stateBefore[qualifyStateKey(local, task.key)]
		projection[qualifyStateKey(block.ShardID, task.key)] = value
		if existing := result.HomeRoots[local]; existing != "" && existing != localRoot {
			return nil, result, fmt.Errorf("optme v22 local Home root changed inside block projection: home=%s", local)
		}
		result.HomeRoots[local] = localRoot
		rows = append(rows, optmeProjectionDigestRow{Key: task.key, HomeShard: local, StateRoot: localRoot, ValueDigest: stateValueDigest(value)})
	}

	remoteTasks := make([]optmeProjectionTask, 0, len(tasks))
	for _, task := range tasks {
		if task.homeShard != local {
			remoteTasks = append(remoteTasks, task)
		}
	}
	if len(remoteTasks) > 0 {
		workerCount := blockExecutorWorkerCountFromProfile(r.pluginSnapshot)
		if workerCount < 1 {
			workerCount = 1
		}
		if workerCount > len(remoteTasks) {
			workerCount = len(remoteTasks)
		}
		jobs := make(chan optmeProjectionTask, len(remoteTasks))
		results := make(chan optmeProjectionFetchResult, len(remoteTasks))
		var wg sync.WaitGroup
		for worker := 0; worker < workerCount; worker++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for task := range jobs {
					response, latency, fetchErr := r.fetchOptMEProjectionState(ctx, block, task)
					results <- optmeProjectionFetchResult{task: task, response: response, latency: latency, err: fetchErr}
				}
			}()
		}
		for _, task := range remoteTasks {
			jobs <- task
		}
		close(jobs)
		wg.Wait()
		close(results)
		collected := make([]optmeProjectionFetchResult, 0, len(remoteTasks))
		for fetch := range results {
			if fetch.err != nil {
				return nil, result, fetch.err
			}
			collected = append(collected, fetch)
		}
		sort.Slice(collected, func(i, j int) bool { return collected[i].task.key < collected[j].task.key })
		for _, fetch := range collected {
			if !fetch.response.Success {
				return nil, result, fmt.Errorf("optme v22 projection fetch failed: key=%s home=%s error=%s", fetch.task.key, fetch.task.homeShard, fetch.response.Error)
			}
			if fetch.response.StateRoot == "" {
				return nil, result, fmt.Errorf("optme v22 projection fetch missing Home root: key=%s home=%s", fetch.task.key, fetch.task.homeShard)
			}
			if existing := result.HomeRoots[fetch.task.homeShard]; existing != "" && existing != fetch.response.StateRoot {
				r.addRuntimeMetric("optme_projection_home_root_mismatch_count", 1)
				return nil, result, fmt.Errorf("optme v22 projection Home root mismatch: home=%s first=%s got=%s", fetch.task.homeShard, existing, fetch.response.StateRoot)
			}
			result.HomeRoots[fetch.task.homeShard] = fetch.response.StateRoot
			projection[qualifyStateKey(block.ShardID, fetch.task.key)] = fetch.response.Value
			rows = append(rows, optmeProjectionDigestRow{Key: fetch.task.key, HomeShard: fetch.task.homeShard, StateRoot: fetch.response.StateRoot, ValueDigest: stateValueDigest(fetch.response.Value)})
			r.recordRemoteStateAccess(block, fetch.task.item, fetch.task.access, fetch.response, fetch.latency)
			result.RemoteFetchCount++
		}
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Key != rows[j].Key {
			return rows[i].Key < rows[j].Key
		}
		return rows[i].HomeShard < rows[j].HomeShard
	})
	result.Digest = stableJSONDigest(rows)
	result.BuildMS = time.Since(started).Milliseconds()
	r.addRuntimeMetric("optme_projection_block_count", 1)
	r.addRuntimeMetric("optme_projection_unique_key_count", int64(result.UniqueKeyCount))
	r.addRuntimeMetric("optme_projection_remote_fetch_count", int64(result.RemoteFetchCount))
	r.addRuntimeMetric("optme_projection_home_root_count", int64(len(result.HomeRoots)))
	r.addRuntimeMetric("optme_projection_build_ms", result.BuildMS)
	return projection, result, nil
}

// optmePartitionOwnedStateDelta maps the globally executed OptME final delta
// back to the persistent Home partition owned by this validator. There is no
// second remote writeback protocol: every PBFT validator already possesses the
// same final logical delta and each storage role materializes only its own keys.
func (r *NodeRuntime) optmePartitionOwnedStateDelta(block realblock.Block, items []state.StateKV) ([]state.StateKV, error) {
	if !r.optmeStatelessProjectionEnabled() {
		return items, nil
	}
	local := r.stateAccessPartitionID()
	shards := r.stateAccessShardIDs()
	out := make([]state.StateKV, 0, len(items))
	for _, item := range items {
		logicalKey := item.Key
		if unqualified, ok := unqualifiedLocalKey(item.Key, block.ShardID); ok {
			logicalKey = unqualified
		} else if index := strings.Index(logicalKey, "::"); index >= 0 && index+2 < len(logicalKey) {
			logicalKey = logicalKey[index+2:]
		}
		if logicalKey == "" {
			continue
		}
		home := r.stateHomeShardForKey(logicalKey, shards)
		if home == "" {
			return nil, fmt.Errorf("optme v22 materialization has no Home partition for key %s", logicalKey)
		}
		if home != local {
			continue
		}
		next := item
		next.Key = qualifyStateKey(local, logicalKey)
		next.ApplyOrigin = optmePartitionMaterializeOrigin
		next.BlockHeight = block.Height
		next.RoutingOrdinal = 0
		next.PreviousVersion = 0
		next.ProducedVersion = 0
		next.OrderingNoop = false
		next.BaseValue = ""
		next.BaseValueDigest = ""
		out = append(out, next)
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return strings.Join(out[i].TxIDs, "|") < strings.Join(out[j].TxIDs, "|")
	})
	r.addRuntimeMetric("optme_partition_materialized_key_count", int64(len(out)))
	return out, nil
}

var _ RoutingPlugin = optmeGlobalRouting{}
var _ CrossShardFinalityCapability = optmeGlobalRouting{}
var _ CrossShardFinalityCapability = statelessOptmeRouting{}
var _ StateStoragePlugin = optmePartitionStateStore{}
var _ ExecutionShardStorageIdentityCapability = optmePartitionStateStore{}
var _ CrossShardPlugin = optmeGlobalCrossShard{}
