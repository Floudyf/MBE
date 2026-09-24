package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const calvinStatelessSameBlockOrigin = "calvin_stateless_same_block"

type CalvinStatelessFetchInput struct {
	Block     realblock.Block
	Item      tx.SignedTransaction
	Access    tx.AccessItem
	HomeShard string
}
type CalvinStatelessFetchResult struct {
	Value         string
	Remote        bool
	BlockStart    bool
	Exact         bool
	WaitMS        int64
	PhysicalFetch int
}
type CalvinStatelessFetchFunc func(context.Context, CalvinStatelessFetchInput) (CalvinStatelessFetchResult, error)

type CalvinStatelessWritebackInput struct {
	Block                 realblock.Block
	Item                  tx.SignedTransaction
	Delta                 execution.TxDelta
	Key, Value, HomeShard string
	OrderingNoop          bool
}
type CalvinStatelessWritebackResult struct {
	Remote                        bool
	WaitMS                        int64
	LogicalWrites, PhysicalWrites int
}
type CalvinStatelessWritebackFunc func(context.Context, CalvinStatelessWritebackInput) (CalvinStatelessWritebackResult, error)

type CalvinStatelessInboundWrite struct {
	Key, Value     string
	RoutingOrdinal uint64
	TxID           string
	OrderingNoop   bool
}
type CalvinStatelessCollectWritebacksFunc func(context.Context, realblock.Block, string) ([]CalvinStatelessInboundWrite, error)

func calvinStatelessExecutionHome(item tx.SignedTransaction, shards []string) (string, error) {
	if item.ExecutionRouting == nil {
		return "", fmt.Errorf("calvin_stateless_execution_routing_missing: tx=%s", item.TxID)
	}
	home := strings.TrimSpace(item.ExecutionRouting.ExecutionShard)
	if home == "" || !containsString(shards, home) {
		return "", fmt.Errorf("calvin_stateless_execution_home_invalid: tx=%s home=%s", item.TxID, home)
	}
	return home, nil
}

func calvinBindConsensusVersions(item tx.SignedTransaction, dependencies []tx.StateVersionDependency) (tx.SignedTransaction, error) {
	if item.ExecutionRouting == nil {
		return tx.SignedTransaction{}, fmt.Errorf("calvin_stateless_execution_routing_missing: tx=%s", item.TxID)
	}
	copyItem := item
	routing := *item.ExecutionRouting
	routing.StateVersions = append([]tx.StateVersionDependency(nil), dependencies...)
	copyItem.ExecutionRouting = &routing
	return copyItem, nil
}

func calvinWithoutConsensusVersions(item tx.SignedTransaction) tx.SignedTransaction {
	copyItem := item
	if item.ExecutionRouting == nil {
		return copyItem
	}
	routing := *item.ExecutionRouting
	routing.StateVersions = nil
	copyItem.ExecutionRouting = &routing
	return copyItem
}

// calvinStateVersionDependenciesForAccessList is retained only as a compatibility
// utility for pre-v34 regression tests and offline diagnostics. Production
// Stateless Calvin never calls it: consensus-bound version dependencies are
// built by buildCalvinConsensusPlan from the final candidate block order.
func calvinStateVersionDependenciesForAccessList(items []tx.AccessItem, ordinal uint64, lastWriter map[string]uint64) []tx.StateVersionDependency {
	type summary struct{ write bool }
	byKey := map[string]summary{}
	for _, access := range items {
		key := strings.TrimSpace(access.Key)
		if key == "" {
			continue
		}
		row := byKey[key]
		row.write = row.write || calvinIsWriteMode(access.Mode)
		byKey[key] = row
	}
	keys := make([]string, 0, len(byKey))
	for key := range byKey {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	out := make([]tx.StateVersionDependency, 0, len(keys))
	for _, key := range keys {
		dep := tx.StateVersionDependency{Key: key, RequiredVersion: lastWriter[key]}
		if byKey[key].write {
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

func (r *NodeRuntime) stateAccessPartitionID() string {
	if r != nil && r.plugins.StateStorage != nil {
		if c, ok := r.plugins.StateStorage.(ExecutionShardStorageIdentityCapability); ok && c.UseExecutionShardStorageIdentity() {
			return effectiveExecutionShardID(r.node)
		}
	}
	return r.node.ShardID
}
func (r *NodeRuntime) stateAccessShardIDs() []string {
	if r != nil && r.plugins.StateStorage != nil {
		if c, ok := r.plugins.StateStorage.(ExecutionShardStorageIdentityCapability); ok && c.UseExecutionShardStorageIdentity() {
			return calvinExecutionShardIDsFromPlan(r.plan)
		}
	}
	return r.shardIDs()
}
func (r *NodeRuntime) stateAccessLeaderID(shardID string) string {
	if r != nil && r.plugins.StateStorage != nil {
		if c, ok := r.plugins.StateStorage.(ExecutionShardStorageIdentityCapability); ok && c.UseExecutionShardStorageIdentity() {
			return r.calvinExecutionShardLeader(shardID)
		}
	}
	return r.leaderID(shardID)
}
func (r *NodeRuntime) stateAccessNodeIDsForShard(shardID string) []string {
	if r != nil && r.plugins.StateStorage != nil {
		if c, ok := r.plugins.StateStorage.(ExecutionShardStorageIdentityCapability); ok && c.UseExecutionShardStorageIdentity() {
			out := []string{}
			for _, node := range r.plan.NodeConfigs {
				if effectiveExecutionShardID(node) == shardID && node.NodeID != "" {
					out = append(out, node.NodeID)
				}
			}
			sort.Strings(out)
			return out
		}
	}
	return r.nodeIDsForShard(shardID)
}

func (r *NodeRuntime) calvinStatelessFetchState(ctx context.Context, input CalvinStatelessFetchInput) (CalvinStatelessFetchResult, error) {
	dep, ok := stateVersionDependencyForKey(input.Item, input.Access.Key)
	if !ok {
		return CalvinStatelessFetchResult{}, fmt.Errorf("calvin_stateless_version_dependency_missing: tx=%s key=%s", input.Item.TxID, input.Access.Key)
	}
	started := time.Now()
	local := r.stateAccessPartitionID()
	// RequiredVersion==0 is not the genesis value. It means there is no earlier
	// writer for this key in the consensus-bound Calvin block, so the exact
	// predecessor is the durable value at the block boundary.
	if dep.RequiredVersion == 0 {
		if input.HomeShard == local {
			return CalvinStatelessFetchResult{Value: r.db.Get(input.Access.Key), BlockStart: true, WaitMS: time.Since(started).Milliseconds()}, nil
		}
		currentItem := calvinWithoutConsensusVersions(input.Item)
		response, latency, err := r.fetchRemoteState(ctx, input.Block, currentItem, input.Access, input.HomeShard)
		if err != nil {
			return CalvinStatelessFetchResult{}, err
		}
		r.recordRemoteStateAccess(input.Block, input.Item, input.Access, response, latency)
		return CalvinStatelessFetchResult{Value: response.Value, Remote: true, BlockStart: true, WaitMS: latency.Milliseconds(), PhysicalFetch: 1}, nil
	}
	if input.HomeShard == local {
		value, err := r.waitForLocalStateVersion(ctx, input.Access.Key, dep.RequiredVersion)
		if err != nil {
			return CalvinStatelessFetchResult{}, err
		}
		return CalvinStatelessFetchResult{Value: value, Exact: true, WaitMS: time.Since(started).Milliseconds()}, nil
	}
	response, latency, err := r.fetchRemoteState(ctx, input.Block, input.Item, input.Access, input.HomeShard)
	if err != nil {
		return CalvinStatelessFetchResult{}, err
	}
	r.recordRemoteStateAccess(input.Block, input.Item, input.Access, response, latency)
	return CalvinStatelessFetchResult{Value: response.Value, Remote: true, Exact: true, WaitMS: latency.Milliseconds(), PhysicalFetch: 1}, nil
}

func (r *NodeRuntime) calvinStatelessWriteback(ctx context.Context, input CalvinStatelessWritebackInput) (CalvinStatelessWritebackResult, error) {
	dep, ok := stateVersionDependencyForKey(input.Item, input.Key)
	if !ok || dep.ProducedVersion == 0 {
		return CalvinStatelessWritebackResult{}, fmt.Errorf("calvin_stateless_produced_version_missing: tx=%s key=%s", input.Item.TxID, input.Key)
	}
	local := r.stateAccessPartitionID()
	if input.HomeShard == local {
		r.publishStateVersion(input.Key, dep.ProducedVersion, input.Value)
		return CalvinStatelessWritebackResult{}, nil
	}
	if r.calvinExecutionShardLeader(local) != r.node.NodeID {
		return CalvinStatelessWritebackResult{Remote: true}, nil
	}
	item := state.StateKV{Key: qualifyStateKey(local, input.Key), Value: input.Value, TxIDs: []string{input.Item.TxID}, UpdateSemantics: "set", ApplyOrigin: calvinStatelessSameBlockOrigin, RoutingOrdinal: dep.ProducedVersion, PreviousVersion: dep.RequiredVersion, ProducedVersion: dep.ProducedVersion, OrderingNoop: input.OrderingNoop}
	acks, latency, err := r.applyRemoteStateDelta(ctx, input.Block, item, input.Key, input.HomeShard, []execution.TxDelta{input.Delta})
	if err != nil {
		return CalvinStatelessWritebackResult{}, err
	}
	for _, ack := range acks {
		r.recordRemoteStateApply(input.Block, item, input.Key, ack, latency)
	}
	return CalvinStatelessWritebackResult{Remote: true, WaitMS: latency.Milliseconds(), LogicalWrites: 1, PhysicalWrites: len(acks)}, nil
}

func (r *NodeRuntime) calvinStatelessCollectWritebacks(_ context.Context, block realblock.Block, homeShard string) ([]CalvinStatelessInboundWrite, error) {
	r.mu.Lock()
	rows := make([]StateDeltaApplyRequest, 0)
	for _, request := range r.pendingStateDeltas {
		if request.ApplyOrigin == calvinStatelessSameBlockOrigin && request.BlockHash == block.BlockHash && request.SourceHeight == block.Height && request.HomeShard == homeShard {
			rows = append(rows, request)
		}
	}
	r.mu.Unlock()
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].RoutingOrdinal != rows[j].RoutingOrdinal {
			return rows[i].RoutingOrdinal < rows[j].RoutingOrdinal
		}
		if rows[i].Key != rows[j].Key {
			return rows[i].Key < rows[j].Key
		}
		return rows[i].TxID < rows[j].TxID
	})
	out := make([]CalvinStatelessInboundWrite, 0, len(rows))
	for _, row := range rows {
		out = append(out, CalvinStatelessInboundWrite{Key: row.Key, Value: row.Value, RoutingOrdinal: row.RoutingOrdinal, TxID: row.TxID, OrderingNoop: row.OrderingNoop})
	}
	return out, nil
}

func (r *NodeRuntime) markCalvinSameBlockWritebacksApplied(block realblock.Block) {
	if strings.TrimSpace(block.BlockHash) == "" {
		return
	}
	local := r.stateAccessPartitionID()
	versions := map[string]uint64{}
	dependencies, _, err := calvinConsensusDependenciesByIndex(block)
	if err != nil {
		return
	}
	for _, row := range dependencies {
		for _, dep := range row {
			if dep.ProducedVersion > 0 && dep.Key != "" && r.calvinStateHome(dep.Key) == local && dep.ProducedVersion > versions[dep.Key] {
				versions[dep.Key] = dep.ProducedVersion
			}
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.appliedStateDeltaKeys == nil {
		r.appliedStateDeltaKeys = map[string]bool{}
	}
	if r.stateVersionMaterialized == nil {
		r.stateVersionMaterialized = map[string]uint64{}
	}
	for key, version := range versions {
		if version > r.stateVersionMaterialized[key] {
			r.stateVersionMaterialized[key] = version
		}
	}
	pending := r.pendingStateDeltas[:0]
	for _, request := range r.pendingStateDeltas {
		if request.ApplyOrigin == calvinStatelessSameBlockOrigin && request.BlockHash == block.BlockHash {
			key := stateDeltaApplyKey(request)
			delete(r.pendingStateDeltaKeys, key)
			r.appliedStateDeltaKeys[key] = true
			continue
		}
		pending = append(pending, request)
	}
	r.pendingStateDeltas = pending
}

func (p calvinBlockExecutor) executeStatelessCalvinParticipant(ctx context.Context, input BlockExecutionInput, serial *execution.SerialExecutor, index int, dependencies []tx.StateVersionDependency, workingMu *sync.Mutex, working map[string]string, limiter *calvinExecutionLimiter) calvinWorkerResult {
	item, err := calvinBindConsensusVersions(input.Block.TxList[index], dependencies)
	if err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	localShard := input.ExecutionShardID
	executionHome, err := calvinStatelessExecutionHome(item, input.CalvinExecutionShards)
	if err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	accesses, err := calvinCanonicalAccesses(item.AccessList)
	if err != nil {
		return calvinWorkerResult{index: index, err: err}
	}
	if localShard != executionHome {
		seed := CalvinTxOutcome{BlockHash: input.Block.BlockHash, Height: input.Block.Height, TxID: item.TxID}
		exchanged, e := input.CalvinOutcomeExchange(ctx, CalvinOutcomeExchangeInput{Outcome: seed, OutcomeShard: executionHome, Publish: false, Timeout: calvinTimeoutFromConfig(p.config, "outcome_timeout_ms")})
		if e != nil {
			return calvinWorkerResult{index: index, err: e}
		}
		receipt := calvinReceiptFromOutcome(exchanged.Outcome)
		return calvinWorkerResult{index: index, delta: execution.TxDelta{TxID: item.TxID, OriginalIndex: index, Receipt: receipt, Success: receipt.Success, Error: receipt.Error, WriteSet: map[string]string{}}, receipt: receipt, localWrites: map[string]string{}, outcomeMessages: exchanged.MessageCount, outcomePhysical: exchanged.PhysicalMessageCount, outcomeWaitMS: exchanged.WaitMS}
	}
	txSnapshot := map[string]string{}
	localReadCount, remoteReadCount, remotePhysical := 0, 0, 0
	remoteWait := int64(0)
	blockStartReads, exactReads := 0, 0
	for _, access := range accesses {
		if !calvinNeedsReadValue(access.Mode) {
			continue
		}
		home := input.CalvinStateHome(access.Key)
		fetched, e := input.CalvinStatelessFetch(ctx, CalvinStatelessFetchInput{Block: input.Block, Item: item, Access: access, HomeShard: home})
		if e != nil {
			return calvinWorkerResult{index: index, err: e}
		}
		txSnapshot[qualifyStateKey(localShard, access.Key)] = fetched.Value
		remoteWait += fetched.WaitMS
		if fetched.BlockStart {
			blockStartReads++
		}
		if fetched.Exact {
			exactReads++
		}
		if fetched.Remote {
			remoteReadCount++
			remotePhysical += fetched.PhysicalFetch
		} else {
			localReadCount++
		}
	}
	localBlock := input.Block
	localBlock.ShardID = localShard
	executionItem := calvinExecutionItem(item)
	if e := limiter.enter(ctx); e != nil {
		return calvinWorkerResult{index: index, err: e}
	}
	receipt, delta := serial.ExecuteTransaction(localBlock, executionItem, txSnapshot, index)
	limiter.leave()
	if e := calvinValidateActualAccess(item, delta); e != nil {
		return calvinWorkerResult{index: index, err: e}
	}
	localWrites := map[string]string{}
	logicalWrites, physicalWrites := 0, 0
	writeWait := int64(0)
	for _, access := range accesses {
		if !calvinIsWriteMode(access.Mode) {
			continue
		}
		key := access.Key
		value, wrote := writeSetLogicalValue(delta.WriteSet, key)
		orderingNoop := !delta.Success || !wrote
		if orderingNoop {
			qualified := qualifyStateKey(localShard, key)
			if predecessor, ok := txSnapshot[qualified]; ok {
				value = predecessor
			} else {
				home := input.CalvinStateHome(key)
				fetched, e := input.CalvinStatelessFetch(ctx, CalvinStatelessFetchInput{Block: input.Block, Item: item, Access: access, HomeShard: home})
				if e != nil {
					return calvinWorkerResult{index: index, err: e}
				}
				value = fetched.Value
				remoteWait += fetched.WaitMS
				if fetched.BlockStart {
					blockStartReads++
				}
				if fetched.Exact {
					exactReads++
				}
				if fetched.Remote {
					remoteReadCount++
					remotePhysical += fetched.PhysicalFetch
				} else {
					localReadCount++
				}
			}
		}
		home := input.CalvinStateHome(key)
		wb, e := input.CalvinStatelessWriteback(ctx, CalvinStatelessWritebackInput{Block: input.Block, Item: item, Delta: delta, Key: key, Value: value, HomeShard: home, OrderingNoop: orderingNoop})
		if e != nil {
			return calvinWorkerResult{index: index, err: e}
		}
		logicalWrites += wb.LogicalWrites
		physicalWrites += wb.PhysicalWrites
		writeWait += wb.WaitMS
		if home == localShard && !orderingNoop {
			localWrites[key] = value
		}
	}
	seed := calvinOutcomeFromReceipt(input.Block.BlockHash, input.Block.Height, executionHome, input.NodeID, receipt)
	exchanged, e := input.CalvinOutcomeExchange(ctx, CalvinOutcomeExchangeInput{Outcome: seed, OutcomeShard: executionHome, Publish: true, Timeout: calvinTimeoutFromConfig(p.config, "outcome_timeout_ms")})
	if e != nil {
		return calvinWorkerResult{index: index, err: e}
	}
	canonical := calvinReceiptFromOutcome(exchanged.Outcome)
	localOutcome := calvinOutcomeFromReceipt(input.Block.BlockHash, input.Block.Height, executionHome, exchanged.Outcome.SenderNodeID, receipt)
	if !calvinOutcomesEqual(localOutcome, exchanged.Outcome) {
		return calvinWorkerResult{index: index, err: fmt.Errorf("calvin stateless execution outcome mismatch tx=%s execution_shard=%s", item.TxID, executionHome)}
	}
	filtered := delta
	filtered.WriteSet = localWrites
	filtered.Receipt = canonical
	filtered.Success = canonical.Success
	filtered.Error = canonical.Error
	return calvinWorkerResult{index: index, delta: filtered, receipt: canonical, localWrites: localWrites, localReadCount: localReadCount, remoteReads: remoteReadCount, remoteWaitMS: remoteWait, statelessBlockStartReads: blockStartReads, statelessExactReads: exactReads, statelessFetchCount: remoteReadCount, statelessFetchPhysical: remotePhysical, statelessWriteCount: logicalWrites, statelessWritePhysical: physicalWrites, statelessWriteWaitMS: writeWait, outcomeMessages: exchanged.MessageCount, outcomePhysical: exchanged.PhysicalMessageCount, outcomeWaitMS: exchanged.WaitMS, executed: true}
}

func annotateCalvinStatelessLocalStateDelta(items []state.StateKV, block realblock.Block, localShard string, home func(string) string) ([]state.StateKV, error) {
	dependencies, _, err := calvinConsensusDependenciesByIndex(block)
	if err != nil {
		return nil, err
	}
	latest := map[string]tx.StateVersionDependency{}
	for _, row := range dependencies {
		for _, dep := range row {
			if dep.ProducedVersion == 0 || home(dep.Key) != localShard {
				continue
			}
			if dep.ProducedVersion > latest[dep.Key].ProducedVersion {
				latest[dep.Key] = dep
			}
		}
	}
	out := make([]state.StateKV, 0, len(items))
	for _, item := range items {
		next := item
		logicalKey, ok := unqualifiedLocalKey(item.Key, localShard)
		if !ok || home(logicalKey) != localShard {
			out = append(out, next)
			continue
		}
		dep := latest[logicalKey]
		if dep.ProducedVersion > 0 {
			next.RoutingOrdinal = dep.ProducedVersion
			next.PreviousVersion = dep.RequiredVersion
			next.ProducedVersion = dep.ProducedVersion
			next.BaseValue = ""
			next.BaseValueDigest = ""
		}
		out = append(out, next)
	}
	return out, nil
}

func (r *NodeRuntime) statelessCalvinRemoteStateEnabled() bool {
	return r != nil && r.plugins.BlockExecutor != nil && r.plugins.BlockExecutor.ID() == calvinStatelessExecutorID
}
