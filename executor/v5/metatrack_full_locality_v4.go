package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

const stateFetchBatchRequestMessage = "V5_STATE_FETCH_BATCH_REQUEST"
const stateFetchBatchResponseMessage = "V5_STATE_FETCH_BATCH_RESPONSE"
const stateDeltaApplyBatchMessage = "V5_STATE_DELTA_APPLY_BATCH"
const stateDeltaApplyBatchAckMessage = "V5_STATE_DELTA_APPLY_BATCH_ACK"

type RemoteStateBatchFetchItem struct {
	Token  string
	Item   tx.SignedTransaction
	Access tx.AccessItem
}

type RemoteStateBatchFetchOutcome struct {
	Events       map[string]RemoteStateReadyEvent
	NotReady     map[string]bool
	RequestCount int
	ItemCount    int
}

type RemoteStateBatchFetchFunc func(context.Context, []RemoteStateBatchFetchItem) (RemoteStateBatchFetchOutcome, error)

type StateFetchBatchRequest struct {
	BatchID        string              `json:"batch_id"`
	BlockHash      string              `json:"block_hash"`
	HomeShard      string              `json:"home_shard"`
	ExecutionShard string              `json:"execution_shard"`
	Items          []StateFetchRequest `json:"items"`
}

type StateFetchBatchResponse struct {
	BatchID        string               `json:"batch_id"`
	BlockHash      string               `json:"block_hash"`
	HomeShard      string               `json:"home_shard"`
	ExecutionShard string               `json:"execution_shard"`
	Items          []StateFetchResponse `json:"items"`
}

type StateDeltaApplyBatchRequest struct {
	BatchID        string                   `json:"batch_id"`
	BlockHash      string                   `json:"block_hash"`
	HomeShard      string                   `json:"home_shard"`
	ExecutionShard string                   `json:"execution_shard"`
	Items          []StateDeltaApplyRequest `json:"items"`
}

type StateDeltaApplyBatchAck struct {
	BatchID        string               `json:"batch_id"`
	BlockHash      string               `json:"block_hash"`
	HomeShard      string               `json:"home_shard"`
	ExecutionShard string               `json:"execution_shard"`
	Items          []StateDeltaApplyAck `json:"items"`
}

func (r *NodeRuntime) metaTrackBlockExecutorFlag(name string) bool {
	if r == nil || r.pluginSnapshot == nil {
		return false
	}
	cfg, ok := r.pluginSnapshot["block_executor"]
	if !ok || cfg.Config == nil {
		return false
	}
	return boolFromAny(cfg.Config[name])
}

func (r *NodeRuntime) metaTrackFullLocalityEnabled() bool {
	return r.metaTrackBlockExecutorFlag("batch_entry_state_prefetch") &&
		r.metaTrackBlockExecutorFlag("batch_remote_writeback")
}

func (r *NodeRuntime) incrementFullLocalityMetric(key string, delta int64) {
	r.mu.Lock()
	if r.runtimeMetricCounts == nil {
		r.runtimeMetricCounts = map[string]int64{}
	}
	r.runtimeMetricCounts[key] += delta
	r.mu.Unlock()
}

// metaTrackBatchStateFetcher batches only entry-state probes that are remote.
// Missing exact versions are returned as not-ready immediately; the scheduler
// then falls back to the legacy long-lived exact-version subscription for those
// future versions. This avoids a block-wide future-version barrier.
func (r *NodeRuntime) metaTrackBatchStateFetcher(block realblock.Block) RemoteStateBatchFetchFunc {
	if !r.metaTrackBlockExecutorFlag("batch_entry_state_prefetch") || len(r.shardIDs()) < 2 || !r.hasBatchRoutingControlPlane() {
		return nil
	}
	return func(ctx context.Context, entries []RemoteStateBatchFetchItem) (RemoteStateBatchFetchOutcome, error) {
		outcome := RemoteStateBatchFetchOutcome{Events: map[string]RemoteStateReadyEvent{}, NotReady: map[string]bool{}}
		if len(entries) == 0 {
			return outcome, nil
		}
		shardIDs := r.shardIDs()
		type prepared struct {
			token   string
			item    tx.SignedTransaction
			access  tx.AccessItem
			request StateFetchRequest
			waiter  chan StateFetchResponse
		}
		groups := map[string][]prepared{}
		localOrUnsupported := map[string]bool{}
		seenToken := map[string]bool{}
		for _, entry := range entries {
			if entry.Token == "" || seenToken[entry.Token] || entry.Access.Key == "" {
				continue
			}
			seenToken[entry.Token] = true
			homeShard := r.stateHomeShardForKey(entry.Access.Key, shardIDs)
			if homeShard == "" || homeShard == r.node.ShardID {
				localOrUnsupported[entry.Token] = true
				continue
			}
			dep, hasDep := stateVersionDependencyForKey(entry.Item, entry.Access.Key)
			versioned := hasDep && isVersionedStateAccess(entry.Access)
			required := uint64(0)
			if versioned {
				required = dep.RequiredVersion
			}
			requestID := stableTextDigest(strings.Join([]string{"batch_entry", r.node.NodeID, entry.Item.TxID, block.BlockHash, entry.Access.Key, homeShard, r.stateAccessPartitionID(), fmt.Sprint(required), fmt.Sprint(versioned)}, "|"))
			request := r.plugins.StateAccess.BuildFetchRequest(StateFetchInput{RequestID: requestID, TxID: entry.Item.TxID, BlockHash: block.BlockHash, Key: entry.Access.Key, HomeShard: homeShard, ExecutionShard: r.stateAccessPartitionID(), AccessKind: string(entry.Access.Mode), RequiredVersion: required, Versioned: versioned})
			waiter := make(chan StateFetchResponse, 1)
			r.mu.Lock()
			if r.stateFetchWaiters == nil {
				r.stateFetchWaiters = map[string]chan StateFetchResponse{}
			}
			r.stateFetchWaiters[requestID] = waiter
			r.mu.Unlock()
			groups[homeShard] = append(groups[homeShard], prepared{token: entry.Token, item: entry.Item, access: entry.Access, request: request, waiter: waiter})
		}
		defer func() {
			r.mu.Lock()
			for _, items := range groups {
				for _, item := range items {
					delete(r.stateFetchWaiters, item.request.RequestID)
				}
			}
			r.mu.Unlock()
		}()
		homes := make([]string, 0, len(groups))
		for home := range groups {
			homes = append(homes, home)
		}
		sort.Strings(homes)
		for _, home := range homes {
			items := groups[home]
			sort.SliceStable(items, func(i, j int) bool { return items[i].request.RequestID < items[j].request.RequestID })
			target := r.stateAccessLeaderID(home)
			if target == "" {
				return outcome, fmt.Errorf("batch entry state home leader missing for %s", home)
			}
			requests := make([]StateFetchRequest, 0, len(items))
			for _, item := range items {
				requests = append(requests, item.request)
			}
			batchID := stableTextDigest(strings.Join([]string{r.node.NodeID, block.BlockHash, home, r.stateAccessPartitionID(), fmt.Sprint(len(requests))}, "|"))
			payload := StateFetchBatchRequest{BatchID: batchID, BlockHash: block.BlockHash, HomeShard: home, ExecutionShard: r.stateAccessPartitionID(), Items: requests}
			envelope, err := p2p.NewEnvelope(stateFetchBatchRequestMessage, r.node.NodeID, target, r.node.ShardID, block.Height, 0, block.Height, payload)
			if err != nil {
				return outcome, err
			}
			if err := r.sendStateAccessToNode(ctx, target, envelope); err != nil {
				return outcome, err
			}
			outcome.RequestCount++
			outcome.ItemCount += len(items)
			r.incrementFullLocalityMetric("metatrack_batch_entry_request_count", 1)
			r.incrementFullLocalityMetric("metatrack_batch_entry_item_count", int64(len(items)))
			for _, item := range items {
				started := time.Now()
				select {
				case response := <-item.waiter:
					if response.Success {
						r.recordRemoteStateAccess(block, item.item, item.access, response, time.Since(started))
						outcome.Events[item.token] = RemoteStateReadyEvent{TxID: item.item.TxID, Key: item.access.Key, ReadinessToken: item.token, Value: response.Value, HomeShard: response.HomeShard, StateVersion: response.StateVersion, LatencyMS: time.Since(started).Milliseconds()}
						r.incrementFullLocalityMetric("metatrack_batch_entry_ready_item_count", 1)
					} else if response.Versioned && response.Error == "state_version_not_ready" {
						outcome.NotReady[item.token] = true
						r.incrementFullLocalityMetric("metatrack_batch_entry_not_ready_item_count", 1)
					} else {
						return outcome, fmt.Errorf("batch entry state fetch failed for %s: %s", item.access.Key, response.Error)
					}
				case <-ctx.Done():
					return outcome, ctx.Err()
				case <-time.After(10 * time.Second):
					return outcome, fmt.Errorf("batch entry state fetch timed out for %s from %s", item.access.Key, home)
				}
			}
		}
		_ = localOrUnsupported
		return outcome, nil
	}
}

func (r *NodeRuntime) handleStateFetchBatchRequest(ctx context.Context, requester string, request StateFetchBatchRequest) error {
	responses := make([]StateFetchResponse, 0, len(request.Items))
	for _, item := range request.Items {
		qualifiedKey := item.HomeShard + "::" + item.Key
		if item.HomeShard != "" && item.HomeShard != r.stateAccessPartitionID() {
			response := StateFetchResponse{RequestID: item.RequestID, TxID: item.TxID, BlockHash: item.BlockHash, Key: item.Key, QualifiedKey: qualifiedKey, HomeShard: item.HomeShard, ExecutionShard: item.ExecutionShard, StateVersion: item.RequiredVersion, Versioned: item.Versioned, Success: false, Error: "wrong_home_shard"}
			response.WitnessDigest = stateFetchWitnessDigest(response, item.AccessKind)
			responses = append(responses, response)
			continue
		}
		if item.Versioned {
			r.mu.Lock()
			value, ready := r.stateVersionValueLocked(item.Key, item.RequiredVersion)
			r.mu.Unlock()
			if !ready {
				response := StateFetchResponse{RequestID: item.RequestID, TxID: item.TxID, BlockHash: item.BlockHash, Key: item.Key, QualifiedKey: qualifiedKey, HomeShard: item.HomeShard, ExecutionShard: item.ExecutionShard, StateVersion: item.RequiredVersion, Versioned: true, Success: false, Error: "state_version_not_ready"}
				response.WitnessDigest = stateFetchWitnessDigest(response, item.AccessKind)
				responses = append(responses, response)
				continue
			}
			response := StateFetchResponse{RequestID: item.RequestID, TxID: item.TxID, BlockHash: item.BlockHash, Key: item.Key, QualifiedKey: qualifiedKey, Value: value, HomeShard: item.HomeShard, ExecutionShard: item.ExecutionShard, StateRoot: r.plugins.StateStorage.Root(r.db), StateVersion: item.RequiredVersion, Versioned: true, Success: true}
			response.WitnessDigest = stateFetchWitnessDigest(response, item.AccessKind)
			responses = append(responses, response)
			continue
		}
		snapshot, root := r.stateFetchSnapshot(item)
		response := StateFetchResponse{RequestID: item.RequestID, TxID: item.TxID, BlockHash: item.BlockHash, Key: item.Key, QualifiedKey: qualifiedKey, Value: snapshot[qualifiedKey], HomeShard: item.HomeShard, ExecutionShard: item.ExecutionShard, StateRoot: root, Success: true}
		response.WitnessDigest = stateFetchWitnessDigest(response, item.AccessKind)
		responses = append(responses, response)
	}
	payload := StateFetchBatchResponse{BatchID: request.BatchID, BlockHash: request.BlockHash, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, Items: responses}
	envelope, err := p2p.NewEnvelope(stateFetchBatchResponseMessage, r.node.NodeID, requester, r.node.ShardID, 0, 0, 0, payload)
	if err != nil {
		return err
	}
	return r.sendStateAccessToNode(ctx, requester, envelope)
}

func (r *NodeRuntime) handleStateFetchBatchResponse(response StateFetchBatchResponse) {
	for _, item := range response.Items {
		r.handleStateFetchResponse(item)
	}
}

func foldSafeMetaTrackRemoteItems(items []state.StateKV) (out []state.StateKV, folded, skippedExact int) {
	if len(items) < 2 {
		return items, 0, 0
	}
	groups := map[string][]state.StateKV{}
	order := []string{}
	for _, item := range items {
		key := item.Key
		if _, ok := groups[key]; !ok {
			order = append(order, key)
		}
		groups[key] = append(groups[key], item)
	}
	for _, key := range order {
		group := groups[key]
		if len(group) < 2 {
			out = append(out, group...)
			continue
		}
		exact := false
		commutative := true
		for _, item := range group {
			if item.ProducedVersion > 0 {
				exact = true
			}
			if item.UpdateSemantics != "commutative_delta" {
				commutative = false
			}
		}
		if exact {
			skippedExact += len(group) - 1
			out = append(out, group...)
			continue
		}
		if !commutative {
			out = append(out, group...)
			continue
		}
		merged := group[len(group)-1]
		merged.Delta = 0
		merged.TxIDs = nil
		seen := map[string]bool{}
		for _, item := range group {
			merged.Delta += item.Delta
			for _, txID := range item.TxIDs {
				if !seen[txID] {
					seen[txID] = true
					merged.TxIDs = append(merged.TxIDs, txID)
				}
			}
			if item.RoutingOrdinal > merged.RoutingOrdinal {
				merged.RoutingOrdinal = item.RoutingOrdinal
			}
		}
		out = append(out, merged)
		folded += len(group) - 1
	}
	return out, folded, skippedExact
}

// applyMetaTrackRemoteDeltasBatched changes transport granularity only. Home
// validators still validate every item and enqueue every non-folded item into
// the existing consensus-bound pendingStateDeltas path.
func (r *NodeRuntime) applyMetaTrackRemoteDeltasBatched(ctx context.Context, block realblock.Block, physicalDelta []state.StateKV, txDeltas []execution.TxDelta) ([]state.StateKV, error) {
	shardIDs := r.shardIDs()
	local := make([]state.StateKV, 0, len(physicalDelta))
	grouped := map[string][]state.StateKV{}
	unqualified := map[string]string{}
	for _, item := range physicalDelta {
		key, ok := unqualifiedLocalKey(item.Key, r.node.ShardID)
		if !ok {
			local = append(local, item)
			continue
		}
		home := r.stateHomeShardForKey(key, shardIDs)
		if home == "" || home == r.node.ShardID {
			local = append(local, item)
			continue
		}
		if !r.isCurrentLeader() {
			continue
		}
		remoteItems := metaTrackRemoteWritebackItems(item, key, block.TxList, txDeltas)
		for _, remoteItem := range remoteItems {
			if remoteItem.ProducedVersion > 0 && remoteItem.UpdateSemantics != "commutative_delta" {
				continue
			}
			grouped[home] = append(grouped[home], remoteItem)
			unqualified[remoteItem.Key] = key
		}
	}
	homes := make([]string, 0, len(grouped))
	for home := range grouped {
		homes = append(homes, home)
	}
	sort.Strings(homes)
	for _, home := range homes {
		items := grouped[home]
		sort.SliceStable(items, func(i, j int) bool {
			if items[i].RoutingOrdinal != items[j].RoutingOrdinal {
				return items[i].RoutingOrdinal < items[j].RoutingOrdinal
			}
			return items[i].Key < items[j].Key
		})
		before := len(items)
		folded := 0
		skipped := 0
		if r.metaTrackBlockExecutorFlag("safe_state_fold") {
			items, folded, skipped = foldSafeMetaTrackRemoteItems(items)
		}
		r.incrementFullLocalityMetric("metatrack_safe_fold_input_item_count", int64(before))
		r.incrementFullLocalityMetric("metatrack_safe_fold_output_item_count", int64(len(items)))
		r.incrementFullLocalityMetric("metatrack_safe_folded_item_count", int64(folded))
		r.incrementFullLocalityMetric("metatrack_safe_fold_skipped_exact_item_count", int64(skipped))
		if err := r.applyRemoteStateDeltaBatch(ctx, block, home, items, unqualified, txDeltas); err != nil {
			return nil, err
		}
	}
	return local, nil
}

func (r *NodeRuntime) applyRemoteStateDeltaBatch(ctx context.Context, block realblock.Block, homeShard string, items []state.StateKV, unqualified map[string]string, txDeltas []execution.TxDelta) error {
	if len(items) == 0 {
		return nil
	}
	targets := r.stateAccessNodeIDsForShard(homeShard)
	if len(targets) == 0 {
		return fmt.Errorf("metatrack batch writeback home nodes missing for %s", homeShard)
	}
	for _, target := range targets {
		requests := make([]StateDeltaApplyRequest, 0, len(items))
		waiters := map[string]chan StateDeltaApplyAck{}
		for _, item := range items {
			key := unqualified[item.Key]
			joined := strings.Join(item.TxIDs, "|")
			baseValue := item.BaseValue
			baseDigest := item.BaseValueDigest
			if item.UpdateSemantics != "commutative_delta" && baseDigest == "" {
				if observed, ok := remoteWritebackBaseObservation(key, item.TxIDs, txDeltas); ok {
					baseValue = observed
					baseDigest = stateValueDigest(observed)
				}
			}
			requestID := stableTextDigest(strings.Join([]string{"batch_writeback", r.node.NodeID, target, block.BlockHash, joined, item.Key, key, item.Value, item.UpdateSemantics, fmt.Sprint(item.Delta), baseDigest, fmt.Sprint(item.RoutingOrdinal), homeShard, r.stateAccessPartitionID()}, "|"))
			request := r.plugins.StateAccess.BuildDeltaApplyRequest(StateDeltaApplyInput{RequestID: requestID, TxID: joined, TxIDs: append([]string(nil), item.TxIDs...), BlockHash: block.BlockHash, Key: key, Value: item.Value, UpdateSemantics: item.UpdateSemantics, Delta: item.Delta, BaseValue: baseValue, BaseValueDigest: baseDigest, ApplyOrigin: item.ApplyOrigin, DeltaKind: item.DeltaKind, HasInitialValue: item.HasInitialValue, InitialValue: item.InitialValue, HomeShard: homeShard, ExecutionShard: r.stateAccessPartitionID(), SourceKey: item.Key, SourceHeight: block.Height, RoutingOrdinal: item.RoutingOrdinal, PreviousVersion: item.PreviousVersion, ProducedVersion: item.ProducedVersion, OrderingNoop: item.OrderingNoop})
			waiter := make(chan StateDeltaApplyAck, 1)
			r.mu.Lock()
			if r.stateApplyWaiters == nil {
				r.stateApplyWaiters = map[string]chan StateDeltaApplyAck{}
			}
			r.stateApplyWaiters[requestID] = waiter
			r.mu.Unlock()
			waiters[requestID] = waiter
			requests = append(requests, request)
		}
		batchID := stableTextDigest(strings.Join([]string{r.node.NodeID, target, block.BlockHash, homeShard, r.stateAccessPartitionID(), fmt.Sprint(len(requests))}, "|"))
		payload := StateDeltaApplyBatchRequest{BatchID: batchID, BlockHash: block.BlockHash, HomeShard: homeShard, ExecutionShard: r.stateAccessPartitionID(), Items: requests}
		envelope, err := p2p.NewEnvelope(stateDeltaApplyBatchMessage, r.node.NodeID, target, r.node.ShardID, block.Height, 0, block.Height, payload)
		if err != nil {
			return err
		}
		started := time.Now()
		if err := r.sendStateAccessToNode(ctx, target, envelope); err != nil {
			return err
		}
		r.incrementFullLocalityMetric("metatrack_batch_writeback_request_count", 1)
		r.incrementFullLocalityMetric("metatrack_batch_writeback_item_count", int64(len(requests)))
		for i, request := range requests {
			waiter := waiters[request.RequestID]
			select {
			case ack := <-waiter:
				r.mu.Lock()
				delete(r.stateApplyWaiters, request.RequestID)
				r.mu.Unlock()
				if !ack.Success {
					return fmt.Errorf("metatrack batch writeback failed on %s: %s", target, ack.Error)
				}
				r.recordRemoteStateApply(block, items[i], request.Key, ack, time.Since(started))
			case <-time.After(2 * time.Second):
				r.mu.Lock()
				delete(r.stateApplyWaiters, request.RequestID)
				r.mu.Unlock()
				return fmt.Errorf("metatrack batch writeback timed out for %s", request.Key)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
	return nil
}

func (r *NodeRuntime) handleStateDeltaApplyBatch(ctx context.Context, requester string, request StateDeltaApplyBatchRequest) error {
	acks := make([]StateDeltaApplyAck, 0, len(request.Items))
	for _, item := range request.Items {
		acks = append(acks, r.handleStateDeltaApplyRequest(item))
	}
	payload := StateDeltaApplyBatchAck{BatchID: request.BatchID, BlockHash: request.BlockHash, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, Items: acks}
	envelope, err := p2p.NewEnvelope(stateDeltaApplyBatchAckMessage, r.node.NodeID, requester, r.node.ShardID, 0, 0, 0, payload)
	if err != nil {
		return err
	}
	return r.sendStateAccessToNode(ctx, requester, envelope)
}
func (r *NodeRuntime) handleStateDeltaApplyBatchAck(ack StateDeltaApplyBatchAck) {
	for _, item := range ack.Items {
		r.handleStateDeltaApplyAck(item)
	}
}
