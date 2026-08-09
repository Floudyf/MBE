package v5

import (
	"context"
	"fmt"
	"sort"
	"strings"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

var errMetaTrackBatchProjectionIncomplete = fmt.Errorf("metatrack batch projection incomplete")

type metaTrackBatchProjectionIdentity struct {
	Sequence              uint64
	PlanDigest            string
	TransactionCount      int
	ShardTransactionCount int
	ExecutionShard        string
}

func metaTrackBatchProjectionIdentityForTransaction(item tx.SignedTransaction) (metaTrackBatchProjectionIdentity, error) {
	if item.ExecutionRouting == nil {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack transaction %s missing signed execution routing metadata", txIdentifier(item))
	}
	if err := tx.ValidateExecutionRouting(item); err != nil {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack transaction %s routing metadata: %w", txIdentifier(item), err)
	}
	routing := item.ExecutionRouting
	if routing.RouteBatchSequence == 0 || routing.RouteBatchTransactionCount <= 0 || routing.RouteBatchShardTransactionCount <= 0 || routing.RouteBatchShardTransactionCount > routing.RouteBatchTransactionCount {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack transaction %s missing complete signed route-batch metadata", txIdentifier(item))
	}
	if strings.TrimSpace(routing.RoutePlanDigest) == "" || strings.TrimSpace(routing.ExecutionShard) == "" {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack transaction %s has incomplete signed batch identity", txIdentifier(item))
	}
	return metaTrackBatchProjectionIdentity{Sequence: routing.RouteBatchSequence, PlanDigest: routing.RoutePlanDigest, TransactionCount: routing.RouteBatchTransactionCount, ShardTransactionCount: routing.RouteBatchShardTransactionCount, ExecutionShard: routing.ExecutionShard}, nil
}

func selectMetaTrackBatchProjection(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, error) {
	if len(items) == 0 {
		return nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, fmt.Errorf("metatrack batch projection requires positive block limit")
	}
	identities := make([]metaTrackBatchProjectionIdentity, len(items))
	target := -1
	for index, item := range items {
		identity, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return nil, nil, err
		}
		if identity.ExecutionShard != shardID {
			return nil, nil, fmt.Errorf("metatrack transaction %s routed to %s but reserved on %s", txIdentifier(item), identity.ExecutionShard, shardID)
		}
		identities[index] = identity
		if target < 0 || identity.Sequence < identities[target].Sequence {
			target = index
		}
	}
	targetIdentity := identities[target]
	if targetIdentity.ShardTransactionCount > limit {
		return nil, nil, fmt.Errorf("metatrack signed shard projection exceeds block limit: sequence=%d expected=%d limit=%d", targetIdentity.Sequence, targetIdentity.ShardTransactionCount, limit)
	}
	selected := make([]tx.SignedTransaction, 0, targetIdentity.ShardTransactionCount)
	deferred := make([]tx.SignedTransaction, 0, len(items))
	for index, item := range items {
		identity := identities[index]
		if identity.Sequence != targetIdentity.Sequence {
			deferred = append(deferred, item)
			continue
		}
		if identity.PlanDigest != targetIdentity.PlanDigest || identity.TransactionCount != targetIdentity.TransactionCount || identity.ShardTransactionCount != targetIdentity.ShardTransactionCount || identity.ExecutionShard != targetIdentity.ExecutionShard {
			return nil, nil, fmt.Errorf("metatrack route-batch identity mismatch within sequence %d", targetIdentity.Sequence)
		}
		selected = append(selected, item)
	}
	if len(selected) < targetIdentity.ShardTransactionCount {
		return nil, nil, fmt.Errorf("%w: sequence=%d shard=%s have=%d expected=%d", errMetaTrackBatchProjectionIncomplete, targetIdentity.Sequence, shardID, len(selected), targetIdentity.ShardTransactionCount)
	}
	if len(selected) > targetIdentity.ShardTransactionCount {
		return nil, nil, fmt.Errorf("metatrack route-batch projection overfilled: sequence=%d shard=%s have=%d expected=%d", targetIdentity.Sequence, shardID, len(selected), targetIdentity.ShardTransactionCount)
	}
	sort.SliceStable(selected, func(i, j int) bool {
		return selected[i].ExecutionRouting.RoutingOrdinal < selected[j].ExecutionRouting.RoutingOrdinal
	})
	lastOrdinal := uint64(0)
	for _, item := range selected {
		ordinal := item.ExecutionRouting.RoutingOrdinal
		if lastOrdinal != 0 && ordinal <= lastOrdinal {
			return nil, nil, fmt.Errorf("metatrack route-batch routing ordinals are not strictly increasing")
		}
		lastOrdinal = ordinal
	}
	return selected, deferred, nil
}

func validateMetaTrackBatchProjection(block realblock.Block, requireMetadata bool) (metaTrackBatchProjectionIdentity, error) {
	if len(block.TxList) == 0 {
		return metaTrackBatchProjectionIdentity{}, nil
	}
	firstRouting := block.TxList[0].ExecutionRouting
	metadataPresent := firstRouting != nil && (firstRouting.RouteBatchSequence != 0 || firstRouting.RouteBatchTransactionCount != 0 || firstRouting.RouteBatchShardTransactionCount != 0)
	if !metadataPresent && !requireMetadata {
		for _, item := range block.TxList[1:] {
			routing := item.ExecutionRouting
			if routing != nil && (routing.RouteBatchSequence != 0 || routing.RouteBatchTransactionCount != 0 || routing.RouteBatchShardTransactionCount != 0) {
				return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack block mixes legacy and signed route-batch metadata")
			}
		}
		return metaTrackBatchProjectionIdentity{}, nil
	}
	identity, err := metaTrackBatchProjectionIdentityForTransaction(block.TxList[0])
	if err != nil {
		return metaTrackBatchProjectionIdentity{}, err
	}
	if identity.ExecutionShard != block.ShardID {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack batch projection shard mismatch: signed=%s block=%s", identity.ExecutionShard, block.ShardID)
	}
	if identity.ShardTransactionCount != len(block.TxList) {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack batch projection incomplete: sequence=%d shard=%s block_count=%d signed_expected=%d", identity.Sequence, block.ShardID, len(block.TxList), identity.ShardTransactionCount)
	}
	lastOrdinal := uint64(0)
	for _, item := range block.TxList {
		current, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return metaTrackBatchProjectionIdentity{}, err
		}
		if current != identity {
			return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack block mixes route-batch identities")
		}
		ordinal := item.ExecutionRouting.RoutingOrdinal
		if lastOrdinal != 0 && ordinal <= lastOrdinal {
			return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack block routing ordinals are not strictly increasing")
		}
		lastOrdinal = ordinal
	}
	return identity, nil
}

type stateVersionRemoteSubscription struct {
	requester string
	request   StateFetchRequest
}

func stateVersionWaitKey(key string, version uint64) string {
	return fmt.Sprintf("%020d|%s", version, key)
}

func (r *NodeRuntime) stateVersionValueLocked(key string, version uint64) (string, bool) {
	if version == 0 {
		return r.stateVersionInitial[key], true
	}
	values := r.stateVersionValues[key]
	if values == nil {
		return "", false
	}
	value, ok := values[version]
	return value, ok
}

func (r *NodeRuntime) stateVersionSignalLocked(key string, version uint64) <-chan struct{} {
	waitKey := stateVersionWaitKey(key, version)
	if r.stateVersionSignals == nil {
		r.stateVersionSignals = map[string]chan struct{}{}
	}
	signal := r.stateVersionSignals[waitKey]
	if signal == nil {
		signal = make(chan struct{})
		r.stateVersionSignals[waitKey] = signal
	}
	return signal
}

func (r *NodeRuntime) registerStateVersionRemoteSubscriptionLocked(requester string, request StateFetchRequest) {
	waitKey := stateVersionWaitKey(request.Key, request.RequiredVersion)
	if r.stateVersionRemoteSubscriptions == nil {
		r.stateVersionRemoteSubscriptions = map[string]map[string]stateVersionRemoteSubscription{}
	}
	bucket := r.stateVersionRemoteSubscriptions[waitKey]
	if bucket == nil {
		bucket = map[string]stateVersionRemoteSubscription{}
		r.stateVersionRemoteSubscriptions[waitKey] = bucket
	}
	if _, exists := bucket[request.RequestID]; exists {
		return
	}
	bucket[request.RequestID] = stateVersionRemoteSubscription{requester: requester, request: request}
	r.incrementRuntimeMetricLocked("state_version_subscription_registered_count")
}

func (r *NodeRuntime) takeStateVersionRemoteSubscriptionsLocked(key string, version uint64) []stateVersionRemoteSubscription {
	waitKey := stateVersionWaitKey(key, version)
	bucket := r.stateVersionRemoteSubscriptions[waitKey]
	if len(bucket) == 0 {
		return nil
	}
	out := make([]stateVersionRemoteSubscription, 0, len(bucket))
	for _, subscription := range bucket {
		out = append(out, subscription)
	}
	delete(r.stateVersionRemoteSubscriptions, waitKey)
	sort.Slice(out, func(i, j int) bool {
		if out[i].requester != out[j].requester {
			return out[i].requester < out[j].requester
		}
		return out[i].request.RequestID < out[j].request.RequestID
	})
	return out
}

func (r *NodeRuntime) notifyStateVersionRemoteSubscriptions(ctx context.Context, key string, version uint64, value string, subscriptions []stateVersionRemoteSubscription) error {
	for _, subscription := range subscriptions {
		request := subscription.request
		response := StateFetchResponse{RequestID: request.RequestID, TxID: request.TxID, BlockHash: request.BlockHash, Key: request.Key, QualifiedKey: request.HomeShard + "::" + request.Key, Value: value, HomeShard: request.HomeShard, ExecutionShard: request.ExecutionShard, StateRoot: r.plugins.StateStorage.Root(r.db), StateVersion: version, Versioned: true, Success: true}
		response.WitnessDigest = stateFetchWitnessDigest(response, request.AccessKind)
		if err := r.enqueueStateFetchResponse(ctx, subscription.requester, response); err != nil {
			return err
		}
		r.mu.Lock()
		r.incrementRuntimeMetricLocked("state_version_subscription_wakeup_count")
		r.mu.Unlock()
	}
	return nil
}
