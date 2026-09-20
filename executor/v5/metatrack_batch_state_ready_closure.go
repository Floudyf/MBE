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

func selectMetaTrackBatchProjectionGroups(items []tx.SignedTransaction, limit int, shardID string, maxProjectionCount int) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	if len(items) == 0 {
		return nil, nil, nil, fmt.Errorf("empty_mempool")
	}
	if limit <= 0 {
		return nil, nil, nil, fmt.Errorf("metatrack batch projection requires positive block limit")
	}

	indicesBySequence := map[uint64][]int{}
	identityBySequence := map[uint64]metaTrackBatchProjectionIdentity{}
	sequences := make([]uint64, 0)
	for index, item := range items {
		identity, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return nil, nil, nil, err
		}
		if identity.ExecutionShard != shardID {
			return nil, nil, nil, fmt.Errorf("metatrack transaction %s routed to %s but reserved on %s", txIdentifier(item), identity.ExecutionShard, shardID)
		}
		if existing, ok := identityBySequence[identity.Sequence]; ok {
			if existing != identity {
				return nil, nil, nil, fmt.Errorf("metatrack route-batch identity mismatch within sequence %d", identity.Sequence)
			}
		} else {
			identityBySequence[identity.Sequence] = identity
			sequences = append(sequences, identity.Sequence)
		}
		indicesBySequence[identity.Sequence] = append(indicesBySequence[identity.Sequence], index)
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })

	selectedIndex := map[int]bool{}
	selected := make([]tx.SignedTransaction, 0, limit)
	projections := make([]metaTrackBatchProjectionIdentity, 0)
	for _, sequence := range sequences {
		if maxProjectionCount > 0 && len(projections) >= maxProjectionCount {
			break
		}
		identity := identityBySequence[sequence]
		indices := append([]int(nil), indicesBySequence[sequence]...)
		if identity.ShardTransactionCount > limit && len(projections) == 0 {
			return nil, nil, nil, fmt.Errorf("metatrack signed shard projection exceeds block limit: sequence=%d expected=%d limit=%d", identity.Sequence, identity.ShardTransactionCount, limit)
		}
		if len(indices) < identity.ShardTransactionCount {
			if len(projections) == 0 {
				return nil, nil, nil, fmt.Errorf("%w: sequence=%d shard=%s have=%d expected=%d", errMetaTrackBatchProjectionIncomplete, identity.Sequence, shardID, len(indices), identity.ShardTransactionCount)
			}
			// Never skip over a partial earlier projection. Later complete
			// projections remain reserved for a future PBFT proposal.
			break
		}
		if len(indices) > identity.ShardTransactionCount {
			return nil, nil, nil, fmt.Errorf("metatrack route-batch projection overfilled: sequence=%d shard=%s have=%d expected=%d", identity.Sequence, shardID, len(indices), identity.ShardTransactionCount)
		}
		if len(selected)+len(indices) > limit {
			break
		}
		sort.SliceStable(indices, func(i, j int) bool {
			left := items[indices[i]].ExecutionRouting.RoutingOrdinal
			right := items[indices[j]].ExecutionRouting.RoutingOrdinal
			return left < right
		})
		lastOrdinal := uint64(0)
		for _, index := range indices {
			ordinal := items[index].ExecutionRouting.RoutingOrdinal
			if lastOrdinal != 0 && ordinal <= lastOrdinal {
				return nil, nil, nil, fmt.Errorf("metatrack route-batch routing ordinals are not strictly increasing")
			}
			lastOrdinal = ordinal
			selectedIndex[index] = true
			selected = append(selected, items[index])
		}
		projections = append(projections, identity)
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack no complete projection fits block limit=%d", limit)
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for index, item := range items {
		if !selectedIndex[index] {
			deferred = append(deferred, item)
		}
	}
	return selected, deferred, projections, nil
}

// selectMetaTrackBatchProjection preserves the legacy single-projection
// selector for compatibility tests and explicit ablations.
func selectMetaTrackBatchProjection(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, error) {
	selected, deferred, _, err := selectMetaTrackBatchProjectionGroups(items, limit, shardID, 1)
	return selected, deferred, err
}

// selectMetaTrackAggregatedBatchProjections is the MetaTrack full-path
// pre-consensus aggregation selector. It packs as many *complete* signed
// route-batch projections as fit under the normal block-size ceiling. A
// projection is never split to fill capacity.
func selectMetaTrackAggregatedBatchProjections(items []tx.SignedTransaction, limit int, shardID string) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	return selectMetaTrackBatchProjectionGroups(items, limit, shardID, 0)
}

func validateMetaTrackAggregatedBatchProjections(block realblock.Block, requireMetadata bool) ([]metaTrackBatchProjectionIdentity, error) {
	if len(block.TxList) == 0 {
		return nil, nil
	}
	firstRouting := block.TxList[0].ExecutionRouting
	metadataPresent := firstRouting != nil && (firstRouting.RouteBatchSequence != 0 || firstRouting.RouteBatchTransactionCount != 0 || firstRouting.RouteBatchShardTransactionCount != 0)
	if !metadataPresent && !requireMetadata {
		for _, item := range block.TxList[1:] {
			routing := item.ExecutionRouting
			if routing != nil && (routing.RouteBatchSequence != 0 || routing.RouteBatchTransactionCount != 0 || routing.RouteBatchShardTransactionCount != 0) {
				return nil, fmt.Errorf("metatrack block mixes legacy and signed route-batch metadata")
			}
		}
		return nil, nil
	}

	identityBySequence := map[uint64]metaTrackBatchProjectionIdentity{}
	countBySequence := map[uint64]int{}
	lastOrdinalBySequence := map[uint64]uint64{}
	sequences := make([]uint64, 0)
	lastSequence := uint64(0)
	for _, item := range block.TxList {
		current, err := metaTrackBatchProjectionIdentityForTransaction(item)
		if err != nil {
			return nil, err
		}
		if current.ExecutionShard != block.ShardID {
			return nil, fmt.Errorf("metatrack batch projection shard mismatch: signed=%s block=%s", current.ExecutionShard, block.ShardID)
		}
		if lastSequence != 0 && current.Sequence < lastSequence {
			return nil, fmt.Errorf("metatrack aggregate projections are not ordered by route-batch sequence")
		}
		if existing, ok := identityBySequence[current.Sequence]; ok {
			if existing != current {
				return nil, fmt.Errorf("metatrack block mixes route-batch identities within sequence %d", current.Sequence)
			}
		} else {
			identityBySequence[current.Sequence] = current
			sequences = append(sequences, current.Sequence)
		}
		lastOrdinal := lastOrdinalBySequence[current.Sequence]
		ordinal := item.ExecutionRouting.RoutingOrdinal
		if lastOrdinal != 0 && ordinal <= lastOrdinal {
			return nil, fmt.Errorf("metatrack route-batch routing ordinals are not strictly increasing")
		}
		lastOrdinalBySequence[current.Sequence] = ordinal
		countBySequence[current.Sequence]++
		lastSequence = current.Sequence
	}
	sort.Slice(sequences, func(i, j int) bool { return sequences[i] < sequences[j] })
	projections := make([]metaTrackBatchProjectionIdentity, 0, len(sequences))
	for _, sequence := range sequences {
		identity := identityBySequence[sequence]
		if countBySequence[sequence] != identity.ShardTransactionCount {
			return nil, fmt.Errorf("metatrack batch projection incomplete: sequence=%d shard=%s block_count=%d signed_expected=%d", identity.Sequence, block.ShardID, countBySequence[sequence], identity.ShardTransactionCount)
		}
		projections = append(projections, identity)
	}
	return projections, nil
}

// validateMetaTrackBatchProjection keeps the old single-projection contract for
// legacy callers/tests. The full MetaTrack path uses the aggregate validator.
func validateMetaTrackBatchProjection(block realblock.Block, requireMetadata bool) (metaTrackBatchProjectionIdentity, error) {
	projections, err := validateMetaTrackAggregatedBatchProjections(block, requireMetadata)
	if err != nil {
		return metaTrackBatchProjectionIdentity{}, err
	}
	if len(projections) == 0 {
		return metaTrackBatchProjectionIdentity{}, nil
	}
	if len(projections) != 1 {
		return metaTrackBatchProjectionIdentity{}, fmt.Errorf("metatrack expected one route-batch projection, got %d", len(projections))
	}
	return projections[0], nil
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
