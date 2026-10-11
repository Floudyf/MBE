package v5

// MBE_MT_TAPOS_V1: bounded, retryable and validated single-item Home delivery.
// Changes only MetaTrack's state-apply transport; durable application stays
// solely in the existing Home-shard consensus-bound pending-delta path.

import (
	"context"
	"fmt"
	"strings"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/state"
)

func metaTrackValidateSingleAckV1(request StateDeltaApplyRequest, ack StateDeltaApplyAck) error {
	if ack.RequestID != request.RequestID || ack.BlockHash != request.BlockHash ||
		ack.TxID != request.TxID || ack.Key != request.Key ||
		ack.HomeShard != request.HomeShard || ack.ExecutionShard != request.ExecutionShard ||
		ack.UpdateSemantics != request.UpdateSemantics || ack.Delta != request.Delta ||
		ack.PreviousVersion != request.PreviousVersion || ack.ProducedVersion != request.ProducedVersion ||
		ack.OrderingNoop != request.OrderingNoop || ack.ApplyOrigin != request.ApplyOrigin ||
		ack.DeltaKind != request.DeltaKind || ack.BaseValueDigest != request.BaseValueDigest ||
		ack.HasInitialValue != request.HasInitialValue || ack.InitialValue != request.InitialValue ||
		ack.QualifiedKey != request.HomeShard+"::"+request.Key ||
		ack.ValueDigest != stableTextDigest("queued:"+request.Value) {
		return fmt.Errorf("metatrack single ACK identity mismatch: %s", request.RequestID)
	}
	if ack.WitnessDigest == "" || ack.WitnessDigest != stateDeltaApplyWitnessDigest(ack) {
		return fmt.Errorf("metatrack single ACK witness mismatch: %s", request.RequestID)
	}
	if !ack.Success {
		return fmt.Errorf("metatrack single Home rejected %s: %s", request.RequestID, ack.Error)
	}
	return nil
}

// The same logical request ID is retained over retries so Home can deduplicate
// with stateDeltaApplyKey. The SEND callback must create a NEW wire envelope on
// each attempt; retransmitting a wire message ID might be ignored by P2P.
func metaTrackWaitSingleAckV1(ctx context.Context, request StateDeltaApplyRequest, replies <-chan StateDeltaApplyAck,
	send func(context.Context) error, interval, limit time.Duration) (StateDeltaApplyAck, int, error) {
	if request.RequestID == "" || interval <= 0 || limit <= interval || send == nil || replies == nil {
		return StateDeltaApplyAck{}, 0, fmt.Errorf("metatrack single ACK invalid wait policy")
	}
	bounded, cancel := context.WithTimeout(ctx, limit)
	defer cancel()
	if err := bounded.Err(); err != nil {
		return StateDeltaApplyAck{}, 0, err
	}
	if err := send(bounded); err != nil {
		return StateDeltaApplyAck{}, 0, err
	}
	attempts := 1
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case ack, ok := <-replies:
			if !ok {
				return StateDeltaApplyAck{}, attempts, fmt.Errorf("metatrack single ACK waiter closed")
			}
			if err := metaTrackValidateSingleAckV1(request, ack); err != nil {
				return StateDeltaApplyAck{}, attempts, err
			}
			return ack, attempts, nil
		case <-ticker.C:
			if err := bounded.Err(); err != nil {
				return StateDeltaApplyAck{}, attempts, err
			}
			if err := send(bounded); err != nil {
				return StateDeltaApplyAck{}, attempts, err
			}
			attempts++
		case <-bounded.Done():
			return StateDeltaApplyAck{}, attempts, fmt.Errorf("metatrack single Home ACK deadline for %s after %d attempts: %w", request.RequestID, attempts, bounded.Err())
		}
	}
}

func (r *NodeRuntime) applyMetaTrackSingleDeltaReliableV1(ctx context.Context, block realblock.Block, item state.StateKV, logicalKey, homeShard string, txDeltas []execution.TxDelta) ([]StateDeltaApplyAck, time.Duration, error) {
	targets := r.stateAccessNodeIDsForShard(homeShard)
	if len(targets) == 0 {
		return nil, 0, fmt.Errorf("metatrack single Home nodes missing for %s", homeShard)
	}
	joined := strings.Join(item.TxIDs, "|")
	baseValue, baseDigest := item.BaseValue, item.BaseValueDigest
	if item.UpdateSemantics != "commutative_delta" && baseDigest == "" {
		if observed, ok := remoteWritebackBaseObservation(logicalKey, item.TxIDs, txDeltas); ok {
			baseValue = observed
			baseDigest = stateValueDigest(observed)
		}
	}
	started := time.Now()
	acks := make([]StateDeltaApplyAck, 0, len(targets))
	for _, target := range targets {
		requestID := stableTextDigest(strings.Join([]string{r.node.NodeID, target, block.BlockHash, joined, item.Key, logicalKey, item.Value, item.UpdateSemantics, fmt.Sprint(item.Delta), baseDigest, fmt.Sprint(item.RoutingOrdinal), fmt.Sprint(item.PreviousVersion), fmt.Sprint(item.ProducedVersion), fmt.Sprint(item.OrderingNoop), homeShard, r.stateAccessPartitionID()}, "|"))
		request := r.plugins.StateAccess.BuildDeltaApplyRequest(StateDeltaApplyInput{
			RequestID: requestID, TxID: joined, TxIDs: append([]string(nil), item.TxIDs...), BlockHash: block.BlockHash, Key: logicalKey,
			Value: item.Value, UpdateSemantics: item.UpdateSemantics, Delta: item.Delta, BaseValue: baseValue, BaseValueDigest: baseDigest,
			ApplyOrigin: item.ApplyOrigin, DeltaKind: item.DeltaKind, HasInitialValue: item.HasInitialValue, InitialValue: item.InitialValue,
			HomeShard: homeShard, ExecutionShard: r.stateAccessPartitionID(), SourceKey: item.Key, SourceHeight: block.Height,
			RoutingOrdinal: item.RoutingOrdinal, PreviousVersion: item.PreviousVersion, ProducedVersion: item.ProducedVersion, OrderingNoop: item.OrderingNoop,
		})
		waiter := make(chan StateDeltaApplyAck, 4)
		r.mu.Lock()
		if r.stateApplyWaiters == nil {
			r.stateApplyWaiters = map[string]chan StateDeltaApplyAck{}
		}
		if r.stateApplyWaiters[requestID] != nil {
			r.mu.Unlock()
			return acks, time.Since(started), fmt.Errorf("metatrack duplicate in-flight single Home request %s", requestID)
		}
		r.stateApplyWaiters[requestID] = waiter
		r.mu.Unlock()
		ack, attempts, err := func() (StateDeltaApplyAck, int, error) {
			defer func() { r.mu.Lock(); delete(r.stateApplyWaiters, requestID); r.mu.Unlock() }()
			return metaTrackWaitSingleAckV1(ctx, request, waiter, func(sendCtx context.Context) error {
				// Every retry gets a fresh wire envelope while its logical
				// RequestID remains unchanged for Home idempotent dedup.
				envelope, err := p2p.NewEnvelope(stateDeltaApplyMessage, r.node.NodeID, target, r.node.ShardID, block.Height, 0, block.Height, request)
				if err != nil {
					return err
				}
				return r.sendStateAccessToNode(sendCtx, target, envelope)
			}, 1500*time.Millisecond, 12*time.Second)
		}()
		if attempts > 1 {
			r.incrementFullLocalityMetric("metatrack_single_writeback_retry_count", int64(attempts-1))
		}
		if err != nil {
			r.incrementFullLocalityMetric("metatrack_single_writeback_failure_count", 1)
			return acks, time.Since(started), fmt.Errorf("metatrack single Home writeback to %s/%s: %w", homeShard, target, err)
		}
		acks = append(acks, ack)
		r.incrementFullLocalityMetric("metatrack_single_writeback_ack_count", 1)
	}
	return acks, time.Since(started), nil
}
