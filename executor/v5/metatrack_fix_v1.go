package v5

// MBE_METATRACK_FIX_V1: deterministic mixed commutative materialization and
// bounded, retryable, fully acknowledged Home batch writeback.

import (
	"context"
	"fmt"
	"math"
	"strconv"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

// metaTrackCanonicalizeTxDeltaV1 converts worker-local absolute values for
// commutative keys to canonical per-tx delta applications. The worker result
// stays speculative: canonical writes are materialized in consensus order.
// Ordinary read/write keys keep the successful worker's validated writes.
// This function does not mutate working or the caller's write set on failure.
func metaTrackCanonicalizeTxDeltaV1(working map[string]string, shard string, item tx.SignedTransaction, writes map[string]string, success bool) (map[string]string, error) {
	canonical := make(map[string]string, len(writes))
	for key, value := range writes {
		canonical[key] = value
	}
	if !success {
		return canonical, nil
	}

	seenModes := map[string]tx.AccessMode{}
	increments := map[string]int64{}
	// SchedulingAccessList is a dependency projection and intentionally omits
	// Delta. Only the signed runtime AccessList carries the actual increment.
	for _, access := range item.AccessList {
		if access.Key == "" {
			continue
		}
		if oldMode, exists := seenModes[access.Key]; exists && oldMode != access.Mode {
			return nil, fmt.Errorf("metatrack mixed access modes on same key %s", access.Key)
		}
		seenModes[access.Key] = access.Mode
		if access.Mode != tx.AccessCommutativeDelta {
			continue
		}
		// A single logical key must have exactly one declared delta per tx;
		// otherwise the signed access-list semantics are ambiguous.
		if _, exists := increments[access.Key]; exists {
			return nil, fmt.Errorf("metatrack duplicate commutative access for %s", access.Key)
		}
		increments[access.Key] = access.Delta
	}
	for key, increment := range increments {
		qualified := qualifyStateKey(shard, key)
		raw := working[qualified]
		var base int64
		if raw != "" {
			parsed, err := strconv.ParseInt(raw, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("metatrack invalid commutative base for %s: %w", key, err)
			}
			base = parsed
		}
		if (increment > 0 && base > math.MaxInt64-increment) || (increment < 0 && base < math.MinInt64-increment) {
			return nil, fmt.Errorf("metatrack commutative overflow on %s", key)
		}
		canonical[key] = strconv.FormatInt(base+increment, 10)
	}
	return canonical, nil
}

// metaTrackValidateBatchAckV1 rejects a reply which cannot be paired with
// the exact request. Home ACK means *queued for Home consensus*, not durable.
func metaTrackValidateBatchAckV1(request StateDeltaApplyRequest, ack StateDeltaApplyAck) error {
	if ack.RequestID != request.RequestID || ack.BlockHash != request.BlockHash ||
		ack.TxID != request.TxID || ack.Key != request.Key ||
		ack.QualifiedKey != request.HomeShard+"::"+request.Key ||
		ack.ValueDigest != stableTextDigest("queued:"+request.Value) ||
		ack.HomeShard != request.HomeShard || ack.ExecutionShard != request.ExecutionShard ||
		ack.UpdateSemantics != request.UpdateSemantics || ack.Delta != request.Delta ||
		ack.PreviousVersion != request.PreviousVersion || ack.ProducedVersion != request.ProducedVersion ||
		ack.OrderingNoop != request.OrderingNoop || ack.ApplyOrigin != request.ApplyOrigin ||
		ack.DeltaKind != request.DeltaKind || ack.BaseValueDigest != request.BaseValueDigest ||
		ack.HasInitialValue != request.HasInitialValue || ack.InitialValue != request.InitialValue {
		return fmt.Errorf("metatrack batch writeback ACK identity mismatch for %s", request.RequestID)
	}
	if ack.WitnessDigest == "" || ack.WitnessDigest != stateDeltaApplyWitnessDigest(ack) {
		return fmt.Errorf("metatrack batch writeback ACK witness mismatch for %s", request.RequestID)
	}
	if !ack.Success {
		return fmt.Errorf("metatrack batch writeback Home rejected %s: %s", request.RequestID, ack.Error)
	}
	return nil
}

// metaTrackAwaitBatchAcksV1 uses one deadline per batch rather than one 2s
// deadline per item. Retried *identical requests* are safe because the Home
// deduplicates by stateDeltaApplyKey before Home consensus applies a delta.
// It does not treat a timeout, send success or a quorum as a Home ACK.
func metaTrackAwaitBatchAcksV1(ctx context.Context, requests []StateDeltaApplyRequest,
	incoming <-chan StateDeltaApplyAck, retryEvery, maxWait time.Duration,
	resend func() error, accept func(int, StateDeltaApplyAck) error) error {
	if len(requests) == 0 {
		return nil
	}
	if retryEvery <= 0 || maxWait <= retryEvery {
		return fmt.Errorf("metatrack invalid batch writeback timeout policy")
	}
	pending := make(map[string]int, len(requests))
	for i, request := range requests {
		if request.RequestID == "" {
			return fmt.Errorf("metatrack batch writeback request missing ID")
		}
		if _, duplicated := pending[request.RequestID]; duplicated {
			return fmt.Errorf("metatrack duplicate batch writeback ID: %s", request.RequestID)
		}
		pending[request.RequestID] = i
	}
	retry := time.NewTicker(retryEvery)
	defer retry.Stop()
	deadline := time.NewTimer(maxWait)
	defer deadline.Stop()
	for len(pending) > 0 {
		select {
		case ack := <-incoming:
			index, wanted := pending[ack.RequestID]
			if !wanted {
				continue
			} // Duplicate ACKs and already-completed IDs.
			if err := metaTrackValidateBatchAckV1(requests[index], ack); err != nil {
				return err
			}
			if err := accept(index, ack); err != nil {
				return err
			}
			delete(pending, ack.RequestID)
		case <-retry.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := resend(); err != nil {
				return fmt.Errorf("metatrack batch writeback retransmit: %w", err)
			}
		case <-deadline.C:
			return fmt.Errorf("metatrack batch writeback ACK deadline exceeded, %d/%d Home items unacknowledged", len(pending), len(requests))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
