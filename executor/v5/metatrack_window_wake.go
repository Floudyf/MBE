package v5

import (
	"fmt"
	"sync"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

type metaTrackWindowProjection struct {
	shard                    string
	window, start, end       uint64
	batches, total, expected int
	criticalPath             int
}

func metaTrackWindowProjectionFor(r *tx.ExecutionRoutingMetadata) metaTrackWindowProjection {
	return metaTrackWindowProjection{
		shard:        r.ExecutionShard,
		window:       r.ConsensusWindowSequence,
		start:        r.ConsensusWindowStartBatchSequence,
		end:          r.ConsensusWindowEndBatchSequence,
		batches:      r.ConsensusWindowRouteBatchCount,
		total:        r.ConsensusWindowTransactionCount,
		expected:     r.ConsensusWindowShardTransactionCount,
		criticalPath: r.ConsensusWindowCriticalPath,
	}
}

type metaTrackIncompleteWindow struct {
	projection metaTrackWindowProjection
	have       int
}

func (err *metaTrackIncompleteWindow) Error() string {
	return fmt.Sprintf("metatrack v6.5.6.8 consensus window incomplete: have=%d expected=%d", err.have, err.projection.expected)
}

// Track only the window the proposer found incomplete. The normal producer
// still validates and selects the block after a wakeup.
type metaTrackWindowWake struct {
	ready      chan struct{}
	mu         sync.Mutex
	projection metaTrackWindowProjection
	ordinals   map[uint64]struct{}
}

func (w *metaTrackWindowWake) arm(pool *mempool.Mempool, projection metaTrackWindowProjection) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.projection = projection
	w.ordinals = make(map[uint64]struct{}, projection.expected)
	// Reconcile arrivals between the proposer snapshot and arming the watch.
	// Admission releases the pool lock before it calls observe.
	pool.VisitReady(func(item tx.SignedTransaction) bool {
		w.record(item)
		return w.ordinals != nil
	})
}

func (w *metaTrackWindowWake) observe(item tx.SignedTransaction) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.record(item)
}

func (w *metaTrackWindowWake) clear() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ordinals = nil
}

func (w *metaTrackWindowWake) record(item tx.SignedTransaction) {
	r := item.ExecutionRouting
	if w.ordinals == nil || r == nil || r.RoutingOrdinal == 0 ||
		metaTrackWindowProjectionFor(r) != w.projection ||
		r.RouteBatchSequence < w.projection.start || r.RouteBatchSequence > w.projection.end {
		return
	}
	w.ordinals[r.RoutingOrdinal] = struct{}{}
	if len(w.ordinals) == w.projection.expected {
		w.ordinals = nil
		select {
		case w.ready <- struct{}{}:
		default:
		}
	}
}
