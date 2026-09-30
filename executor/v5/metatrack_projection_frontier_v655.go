package v5

import (
	"fmt"
	"sort"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_DEPENDENCY_ORDER_READY_PROJECTION_FRONTIER_V655
//
// v6.5.5 keeps the v6.5.3 per-mempool static dependency index and changes only
// which complete signed RouteBatch projections may share one PBFT block. A
// projection is eligible when none of its currently-active predecessor
// projections constrain either:
//  1. execution readiness (RAW / exact-value / same-sender nonce), or
//  2. deterministic materialization order (WAW / WAR).
//
// All eligible projections are emitted in signed RouteBatchSequence order under
// the existing common block_size ceiling. No empirical width/depth threshold is
// introduced, projections are never split, PBFT remains single-height, and a
// blocked projection cannot be crossed when any signed static execution/order
// relation connects it to the later projection.
type metaTrackProjectionPredecessorsV655 struct {
	Execution []uint64
	Ordering  []uint64
}

func (index *metaTrackDependencyIndexV653) activeProjectionPredecessorsV655(items []tx.SignedTransaction) (map[uint64]metaTrackProjectionPredecessorsV655, error) {
	index.mu.Lock()
	defer index.mu.Unlock()

	activeSequences := map[uint64]bool{}
	for _, item := range items {
		if item.ExecutionRouting == nil {
			return nil, fmt.Errorf("metatrack v6.5.5 active projection missing routing metadata")
		}
		activeSequences[item.ExecutionRouting.RouteBatchSequence] = true
	}

	out := map[uint64]metaTrackProjectionPredecessorsV655{}
	for sequence := range activeSequences {
		execution := make([]uint64, 0)
		for predecessor := range index.projectionPred[sequence] {
			if activeSequences[predecessor] {
				execution = append(execution, predecessor)
			}
		}
		ordering := make([]uint64, 0)
		for predecessor := range index.projectionOrderPred[sequence] {
			if activeSequences[predecessor] {
				ordering = append(ordering, predecessor)
			}
		}
		sort.Slice(execution, func(i, j int) bool { return execution[i] < execution[j] })
		sort.Slice(ordering, func(i, j int) bool { return ordering[i] < ordering[j] })
		out[sequence] = metaTrackProjectionPredecessorsV655{Execution: execution, Ordering: ordering}
	}
	return out, nil
}

// selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655 selects the whole
// currently-ready projection frontier instead of only the contiguous ready
// prefix used by v6.5.3. This lets a later independent projection share a block
// with an earlier ready projection even when an unrelated middle projection is
// waiting on a predecessor. The final v6 dependency-closure validator remains
// the fail-closed proposal oracle.
func selectMetaTrackDependencyClosedPBFTProjectionsIndexedV655(items []tx.SignedTransaction, limit int, shardID string, pool *mempool.Mempool) ([]tx.SignedTransaction, []tx.SignedTransaction, []metaTrackBatchProjectionIdentity, error) {
	candidate, _, projections, err := selectMetaTrackAggregatedBatchProjections(items, limit, shardID)
	if err != nil {
		return nil, nil, nil, err
	}
	index := metaTrackDependencyIndexForPoolV653(pool)
	if err := index.ensure(candidate); err != nil {
		return nil, nil, nil, err
	}
	projectionPredecessors, err := index.activeProjectionPredecessorsV655(candidate)
	if err != nil {
		return nil, nil, nil, err
	}

	readySequences := map[uint64]bool{}
	for _, projection := range projections {
		preds := projectionPredecessors[projection.Sequence]
		// Execution predecessors stay out of the same block by design. This keeps
		// the v6.5.3 shallow dependency-wave behavior and avoids recreating the
		// v6.5.1/v6.5.2 deep-chain aggregate block.
		if len(preds.Execution) != 0 {
			continue
		}
		// Ordering-only predecessors do not block parallel execution, but they do
		// constrain ledger materialization order. A later projection may therefore
		// share this block only when every still-active ordering predecessor was
		// already selected earlier in the same signed sequence order.
		orderingClosed := true
		for _, predecessor := range preds.Ordering {
			if !readySequences[predecessor] {
				orderingClosed = false
				break
			}
		}
		if orderingClosed {
			readySequences[projection.Sequence] = true
		}
	}
	if len(readySequences) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.5 dependency/order-ready projection frontier is empty")
	}

	selected := make([]tx.SignedTransaction, 0, len(candidate))
	selectedOrdinals := map[uint64]bool{}
	for _, item := range candidate {
		if item.ExecutionRouting == nil {
			return nil, nil, nil, fmt.Errorf("metatrack v6.5.5 projection missing execution routing: tx=%s", txIdentifier(item))
		}
		if !readySequences[item.ExecutionRouting.RouteBatchSequence] {
			continue
		}
		selected = append(selected, item)
		selectedOrdinals[item.ExecutionRouting.RoutingOrdinal] = true
	}
	if len(selected) == 0 {
		return nil, nil, nil, fmt.Errorf("metatrack v6.5.5 ready projection frontier chose no transactions")
	}

	selectedProjections := make([]metaTrackBatchProjectionIdentity, 0, len(readySequences))
	for _, projection := range projections {
		if readySequences[projection.Sequence] {
			selectedProjections = append(selectedProjections, projection)
		}
	}
	deferred := make([]tx.SignedTransaction, 0, len(items)-len(selected))
	for _, item := range items {
		if item.ExecutionRouting != nil && selectedOrdinals[item.ExecutionRouting.RoutingOrdinal] {
			continue
		}
		deferred = append(deferred, item)
	}

	if _, err := validateMetaTrackDependencyClosedProjectionBlock(realblock.Block{ShardID: shardID, TxList: selected}, true); err != nil {
		return nil, nil, nil, err
	}
	return selected, deferred, selectedProjections, nil
}
