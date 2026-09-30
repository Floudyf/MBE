package v5

import (
	"context"
	"fmt"
	"sync"

	realblock "metaverse-chainlab/executor/realism/block"
)

// V6.5.6 does not weaken durability. It only moves the existing v6.4 async
// publisher join from the end of transaction execution to the last barrier
// before durable block-store commit, allowing local commit planning/state WAL
// work to overlap background final-version publication.
type metaTrackDeferredFinalJoinV656 struct {
	buffer *metaTrackVersionPublishBuffer
}

var metaTrackDeferredFinalJoinsV656 sync.Map // key runtime|block_hash -> *metaTrackDeferredFinalJoinV656

func metaTrackDeferredFinalJoinKeyV656(r *NodeRuntime, block realblock.Block) string {
	return fmt.Sprintf("%p|%s", r, block.BlockHash)
}

func (r *NodeRuntime) deferOrJoinMetaTrackAsyncVersionsV656(ctx context.Context, block realblock.Block, buffer *metaTrackVersionPublishBuffer) error {
	// v6.5.6.5 liveness rollback: real-cluster evidence showed that deferring the
	// leader-owned cross-shard final publisher into the durable tail can stall both
	// shard leaders while validators advance. Restore the proven v6.4 boundary:
	// all async final publication is joined before commit planning/local WAL.
	// The transaction-frontier consensus cut remains enabled independently.
	if buffer == nil || buffer.asyncV640 == nil {
		return nil
	}
	r.incrementFullLocalityMetric("metatrack_final_join_immediate_restore_v6565_count", 1)
	return r.joinMetaTrackAsyncVersionsV640(ctx, buffer)
}

func (r *NodeRuntime) joinDeferredMetaTrackAsyncVersionsV656(ctx context.Context, block realblock.Block) error {
	key := metaTrackDeferredFinalJoinKeyV656(r, block)
	raw, ok := metaTrackDeferredFinalJoinsV656.LoadAndDelete(key)
	if !ok {
		return nil
	}
	r.incrementFullLocalityMetric("metatrack_final_join_late_barrier_v656_count", 1)
	return r.joinMetaTrackAsyncVersionsV640(ctx, raw.(*metaTrackDeferredFinalJoinV656).buffer)
}

func (r *NodeRuntime) cancelDeferredMetaTrackAsyncVersionsV656(blockHash string) {
	if r == nil || blockHash == "" {
		return
	}
	key := fmt.Sprintf("%p|%s", r, blockHash)
	raw, ok := metaTrackDeferredFinalJoinsV656.LoadAndDelete(key)
	if !ok {
		return
	}
	buffer := raw.(*metaTrackDeferredFinalJoinV656).buffer
	cancelMetaTrackAsyncVersionsV640(buffer)
	_ = r.joinMetaTrackAsyncVersionsV640(context.Background(), buffer)
	r.incrementFullLocalityMetric("metatrack_final_join_cancelled_v656_count", 1)
}
