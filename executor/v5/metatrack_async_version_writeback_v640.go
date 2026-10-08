package v5

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_METATRACK_ASYNC_VERSION_WRITEBACK_V640
//
// v6.4.0 keeps the dependency-closed PBFT block and the original shared
// transaction-level Full Locality executor. Only closure-final durability work
// that is not needed to unblock a same-block remote exact-value consumer is
// moved off the execution-critical publication path.
//
// Critical remote_live versions and closure-final versions with a same-block
// remote exact-value consumer keep the historical producer-completion immediate
// publication path. Non-critical closure-final versions are enqueued at producer
// completion and drained per Home in the background. Commit joins all background
// writes before it can proceed. There is no timer or fixed transaction chunk.

type metaTrackAsyncVersionTaskV640 struct {
	HomeShard  string
	LogicalKey string
	Item       state.StateKV
	Delta      execution.TxDelta
	EnqueuedAt time.Time
}

type metaTrackAsyncVersionLaneV640 struct {
	finals   []metaTrackAsyncVersionTaskV640
	draining bool
}

type metaTrackAsyncVersionPublisherV640 struct {
	mu     sync.Mutex
	ctx    context.Context
	cancel context.CancelFunc
	lanes  map[string]*metaTrackAsyncVersionLaneV640
	wg     sync.WaitGroup
	closed bool
	err    error

	backgroundFinalEnqueued int64
	backgroundFinalWorkMS   int64
	batchCount              int64
	batchItemCount          int64
	maxQueueDepth           int64
}

func newMetaTrackAsyncVersionPublisherV640() *metaTrackAsyncVersionPublisherV640 {
	ctx, cancel := context.WithCancel(context.Background())
	return &metaTrackAsyncVersionPublisherV640{
		ctx:    ctx,
		cancel: cancel,
		lanes:  map[string]*metaTrackAsyncVersionLaneV640{},
	}
}

// metaTrackBlockRemoteValueConsumerIndexV640 derives an aggregate-PBFT-block
// view from already signed exact-version metadata. Only consumers on a different
// execution shard count here; same-execution-shard consumers keep using the
// existing local exact-version handoff and do not require Home publication to
// unblock execution.
func metaTrackBlockRemoteValueConsumerIndexV640(block realblock.Block) map[metaTrackVersionIdentity]int {
	producerShard := map[metaTrackVersionIdentity]string{}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.ProducedVersion == 0 {
				continue
			}
			producerShard[metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.ProducedVersion}] = item.ExecutionRouting.ExecutionShard
		}
	}
	consumers := map[metaTrackVersionIdentity]int{}
	for _, item := range block.TxList {
		if item.ExecutionRouting == nil {
			continue
		}
		consumerShard := item.ExecutionRouting.ExecutionShard
		for _, dependency := range item.ExecutionRouting.StateVersions {
			if dependency.Key == "" || dependency.RequiredVersion == 0 || !transactionRequiresExactStateValue(item, dependency.Key) {
				continue
			}
			identity := metaTrackVersionIdentity{Key: dependency.Key, Version: dependency.RequiredVersion}
			sourceShard := producerShard[identity]
			if sourceShard != "" && consumerShard != "" && sourceShard != consumerShard {
				consumers[identity]++
			}
		}
	}
	return consumers
}

func metaTrackAsyncQueueDepthV640(publisherState *metaTrackAsyncVersionPublisherV640) int64 {
	var depth int64
	for _, lane := range publisherState.lanes {
		depth += int64(len(lane.finals))
	}
	return depth
}

func (r *NodeRuntime) enqueueMetaTrackAsyncFinalV640(block realblock.Block, buffer *metaTrackVersionPublishBuffer, task metaTrackAsyncVersionTaskV640) error {
	if buffer == nil || buffer.asyncV640 == nil {
		return fmt.Errorf("metatrack v6.4 async version publisher is not initialized")
	}
	publisherState := buffer.asyncV640
	if task.HomeShard == "" || task.LogicalKey == "" || task.Item.ProducedVersion == 0 {
		return fmt.Errorf("metatrack v6.4 async final-version task is incomplete")
	}
	task.EnqueuedAt = time.Now()

	publisherState.mu.Lock()
	if publisherState.err != nil {
		err := publisherState.err
		publisherState.mu.Unlock()
		return err
	}
	if publisherState.closed {
		publisherState.mu.Unlock()
		return fmt.Errorf("metatrack v6.4 async version publisher already closed")
	}
	lane := publisherState.lanes[task.HomeShard]
	if lane == nil {
		lane = &metaTrackAsyncVersionLaneV640{}
		publisherState.lanes[task.HomeShard] = lane
	}
	lane.finals = append(lane.finals, task)
	publisherState.backgroundFinalEnqueued++
	if depth := metaTrackAsyncQueueDepthV640(publisherState); depth > publisherState.maxQueueDepth {
		publisherState.maxQueueDepth = depth
	}
	startDrain := !lane.draining
	if startDrain {
		lane.draining = true
		publisherState.wg.Add(1)
	}
	publisherState.mu.Unlock()

	if startDrain {
		go r.drainMetaTrackAsyncFinalLaneV640(block, publisherState, task.HomeShard)
	}
	return nil
}

func (r *NodeRuntime) drainMetaTrackAsyncFinalLaneV640(block realblock.Block, publisherState *metaTrackAsyncVersionPublisherV640, homeShard string) {
	defer publisherState.wg.Done()
	for {
		publisherState.mu.Lock()
		lane := publisherState.lanes[homeShard]
		if lane == nil {
			publisherState.mu.Unlock()
			return
		}
		if publisherState.err != nil {
			lane.draining = false
			publisherState.mu.Unlock()
			return
		}
		if len(lane.finals) == 0 {
			lane.draining = false
			publisherState.mu.Unlock()
			return
		}
		tasks := append([]metaTrackAsyncVersionTaskV640(nil), lane.finals...)
		lane.finals = nil
		publisherState.mu.Unlock()

		// MBE_METATRACK_VARIANCE_DIAG_V30_ASYNC
		// Keep the original opportunistic drain semantics unchanged; only capture
		// the batch boundary that the goroutine happened to observe.
		oldestEnqueuedAtV30 := tasks[0].EnqueuedAt
		for _, task := range tasks[1:] {
			if !task.EnqueuedAt.IsZero() && (oldestEnqueuedAtV30.IsZero() || task.EnqueuedAt.Before(oldestEnqueuedAtV30)) {
				oldestEnqueuedAtV30 = task.EnqueuedAt
			}
		}

		// Home consensus later sorts versioned deltas by exact ProducedVersion.
		// Sorting here makes transport deterministic as well and prevents a batch
		// from depending on goroutine completion order.
		sort.SliceStable(tasks, func(i, j int) bool {
			if tasks[i].Item.RoutingOrdinal != tasks[j].Item.RoutingOrdinal {
				return tasks[i].Item.RoutingOrdinal < tasks[j].Item.RoutingOrdinal
			}
			if tasks[i].LogicalKey != tasks[j].LogicalKey {
				return tasks[i].LogicalKey < tasks[j].LogicalKey
			}
			return tasks[i].Item.Key < tasks[j].Item.Key
		})
		items := make([]state.StateKV, 0, len(tasks))
		unqualified := make(map[string]string, len(tasks))
		deltas := make([]execution.TxDelta, 0, len(tasks))
		for _, task := range tasks {
			items = append(items, task.Item)
			unqualified[task.Item.Key] = task.LogicalKey
			deltas = append(deltas, task.Delta)
		}

		started := time.Now()
		err := r.applyRemoteStateDeltaBatch(publisherState.ctx, block, homeShard, items, unqualified, deltas)
		finished := time.Now()
		r.recordMetaTrackAsyncDrainV30(block, homeShard, len(tasks), oldestEnqueuedAtV30, started, finished, err)
		elapsed := finished.Sub(started)

		publisherState.mu.Lock()
		if err != nil {
			if publisherState.err == nil {
				publisherState.err = fmt.Errorf("metatrack v6.4 async final writeback to %s: %w", homeShard, err)
			}
			lane.draining = false
			publisherState.cancel()
			publisherState.mu.Unlock()
			return
		}
		publisherState.batchCount++
		publisherState.batchItemCount += int64(len(tasks))
		publisherState.backgroundFinalWorkMS += elapsed.Milliseconds()
		publisherState.mu.Unlock()
	}
}

func (r *NodeRuntime) joinMetaTrackAsyncVersionsV640(ctx context.Context, buffer *metaTrackVersionPublishBuffer) error {
	if buffer == nil || buffer.asyncV640 == nil {
		return nil
	}
	publisherState := buffer.asyncV640
	publisherState.mu.Lock()
	publisherState.closed = true
	publisherState.mu.Unlock()

	started := time.Now()
	done := make(chan struct{})
	go func() {
		publisherState.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		publisherState.cancel()
		<-done
		return ctx.Err()
	}
	joinMS := time.Since(started).Milliseconds()
	publisherState.cancel()

	publisherState.mu.Lock()
	err := publisherState.err
	backgroundFinal := publisherState.backgroundFinalEnqueued
	backgroundWorkMS := publisherState.backgroundFinalWorkMS
	batchCount := publisherState.batchCount
	batchItemCount := publisherState.batchItemCount
	maxQueueDepth := publisherState.maxQueueDepth
	publisherState.mu.Unlock()

	r.incrementFullLocalityMetric("metatrack_background_final_enqueue_count", backgroundFinal)
	r.incrementFullLocalityMetric("metatrack_background_final_writeback_ms", backgroundWorkMS)
	r.incrementFullLocalityMetric("metatrack_final_join_wait_ms", joinMS)
	r.incrementFullLocalityMetric("metatrack_async_version_writeback_batch_count", batchCount)
	r.incrementFullLocalityMetric("metatrack_async_version_writeback_item_count", batchItemCount)
	r.incrementFullLocalityMetric("metatrack_async_version_writeback_max_queue_depth", maxQueueDepth)
	return err
}

func cancelMetaTrackAsyncVersionsV640(buffer *metaTrackVersionPublishBuffer) {
	if buffer == nil || buffer.asyncV640 == nil {
		return
	}
	buffer.asyncV640.cancel()
}

var _ = tx.StateVersionDependency{}
