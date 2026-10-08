package v5

import (
	"context"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

// Reuse the existing Witness threshold wait bound as the complete lifetime
// bound for the local cross-batch prewitness optimization. This is not a new
// protocol timeout: it closes a liveness hole where request send / local
// bookkeeping could outlive the existing five-second Witness wait forever.
const porygonWitnessCollectionTimeout = 5 * time.Second

type porygonPrewitnessInFlightRecord struct {
	mu           sync.Mutex
	runtime      *NodeRuntime
	started      time.Time
	targetHeight uint64
	leaderID     string
	cancel       context.CancelFunc
	items        []tx.SignedTransaction
	settled      bool
}

func porygonV53TryBeginPrewitness(r *NodeRuntime, ctx context.Context, currentHeight uint64) (*porygonPrewitnessInFlightRecord, context.Context, bool) {
	if r == nil || r.pool == nil {
		return nil, nil, false
	}
	witnessCtx, cancel := context.WithCancel(ctx)
	record := &porygonPrewitnessInFlightRecord{
		runtime:      r,
		started:      time.Now(),
		targetHeight: currentHeight + 1,
		leaderID:     r.node.NodeID,
		cancel:       cancel,
	}
	if _, loaded := porygonPrewitnessInFlight.LoadOrStore(r.pool, record); loaded {
		cancel()
		return nil, nil, false
	}
	return record, witnessCtx, true
}

func porygonV53AttachPrewitnessItems(record *porygonPrewitnessInFlightRecord, items []tx.SignedTransaction) bool {
	if record == nil {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.settled {
		return false
	}
	record.items = porygonCloneTransactions(items)
	return true
}

func porygonV53ReleasePrewitness(pool *mempool.Mempool, record *porygonPrewitnessInFlightRecord, metric string) bool {
	if pool == nil || record == nil {
		return false
	}
	record.mu.Lock()
	if record.settled {
		record.mu.Unlock()
		return false
	}
	record.settled = true
	items := porygonCloneTransactions(record.items)
	cancel := record.cancel
	runtime := record.runtime
	record.mu.Unlock()
	porygonPrewitnessInFlight.CompareAndDelete(pool, record)
	if cancel != nil {
		cancel()
	}
	if len(items) > 0 {
		pool.ReleaseReserved(items)
	}
	if runtime != nil && metric != "" {
		runtime.addPorygonRuntimeMetric(metric, 1)
	}
	return true
}

// Successful handoff is atomic from the producer's perspective: publish the
// cached batch before clearing in-flight. No bookkeeping after this point may
// keep ShouldProduce suppressed.
func porygonV53CompletePrewitness(pool *mempool.Mempool, record *porygonPrewitnessInFlightRecord, batch porygonPrewitnessedBatch) bool {
	if pool == nil || record == nil || len(batch.Items) == 0 {
		return false
	}
	record.mu.Lock()
	if record.settled {
		record.mu.Unlock()
		return false
	}
	current, ok := porygonPrewitnessInFlight.Load(pool)
	if !ok || current != record {
		record.mu.Unlock()
		return false
	}
	// A completed Witness batch owns a local reservation until it is consumed.
	// Never overwrite an older cached owner and strand its reserved items.
	porygonV54CacheMu.Lock()
	if _, already := porygonPrewitnessedBatches.Load(pool); already {
		porygonV54CacheMu.Unlock()
		record.mu.Unlock()
		porygonV53ReleasePrewitness(pool, record, "porygon_v54_duplicate_cache_handoff_release_count")
		return false
	}
	record.settled = true
	cancel := record.cancel
	batch.StoredAt = time.Now()
	porygonPrewitnessedBatches.Store(pool, batch)
	porygonV54CacheMu.Unlock()
	record.mu.Unlock()
	porygonPrewitnessInFlight.CompareAndDelete(pool, record)
	if cancel != nil {
		cancel()
	}
	return true
}

func porygonV53PrewitnessRunning(pool *mempool.Mempool) bool {
	if pool == nil {
		return false
	}
	value, ok := porygonPrewitnessInFlight.Load(pool)
	if !ok {
		return false
	}
	record, ok := value.(*porygonPrewitnessInFlightRecord)
	if !ok || record == nil {
		// A bool entry can only come from an already-running pre-v5.3 binary.
		// It has no cancellable ownership identity, so fail open rather than let
		// an obsolete local optimization suppress block production forever.
		porygonPrewitnessInFlight.Delete(pool)
		return false
	}
	record.mu.Lock()
	if record.settled {
		record.mu.Unlock()
		porygonPrewitnessInFlight.CompareAndDelete(pool, record)
		return false
	}
	if time.Since(record.started) < porygonWitnessCollectionTimeout {
		record.mu.Unlock()
		return true
	}
	record.settled = true
	items := porygonCloneTransactions(record.items)
	cancel := record.cancel
	runtime := record.runtime
	record.mu.Unlock()
	porygonPrewitnessInFlight.CompareAndDelete(pool, record)
	if cancel != nil {
		cancel()
	}
	if len(items) > 0 {
		pool.ReleaseReserved(items)
	}
	if runtime != nil {
		runtime.addPorygonRuntimeMetric("porygon_cross_batch_witness_timeout_release_count", 1)
	}
	return false
}

func (r *NodeRuntime) porygonV53ReleaseStalePrewitnessInFlight() {
	if r == nil || r.pool == nil {
		return
	}
	value, ok := porygonPrewitnessInFlight.Load(r.pool)
	if !ok {
		return
	}
	record, ok := value.(*porygonPrewitnessInFlightRecord)
	if !ok || record == nil {
		porygonPrewitnessInFlight.Delete(r.pool)
		return
	}
	record.mu.Lock()
	leaderID := record.leaderID
	targetHeight := record.targetHeight
	record.mu.Unlock()
	stale := leaderID == "" || leaderID != r.node.NodeID || !r.isCurrentLeader() || targetHeight != r.porygonConsensusNextHeight()
	if stale {
		porygonV53ReleasePrewitness(r.pool, record, "porygon_cross_batch_witness_stale_inflight_release_count")
	}
}
