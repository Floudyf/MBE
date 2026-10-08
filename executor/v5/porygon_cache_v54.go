package v5

import (
	"context"
	"sync"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

// Serialize only *local cache* handoff, consumption, and expiry. No paper
// certificate, PBFT quorum, consensus ordering, or execution read depends on it.
var porygonV54CacheMu sync.Mutex

// This watchdog is independent of the RunNode proposal ticker: a blocked
// synchronous pre-PBFT candidate path must not prevent local cache expiry.
// Cadence is the configured block interval, not a new consensus timeout.
func (r *NodeRuntime) porygonV54StartLeaseWatch(ctx context.Context, cadence time.Duration) func() {
	if r == nil || r.pool == nil || r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return func() {}
	}
	if cadence <= 0 {
		cadence = time.Second
	}
	watchCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(cadence)
		defer ticker.Stop()
		for {
			select {
			case <-watchCtx.Done():
				return
			case <-ticker.C:
				r.porygonV54SweepLocalLease()
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (r *NodeRuntime) porygonV54SweepLocalLease() {
	if r == nil || r.pool == nil || r.plugins.BlockProducer == nil || r.plugins.BlockProducer.ID() != porygonBlockProducerID {
		return
	}
	// The 5.3 in-flight record already owns its cancellation/generation.
	// Never use strict target-height equality here: Witness(h+1) is allowed
	// to overlap a still-ordering O(h).
	porygonV53PrewitnessRunning(r.pool)
	r.porygonV54SweepCachedLease()
}

func (r *NodeRuntime) porygonV54SweepCachedLease() {
	if r == nil || r.pool == nil {
		return
	}
	porygonV54CacheMu.Lock()
	value, ok := porygonPrewitnessedBatches.Load(r.pool)
	if !ok {
		porygonV54CacheMu.Unlock()
		return
	}
	batch := value.(porygonPrewitnessedBatch)
	next := r.porygonConsensusNextHeight()
	stale := !r.isCurrentLeader() || batch.LeaderID != r.node.NodeID || batch.TargetHeight < next || batch.TargetHeight > next+1
	expired := !batch.StoredAt.IsZero() && time.Since(batch.StoredAt) >= porygonWitnessCollectionTimeout
	if !stale && !expired {
		porygonV54CacheMu.Unlock()
		return
	}
	porygonPrewitnessedBatches.Delete(r.pool)
	porygonV54CacheMu.Unlock()
	if len(batch.Items) != 0 {
		r.pool.ReleaseReserved(batch.Items)
	}
	if expired {
		r.addPorygonRuntimeMetric("porygon_v54_cached_witness_timeout_release_count", 1)
	} else {
		r.addPorygonRuntimeMetric("porygon_v54_cached_witness_stale_release_count", 1)
	}
}

// Repeated PBFT certificates for an already-ordered hash must converge only
// local proposer/flight metadata. Keep the reservation ledger until the normal
// durable-commit path consumes it; no second logical execution or hash swap.
func (r *NodeRuntime) porygonV54ConvergeOrderedDuplicate(block block.Block) {
	if r == nil {
		return
	}
	if r.proposer != nil && r.proposer.NextHeight == block.Height && r.proposer.PreviousHash == block.PreviousHash {
		r.proposer.Confirm(block)
	}
	r.mu.Lock()
	if r.proposalInFlightHash == block.BlockHash {
		r.proposalInFlight = false
		r.proposalInFlightHash = ""
		r.proposalStartedAt = time.Time{}
		r.proposalLastBroadcastAt = time.Time{}
		r.proposalRetransmitCount = 0
		r.proposalWorkUnits.Store(0)
	}
	r.mu.Unlock()
}
