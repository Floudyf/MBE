package v5

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/block"
)

func TestPorygonV54CachedWitnessLeaseExpiryReleasesReservedItems(t *testing.T) {
	pool, items := porygonV53TestReserved(t)
	r := pbftUnitRuntime("n0")
	r.pool = pool
	r.committedHeight = 1
	r.runtimeMetricCounts = map[string]int64{}
	batch := porygonPrewitnessedBatch{TargetHeight: 2, LeaderID: r.node.NodeID, Items: items, StoredAt: time.Now().Add(-porygonWitnessCollectionTimeout - time.Second)}
	porygonV54CacheMu.Lock()
	porygonPrewitnessedBatches.Store(pool, batch)
	porygonV54CacheMu.Unlock()
	r.porygonV54SweepCachedLease()
	if _, ok := porygonPrewitnessedBatches.Load(pool); ok {
		t.Fatal("expired cached witness was not evicted")
	}
	if count := pool.ReservedCount(); count != 0 {
		t.Fatalf("expired cached witness still owns reservation: %d", count)
	}
	if r.runtimeMetricCounts["porygon_v54_cached_witness_timeout_release_count"] != 1 {
		t.Fatalf("missing expiry truth metric: %#v", r.runtimeMetricCounts)
	}
}

func TestPorygonV54FreshOverlappingWitnessNotPrematurelyReleased(t *testing.T) {
	pool, items := porygonV53TestReserved(t)
	r := pbftUnitRuntime("n0")
	r.pool = pool
	r.committedHeight = 1 // O(2) not ordered; Witness(3) legitimately overlaps.
	batch := porygonPrewitnessedBatch{TargetHeight: 3, LeaderID: r.node.NodeID, Items: items, StoredAt: time.Now()}
	porygonV54CacheMu.Lock()
	porygonPrewitnessedBatches.Store(pool, batch)
	porygonV54CacheMu.Unlock()
	r.porygonV54SweepCachedLease()
	if count := pool.ReservedCount(); count != 1 {
		t.Fatalf("valid next-height overlap lost reservation: %d", count)
	}
	if _, ok := porygonPrewitnessedBatches.Load(pool); !ok {
		t.Fatal("valid next-height overlap was evicted")
	}
	porygonV54CacheMu.Lock()
	porygonPrewitnessedBatches.Delete(pool)
	porygonV54CacheMu.Unlock()
	pool.ReleaseReserved(items)
}

func TestPorygonV54DuplicateOrderedCertificateConvergesOnlyMatchingFlight(t *testing.T) {
	r := &NodeRuntime{proposalInFlight: true, proposalInFlightHash: "h13"}
	r.porygonV54ConvergeOrderedDuplicate(block.Block{BlockHash: "other"})
	if !r.proposalInFlight || r.proposalInFlightHash != "h13" {
		t.Fatal("unrelated certified digest changed proposal flight")
	}
	r.porygonV54ConvergeOrderedDuplicate(block.Block{BlockHash: "h13"})
	if r.proposalInFlight || r.proposalInFlightHash != "" {
		t.Fatal("same-digest repeated certificate left stale flight")
	}
}

func TestPorygonV54IndependentLeaseWatchSurvivesBlockedProposalTicker(t *testing.T) {
	pool, items := porygonV53TestReserved(t)
	r := pbftUnitRuntime("n0")
	r.pool = pool
	r.plugins.BlockProducer = porygonBlockProducer{basicPlugin: makeBasic("block_producer", porygonBlockProducerID, map[string]any{})}
	if got := r.plugins.BlockProducer.ID(); got != porygonBlockProducerID {
		t.Fatalf("invalid fixture producer ID: got=%q want=%q", got, porygonBlockProducerID)
	}
	r.committedHeight = 1
	batch := porygonPrewitnessedBatch{TargetHeight: 2, LeaderID: "n0", Items: items, StoredAt: time.Now().Add(-porygonWitnessCollectionTimeout - time.Second)}
	porygonV54CacheMu.Lock()
	porygonPrewitnessedBatches.Store(pool, batch)
	porygonV54CacheMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := r.porygonV54StartLeaseWatch(ctx, time.Millisecond)
	defer stop()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		if pool.ReservedCount() == 0 {
			if _, ok := porygonPrewitnessedBatches.Load(pool); ok {
				t.Fatal("watch released reservation but left cached Witness")
			}
			return
		}
		select {
		case <-deadline:
			t.Fatal("independent local cached-Witness lease did not expire")
		case <-ticker.C:
		}
	}
}
