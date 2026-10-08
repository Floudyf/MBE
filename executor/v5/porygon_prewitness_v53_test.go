package v5

import (
	"context"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/account"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonV53TestReserved(t *testing.T) (*mempool.Mempool, []tx.SignedTransaction) {
	t.Helper()
	pool := mempool.New("n0", "porygon-global", mempool.DefaultPolicy(), account.NewNonceManager())
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 1, Sender: "pory-v53", Receiver: "receiver", StartNonce: 0, Value: 1, Seed: "pory-v53", SourceKind: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if got := pool.Admit(items[0]); !got.Accepted {
		t.Fatalf("admit failed: %+v", got)
	}
	reserved := pool.ReserveReady(1)
	if len(reserved) != 1 || pool.ReservedCount() != 1 {
		t.Fatalf("reservation setup failed: reserved=%d count=%d", len(reserved), pool.ReservedCount())
	}
	return pool, reserved
}

func TestPorygonV53StaleCrossBatchWitnessCannotSuppressNextProposal(t *testing.T) {
	pool, reserved := porygonV53TestReserved(t)
	_, cancel := context.WithCancel(context.Background())
	runtime := &NodeRuntime{runtimeMetricCounts: map[string]int64{}}
	record := &porygonPrewitnessInFlightRecord{runtime: runtime, started: time.Now().Add(-porygonWitnessCollectionTimeout - time.Second), cancel: cancel, items: reserved}
	porygonPrewitnessInFlight.Store(pool, record)
	producer := porygonBlockProducer{}
	if !producer.ShouldProduce(BlockProductionInput{Pool: pool}) {
		t.Fatal("expired local prewitness still suppressed next proposal")
	}
	if got := pool.ReservedCount(); got != 0 {
		t.Fatalf("expired prewitness reservation leaked: %d", got)
	}
	if _, ok := porygonPrewitnessInFlight.Load(pool); ok {
		t.Fatal("expired prewitness in-flight marker leaked")
	}
	if runtime.runtimeMetricCounts["porygon_cross_batch_witness_timeout_release_count"] != 1 {
		t.Fatalf("timeout release metric missing: %#v", runtime.runtimeMetricCounts)
	}
}

func TestPorygonV53FreshCrossBatchWitnessStillSuppressesDuplicateProposal(t *testing.T) {
	pool, reserved := porygonV53TestReserved(t)
	_, cancel := context.WithCancel(context.Background())
	record := &porygonPrewitnessInFlightRecord{started: time.Now(), cancel: cancel, items: reserved}
	porygonPrewitnessInFlight.Store(pool, record)
	producer := porygonBlockProducer{}
	if producer.ShouldProduce(BlockProductionInput{Pool: pool}) {
		t.Fatal("fresh prewitness allowed duplicate proposal")
	}
	if got := pool.ReservedCount(); got != 1 {
		t.Fatalf("fresh prewitness reservation changed: %d", got)
	}
	porygonV53ReleasePrewitness(pool, record, "")
}

func TestPorygonV53ExpiredGenerationCannotPublishLateCertificate(t *testing.T) {
	pool, reserved := porygonV53TestReserved(t)
	_, cancel := context.WithCancel(context.Background())
	record := &porygonPrewitnessInFlightRecord{started: time.Now().Add(-porygonWitnessCollectionTimeout - time.Second), cancel: cancel, items: reserved}
	porygonPrewitnessInFlight.Store(pool, record)
	if porygonV53PrewitnessRunning(pool) {
		t.Fatal("expired prewitness reported running")
	}
	batch := porygonPrewitnessedBatch{TargetHeight: 13, LeaderID: "n0", Items: reserved, Certificate: PorygonWitnessCertificate{Height: 13}}
	if porygonV53CompletePrewitness(pool, record, batch) {
		t.Fatal("late certificate from expired generation entered cache")
	}
	if _, ok := porygonPrewitnessedBatches.Load(pool); ok {
		t.Fatal("expired generation polluted prewitness cache")
	}
}

func TestPorygonV53SuccessfulPrewitnessPublishesCacheBeforeClearingInFlight(t *testing.T) {
	pool, reserved := porygonV53TestReserved(t)
	_, cancel := context.WithCancel(context.Background())
	record := &porygonPrewitnessInFlightRecord{started: time.Now(), cancel: cancel, items: reserved}
	porygonPrewitnessInFlight.Store(pool, record)
	batch := porygonPrewitnessedBatch{TargetHeight: 13, LeaderID: "n0", Items: reserved, Certificate: PorygonWitnessCertificate{Height: 13}}
	if !porygonV53CompletePrewitness(pool, record, batch) {
		t.Fatal("successful prewitness did not publish cache")
	}
	if _, ok := porygonPrewitnessInFlight.Load(pool); ok {
		t.Fatal("successful prewitness left in-flight marker")
	}
	if got := pool.ReservedCount(); got != 1 {
		t.Fatalf("cache handoff released reservation too early: %d", got)
	}
	got, ok := porygonTakePrewitnessedBatch(pool, 13, "n0")
	if !ok || len(got.Items) != 1 {
		t.Fatalf("cached batch unavailable: ok=%t items=%d", ok, len(got.Items))
	}
	pool.ReleaseReserved(got.Items)
}
