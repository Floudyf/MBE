package v5

import (
	"context"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

// MBE_OPTME_V22_4_LEGACY_TEST_RETIREMENT
func TestOptMEV22ExitsMethodPreservingExactVersionTransport(t *testing.T) {
	r := &NodeRuntime{plugins: RuntimePlugins{Routing: statelessOptmeRouting{basicPlugin: makeBasic("routing", optmeStatelessRoutingID, nil)}}}
	if r.methodPreservingExactVersionTransportEnabled() {
		t.Fatal("Stateless-OptME v22 must not use the legacy method-preserving exact-version transport")
	}
}

func TestTxAlloMethodPreservingAdmissionUsesSourceLocalExactVersion(t *testing.T) {
	routing := txalloRouting{basicPlugin: makeBasic("routing", txalloStatelessRoutingID, nil), stateless: true}
	r := &NodeRuntime{
		plugins: RuntimePlugins{Routing: routing},
		stateVersionValues: map[string]map[uint64]string{
			"k": {17: "v17"},
		},
		runtimeMetricCounts: map[string]int64{},
	}
	if !r.methodPreservingExactVersionTransportEnabled() {
		t.Fatalf("Stateless-TxAllo exact-version transport unexpectedly disabled: id=%q", routing.ID())
	}
	ready, err := r.probeStatelessVersionAdmissionOnce(context.Background(), realBlockForWriteBehindTest(), statelessVersionAdmissionRequirement{key: "k", version: 17, homeShard: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	if !ready {
		t.Fatal("TxAllo source-local exact predecessor must satisfy method-preserving admission")
	}
	if got := r.runtimeMetricCounts["method_preserving_source_local_version_admission_hit_count"]; got != 1 {
		t.Fatalf("source-local admission metric=%d want 1", got)
	}
}

func TestMethodPreservingHomeReceiptPublishesOnlyContiguousExactVersions(t *testing.T) {
	r := &NodeRuntime{
		pendingStateDeltas: []StateDeltaApplyRequest{
			{Key: "k", Value: "v9", PreviousVersion: 2, ProducedVersion: 9, ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin, TxID: "t9"},
		},
		stateVersionMaterialized: map[string]uint64{"k": 0},
		stateVersionValues:       map[string]map[uint64]string{},
		stateVersionSignals:      map[string]chan struct{}{},
		runtimeMetricCounts:      map[string]int64{},
	}
	r.publishReadyMethodPreservingHomeVersions()
	if _, ready := r.stateVersionValue("k", 9); ready {
		t.Fatal("future Home version became visible before its exact predecessor")
	}
	r.mu.Lock()
	r.pendingStateDeltas = append(r.pendingStateDeltas, StateDeltaApplyRequest{Key: "k", Value: "v2", PreviousVersion: 0, ProducedVersion: 2, ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin, TxID: "t2"})
	r.mu.Unlock()
	r.publishReadyMethodPreservingHomeVersions()
	if value, ready := r.stateVersionValue("k", 2); !ready || value != "v2" {
		t.Fatalf("receipt-visible predecessor missing: value=%q ready=%t", value, ready)
	}
	if value, ready := r.stateVersionValue("k", 9); !ready || value != "v9" {
		t.Fatalf("queued successor was not released after predecessor receipt: value=%q ready=%t", value, ready)
	}
	if got := r.stateVersionMaterialized["k"]; got != 0 {
		t.Fatalf("receipt visibility must not advance durable Home frontier: got=%d", got)
	}
	if got := r.runtimeMetricCounts["method_preserving_home_version_receipt_publish_count"]; got != 2 {
		t.Fatalf("receipt publish metric=%d want 2", got)
	}
}

func TestMethodPreservingHomeDrainKeepsFutureVersionPending(t *testing.T) {
	pending := []StateDeltaApplyRequest{{
		Key: "k", PreviousVersion: 2, ProducedVersion: 3,
		ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin,
	}}
	ready, shouldDrain, blocked := selectRemoteStateDeltasForHomeBlock(pending, map[string]uint64{"k": 1}, 9)
	if len(ready) != 0 || shouldDrain || blocked != 1 {
		t.Fatalf("future version must remain pending without forcing empty height-advance blocks: ready=%d shouldDrain=%t blocked=%d", len(ready), shouldDrain, blocked)
	}
}

func TestMethodPreservingHomeDrainReleasesContinuousChain(t *testing.T) {
	pending := []StateDeltaApplyRequest{
		{Key: "k", PreviousVersion: 2, ProducedVersion: 9, ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin, TxID: "t9"},
		{Key: "k", PreviousVersion: 0, ProducedVersion: 2, ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin, TxID: "t2"},
		{Key: "k", PreviousVersion: 9, ProducedVersion: 15, ApplyOrigin: methodPreservingVersionedRemoteHomeOrigin, TxID: "t15"},
	}
	ready, shouldDrain, blocked := selectRemoteStateDeltasForHomeBlock(pending, map[string]uint64{"k": 0}, 9)
	if !shouldDrain || blocked != 0 || len(ready) != 3 {
		t.Fatalf("continuous version chain was not released: ready=%d shouldDrain=%t blocked=%d", len(ready), shouldDrain, blocked)
	}
	want := []uint64{2, 9, 15}
	for i := range want {
		if ready[i].ProducedVersion != want[i] {
			t.Fatalf("ready[%d] version=%d want %d", i, ready[i].ProducedVersion, want[i])
		}
	}
}

func TestLegacyVersionedHomeDrainSemanticsRemainUnchanged(t *testing.T) {
	legacy := StateDeltaApplyRequest{
		Key: "k", PreviousVersion: 999, ProducedVersion: 1000,
		ApplyOrigin: "versioned_remote_home", TxID: "legacy",
	}
	ready, shouldDrain, blocked := selectRemoteStateDeltasForHomeBlock([]StateDeltaApplyRequest{legacy}, map[string]uint64{"k": 0}, 1)
	if !shouldDrain || blocked != 0 || len(ready) != 1 {
		t.Fatalf("legacy versioned_remote_home readiness changed: ready=%d shouldDrain=%t blocked=%d", len(ready), shouldDrain, blocked)
	}
}

func TestSourceLocalFetchEvidenceIsNotCountedAsPhysicalRemoteOperation(t *testing.T) {
	r := &NodeRuntime{remoteStateRows: [][]string{}, runtimeMetricCounts: map[string]int64{}}
	r.recordRemoteStateAccess(realBlockForWriteBehindTest(), signedTransactionForWriteBehindTest(), accessItemForWriteBehindTest(), StateFetchResponse{SourceLocal: true, Success: true}, 0)
	if len(r.remoteStateRows) != 0 {
		t.Fatalf("source-local forwarding polluted physical remote-state rows: %d", len(r.remoteStateRows))
	}
}

// Keep helper construction local to the regression file so the production API
// surface does not grow for tests.
func realBlockForWriteBehindTest() realblock.Block              { return realblock.Block{} }
func signedTransactionForWriteBehindTest() tx.SignedTransaction { return tx.SignedTransaction{} }
func accessItemForWriteBehindTest() tx.AccessItem               { return tx.AccessItem{} }
