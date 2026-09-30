package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func withMetaTrackV6566StateDependency(t *testing.T, item tx.SignedTransaction, key string, mode tx.AccessMode, required, produced uint64, localExecPred []uint64, depth int) tx.SignedTransaction {
	t.Helper()
	item.StateKeys = []string{key}
	item.AccessList = []tx.AccessItem{{Key: key, Mode: mode, UpdateSemantics: "set"}}
	routing := *item.ExecutionRouting
	routing.StateVersions = []tx.StateVersionDependency{{Key: key, RequiredVersion: required, ProducedVersion: produced}}
	routing.ConsensusExecutionPredecessorOrdinals = append([]uint64(nil), localExecPred...)
	routing.ConsensusOrderingPredecessorOrdinals = nil
	routing.ConsensusExecutionDepth = depth
	routing.RouteEntryDigest = ""
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	if err := tx.ValidateExecutionRouting(item); err != nil {
		t.Fatal(err)
	}
	return item
}

func txIDsV6566(items []tx.SignedTransaction) []string {
	out := make([]string, 0, len(items))
	for _, item := range items {
		out = append(out, item.TxID)
	}
	return out
}

func TestMetaTrackV6566DefersRemoteExactConsumerButKeepsIndependentPacking(t *testing.T) {
	root := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "root", 201, 3, "p3", "s0", 100, 50), nil, nil, 1)
	future := withMetaTrackV6566StateDependency(t, signedMetaTrackBatchTestTx(t, "future-497", 498, 5, "p5", "s0", 100, 50), "neri.world", tx.AccessReadWrite, 497, 498, nil, 1)
	independent := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "independent", 499, 5, "p5", "s0", 100, 50), nil, nil, 1)

	readyCalls := 0
	selected, deferred, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566(
		[]tx.SignedTransaction{root, future, independent}, 1000, "s0", nil,
		func(_ tx.SignedTransaction, dep tx.StateVersionDependency) bool {
			readyCalls++
			return dep.RequiredVersion != 497
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if got := txIDsV6566(selected); len(got) != 2 || got[0] != "root" || got[1] != "independent" {
		t.Fatalf("unexpected selected frontier: %v", got)
	}
	if got := txIDsV6566(deferred); len(got) != 1 || got[0] != "future-497" {
		t.Fatalf("unexpected deferred set: %v", got)
	}
	if readyCalls != 1 {
		t.Fatalf("expected one external exact readiness check, got %d", readyCalls)
	}
}

func TestMetaTrackV6566RemoteConsumerBecomesEligibleAfterVersionObserved(t *testing.T) {
	future := withMetaTrackV6566StateDependency(t, signedMetaTrackBatchTestTx(t, "future-497", 498, 5, "p5", "s0", 100, 50), "neri.world", tx.AccessRead, 497, 498, nil, 1)
	ready := false
	check := func(_ tx.SignedTransaction, _ tx.StateVersionDependency) bool { return ready }
	if _, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566([]tx.SignedTransaction{future}, 1000, "s0", nil, check); err == nil {
		t.Fatal("unpublished remote exact predecessor must keep consumer out of PBFT")
	}
	ready = true
	selected, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566([]tx.SignedTransaction{future}, 1000, "s0", nil, check)
	if err != nil || len(selected) != 1 || selected[0].TxID != "future-497" {
		t.Fatalf("consumer did not become eligible after exact version readiness: selected=%v err=%v", txIDsV6566(selected), err)
	}
}

func TestMetaTrackV6566SameShardExactPredecessorUsesSignedLocalClosureNotRemoteGate(t *testing.T) {
	producer := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "producer", 320, 4, "p4", "s0", 100, 50), nil, nil, 1)
	consumer := withMetaTrackV6566StateDependency(t, signedMetaTrackBatchTestTx(t, "consumer", 323, 4, "p4", "s0", 100, 50), "shared", tx.AccessReadWrite, 320, 323, []uint64{320}, 2)
	called := false
	selected, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566(
		[]tx.SignedTransaction{producer, consumer}, 1000, "s0", nil,
		func(_ tx.SignedTransaction, _ tx.StateVersionDependency) bool { called = true; return false },
	)
	if err != nil || len(selected) != 2 {
		t.Fatalf("same-shard exact chain should remain a local transaction-frontier edge: selected=%v err=%v", txIDsV6566(selected), err)
	}
	if called {
		t.Fatal("same-shard exact predecessor unexpectedly consulted remote readiness")
	}
}

func TestMetaTrackV6566IncidentShapeCannotCreateCrossShardFutureBlockWait(t *testing.T) {
	// Reproduces the essential shape from the stalled real run:
	// s0 has current work plus ordinal 498 whose exact predecessor 497 is remote;
	// s1 has current work whose exact predecessor 322 is remote. Neither consumer
	// may enter PBFT until its remote version is actually published.
	s0Current := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "s0-current", 250, 3, "p3", "s0", 100, 50), nil, nil, 1)
	s0Future := withMetaTrackV6566StateDependency(t, signedMetaTrackBatchTestTx(t, "s0-future", 498, 5, "p5", "s0", 100, 50), "neri.world", tx.AccessReadWrite, 497, 498, nil, 1)
	s1Consumer := withMetaTrackV6566StateDependency(t, signedMetaTrackBatchTestTx(t, "s1-consumer", 323, 4, "p4", "s1", 100, 50), "shared-322", tx.AccessRead, 322, 323, nil, 1)
	s1Root := withMetaTrackV656Frontier(t, signedMetaTrackBatchTestTx(t, "s1-root", 340, 4, "p4", "s1", 100, 50), nil, nil, 1)

	noneReady := func(_ tx.SignedTransaction, _ tx.StateVersionDependency) bool { return false }
	s0Selected, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566([]tx.SignedTransaction{s0Current, s0Future}, 1000, "s0", nil, noneReady)
	if err != nil || len(s0Selected) != 1 || s0Selected[0].TxID != "s0-current" {
		t.Fatalf("s0 future consumer leaked into current block: selected=%v err=%v", txIDsV6566(s0Selected), err)
	}
	s1Selected, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566([]tx.SignedTransaction{s1Consumer, s1Root}, 1000, "s1", nil, noneReady)
	if err != nil || len(s1Selected) != 1 || s1Selected[0].TxID != "s1-root" {
		t.Fatalf("s1 remote consumer leaked into current block: selected=%v err=%v", txIDsV6566(s1Selected), err)
	}
}

func TestMetaTrackV6566MissingRoutingFailsClosedWithoutPanic(t *testing.T) {
	item := tx.SignedTransaction{TxID: "missing-routing"}
	if _, _, _, err := selectMetaTrackRemoteReadyTransactionFrontierV6566([]tx.SignedTransaction{item}, 1000, "s0", nil, nil); err == nil {
		t.Fatal("missing signed execution routing must fail closed")
	}
}

func TestMetaTrackV6566ReadyResponseIdentityFailsClosed(t *testing.T) {
	requestID := metaTrackRemoteReadyRequestIDV6566("n0", "s0", "s1", "asset", 77)
	valid := StateFetchResponse{RequestID: requestID, BlockHash: metaTrackRemoteReadyPolicyV6566, Key: "asset", HomeShard: "s1", ExecutionShard: "s0", StateVersion: 77, Versioned: true, Success: true}
	if !metaTrackRemoteReadyResponseValidV6566("n0", "s0", "s1", valid) {
		t.Fatal("valid remote-ready evidence was rejected")
	}
	invalid := valid
	invalid.BlockHash = "wrong-policy"
	if metaTrackRemoteReadyResponseValidV6566("n0", "s0", "s1", invalid) {
		t.Fatal("wrong-policy readiness evidence was accepted")
	}
	invalid = valid
	invalid.HomeShard = "s0"
	if metaTrackRemoteReadyResponseValidV6566("n0", "s0", "s1", invalid) {
		t.Fatal("wrong-home readiness evidence was accepted")
	}
}
