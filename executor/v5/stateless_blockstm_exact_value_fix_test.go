package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func TestStatelessVersionAdmissionBlindWriteKeepsOrderingButSkipsPredecessorReadiness(t *testing.T) {
	_, executionRuntime, key := statelessVersionAdmissionTestRuntimes(t)
	writer := statelessVersionAdmissionItem("blind-write", "s1", key, 100, 99)
	writer.AccessList[0].Mode = tx.AccessWrite
	block := realblock.Block{
		BlockHash: "blind-write-admission",
		Height:    1,
		ShardID:   "s1",
		Timestamp: time.Now().UnixMilli(),
		TxIDs:     []string{writer.TxID},
		TxList:    []tx.SignedTransaction{writer},
	}

	admitted, deferred, err := executionRuntime.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(deferred) != 0 || len(admitted.TxList) != 1 || admitted.TxList[0].TxID != writer.TxID {
		t.Fatalf("blind write was incorrectly gated on predecessor value: admitted=%#v deferred=%#v", admitted.TxIDs, deferred)
	}
	dependency := writer.ExecutionRouting.StateVersions[0]
	if dependency.RequiredVersion != 99 || dependency.ProducedVersion != 100 {
		t.Fatalf("blind-write ordering metadata changed: %#v", dependency)
	}
}

func TestVersionedStateAccessesBlindWriteDoesNotProbePredecessor(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:             "blind-write-probe",
		AccessListSchema: "mbe_workload_record_v3",
		AccessList:       []tx.AccessItem{{Key: "asset:k", Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RoutingOrdinal: 100,
			ExecutionShard: "s1",
			StateVersions:  []tx.StateVersionDependency{{Key: "asset:k", RequiredVersion: 99, ProducedVersion: 100}},
		},
	}
	home := func(string, []string) string { return "s0" }
	if probes := versionedStateAccesses(item, []string{"s0", "s1"}, home, "s1"); len(probes) != 0 {
		t.Fatalf("blind write unexpectedly probes predecessor value: %#v", probes)
	}

	item.AccessList[0].Mode = tx.AccessReadWrite
	if probes := versionedStateAccesses(item, []string{"s0", "s1"}, home, "s1"); len(probes) != 1 {
		t.Fatalf("read-write access lost exact-version readiness: %#v", probes)
	}
}

func TestBlindWriteOrderingNoopResolvesPredecessorLazily(t *testing.T) {
	homeRuntime, _, key := statelessVersionAdmissionTestRuntimes(t)
	homeRuntime.publishStateVersion(key, 99, "v99")
	writer := statelessVersionAdmissionItem("blind-write-noop", "s0", key, 100, 99)
	writer.AccessList[0].Mode = tx.AccessWrite
	delta := execution.TxDelta{TxID: writer.TxID, Success: false, WriteSet: map[string]string{}}
	wrongSnapshot := map[string]string{qualifyStateKey("s0", key): "wrong-snapshot-value"}
	block := realblock.Block{BlockHash: "blind-write-noop", Height: 1, ShardID: "s0", ProposerID: homeRuntime.node.NodeID}

	if err := homeRuntime.publishTransactionStateVersions(context.Background(), block, writer, delta, wrongSnapshot); err != nil {
		t.Fatal(err)
	}
	value, ok := homeRuntime.stateVersionValue(key, 100)
	if !ok || value != "v99" {
		t.Fatalf("ordering no-op did not lazily alias exact predecessor: value=%q ok=%t", value, ok)
	}
}
