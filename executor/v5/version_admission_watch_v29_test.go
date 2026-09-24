package v5

import (
	"context"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func TestV29StatelessAdmissionWatchDoesNotPollEveryCandidate(t *testing.T) {
	s0, s1, key := statelessVersionAdmissionTestRuntimes(t)
	original := s1.sendToNodeHook
	sends := 0
	s1.sendToNodeHook = func(ctx context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
		sends++
		return original(ctx, nodeID, envelope)
	}
	consumer := statelessVersionAdmissionItem("watch-consumer", "s1", key, 2, 1)
	block := realblock.Block{BlockHash: "watch-block", Height: 1, ShardID: "s1", Timestamp: time.Now().UnixMilli(), TxIDs: []string{consumer.TxID}, TxList: []tx.SignedTransaction{consumer}}

	for attempt := 0; attempt < 2; attempt++ {
		admitted, deferred, err := s1.admitStatelessVersionCandidate(context.Background(), block)
		if err != nil {
			t.Fatal(err)
		}
		if len(admitted.TxList) != 0 || len(deferred) != 1 {
			t.Fatalf("attempt %d should remain deferred: admitted=%d deferred=%d", attempt, len(admitted.TxList), len(deferred))
		}
	}
	if sends != 1 {
		t.Fatalf("not-ready version was polled %d times; want one subscription send", sends)
	}

	s0.publishStateVersion(key, 1, "v1")
	deadline := time.Now().Add(time.Second)
	for {
		s1.mu.Lock()
		ready := s1.stateVersionAdmissionReady[statelessVersionAdmissionToken(key, 1)]
		s1.mu.Unlock()
		if ready {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("version-ready subscription did not wake requester")
		}
		time.Sleep(5 * time.Millisecond)
	}
	admitted, deferred, err := s1.admitStatelessVersionCandidate(context.Background(), block)
	if err != nil {
		t.Fatal(err)
	}
	if len(admitted.TxList) != 1 || len(deferred) != 0 {
		t.Fatalf("woken exact version did not admit transaction: admitted=%d deferred=%d", len(admitted.TxList), len(deferred))
	}
	if sends != 1 {
		t.Fatalf("ready cache triggered an extra network probe: sends=%d", sends)
	}
}

func TestV29MetaTrackPBFTCandidateKeepsOneSignedProjectionWindow(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "a", 1, 1, "plan-1", "s0", 4, 2)
	second := signedMetaTrackBatchTestTx(t, "b", 2, 1, "plan-1", "s0", 4, 2)
	next := signedMetaTrackBatchTestTx(t, "c", 5, 2, "plan-2", "s0", 4, 1)
	selected, deferred, err := selectMetaTrackLivenessSafePBFTProjection([]tx.SignedTransaction{first, second, next}, 10, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].TxID != "a" || selected[1].TxID != "b" {
		t.Fatalf("unexpected selected projection: %#v", selected)
	}
	if len(deferred) != 1 || deferred[0].TxID != "c" {
		t.Fatalf("later signed projection was aggregated into the same PBFT block: %#v", deferred)
	}
}

func TestV29AdmissionWatchRegistersHomeSubscriptionAndWakesOnPublish(t *testing.T) {
	s0, _, key := statelessVersionAdmissionTestRuntimes(t)
	s0.mu.Lock()
	s0.stateFetchResponseTasks = make(chan stateFetchResponseTask, 1)
	s0.mu.Unlock()
	request := StateFetchRequest{
		RequestID: "watch-request", TxID: "consumer", BlockHash: "candidate",
		Key: key, HomeShard: "s0", ExecutionShard: "s1",
		AccessKind:      statelessVersionAdmissionWatchAccessKind,
		RequiredVersion: 1, Versioned: true,
	}
	if err := s0.handleStateFetchRequest(context.Background(), "n-s1", request); err != nil {
		t.Fatal(err)
	}
	s0.mu.Lock()
	pending := s0.stateVersionRemoteSubscriptionCountLocked()
	s0.mu.Unlock()
	if pending != 1 {
		t.Fatalf("home subscription count=%d want=1", pending)
	}
	select {
	case task := <-s0.stateFetchResponseTasks:
		t.Fatalf("watch responded before version materialized: %+v", task.response)
	default:
	}
	s0.publishStateVersion(key, 1, "v1")
	select {
	case task := <-s0.stateFetchResponseTasks:
		if !task.response.Success || task.response.StateVersion != 1 || task.response.Value != "v1" {
			t.Fatalf("unexpected version-ready response: %+v", task.response)
		}
	case <-time.After(time.Second):
		t.Fatal("home subscription did not wake after version publication")
	}
	s0.mu.Lock()
	pending = s0.stateVersionRemoteSubscriptionCountLocked()
	s0.mu.Unlock()
	if pending != 0 {
		t.Fatalf("home subscription leaked after wake: %d", pending)
	}
}
