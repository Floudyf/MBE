package v5

import (
	"context"
	"errors"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/p2p"
	"metaverse-chainlab/executor/realism/tx"
)

func signedMetaTrackBatchTestTx(t *testing.T, txID string, ordinal, sequence uint64, planDigest, shard string, globalCount, shardCount int) tx.SignedTransaction {
	t.Helper()
	item := tx.SignedTransaction{TxID: txID, Sender: "sender-" + txID, Receiver: "receiver-" + txID, StateKeys: []string{"k-" + txID}}
	routing := tx.ExecutionRoutingMetadata{SenderID: item.Sender, ReceiverID: item.Receiver, RoutingOrdinal: ordinal, ExecutionShard: shard, RoutingReason: "test_batch_projection", RoutePlanDigest: planDigest, RouteBatchSequence: sequence, RouteBatchTransactionCount: globalCount, RouteBatchShardTransactionCount: shardCount}
	digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
	if err != nil {
		t.Fatal(err)
	}
	routing.RouteEntryDigest = digest
	item.ExecutionRouting = &routing
	return item
}

func TestMetaTrackBatchProjectionSelectorFencesPlanBatchesAndWaitsForCompleteness(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "a", 1, 1, "plan-1", "s0", 4, 2)
	second := signedMetaTrackBatchTestTx(t, "b", 3, 1, "plan-1", "s0", 4, 2)
	next := signedMetaTrackBatchTestTx(t, "c", 5, 2, "plan-2", "s0", 4, 1)
	selected, deferred, err := selectMetaTrackBatchProjection([]tx.SignedTransaction{second, next, first}, 4, "s0")
	if err != nil {
		t.Fatal(err)
	}
	if len(selected) != 2 || selected[0].TxID != "a" || selected[1].TxID != "b" || len(deferred) != 1 || deferred[0].TxID != "c" {
		t.Fatalf("unexpected projection")
	}
	if _, _, err := selectMetaTrackBatchProjection([]tx.SignedTransaction{first}, 4, "s0"); !errors.Is(err, errMetaTrackBatchProjectionIncomplete) {
		t.Fatalf("partial projection must wait, got %v", err)
	}
}

func TestMetaTrackBatchProjectionMetadataIsDigestBound(t *testing.T) {
	item := signedMetaTrackBatchTestTx(t, "bound", 7, 3, "plan-bound", "s1", 10, 4)
	if err := tx.ValidateExecutionRouting(item); err != nil {
		t.Fatal(err)
	}
	item.ExecutionRouting.RouteBatchShardTransactionCount = 5
	if err := tx.ValidateExecutionRouting(item); err == nil {
		t.Fatal("tampering signed shard count did not invalidate digest")
	}
}

func TestMetaTrackBlockProjectionRejectsMixedDigestAndPartialProjection(t *testing.T) {
	first := signedMetaTrackBatchTestTx(t, "a", 1, 1, "plan-1", "s0", 4, 2)
	second := signedMetaTrackBatchTestTx(t, "b", 3, 1, "plan-1", "s0", 4, 2)
	block := realblock.Block{ShardID: "s0", TxList: []tx.SignedTransaction{first, second}}
	if _, err := validateMetaTrackBatchProjection(block, true); err != nil {
		t.Fatal(err)
	}
	partial := block
	partial.TxList = partial.TxList[:1]
	if _, err := validateMetaTrackBatchProjection(partial, true); err == nil {
		t.Fatal("partial projection accepted")
	}
	mixed := block
	mixed.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	mixed.TxList[1] = signedMetaTrackBatchTestTx(t, "b2", 3, 1, "other-plan", "s0", 4, 2)
	if _, err := validateMetaTrackBatchProjection(mixed, true); err == nil {
		t.Fatal("mixed digest accepted")
	}
}

func TestMetaTrackVersionedHomeFetchSubscribesUntilPublication(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	profile := testMetaTrackProfile()
	root := t.TempDir()
	plan := Plan{ExecutionBackend: "real_cluster", NoFallback: true, NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: root + "/n0", Validators: []string{"n0"}, PluginProfile: profile},
		{NodeID: "n1", ShardID: "s1", Role: "leader", Leader: true, ListenAddr: freeLocalAddr(t), DataDir: root + "/n1", Validators: []string{"n1"}, PluginProfile: profile},
	}}
	home, err := newNodeRuntime(plan, plan.NodeConfigs[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := home.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer home.Stop()
	key := keyWithHomeShard(t, "s0", []string{"s0", "s1"})
	responses := make(chan StateFetchResponse, 2)
	home.sendToNodeHook = func(_ context.Context, _ string, message p2p.MessageEnvelope) error {
		if message.MessageType != stateFetchResponseMessage {
			return nil
		}
		response, err := p2p.DecodePayload[StateFetchResponse](message)
		if err != nil {
			return err
		}
		responses <- response
		return nil
	}
	request := StateFetchRequest{RequestID: "sub-17", TxID: "tx-17", BlockHash: "block-17", Key: key, HomeShard: "s0", ExecutionShard: "s1", AccessKind: string(tx.AccessRead), RequiredVersion: 17, Versioned: true}
	if err := home.handleStateFetchRequest(ctx, "n1", request); err != nil {
		t.Fatal(err)
	}
	select {
	case response := <-responses:
		t.Fatalf("not-ready version responded eagerly: %+v", response)
	case <-time.After(80 * time.Millisecond):
	}
	home.publishStateVersion(key, 17, "v17")
	select {
	case response := <-responses:
		if !response.Success || response.Value != "v17" || response.StateVersion != 17 {
			t.Fatalf("unexpected wake: %+v", response)
		}
	case <-ctx.Done():
		t.Fatalf("subscription not woken: %v", ctx.Err())
	}
}
