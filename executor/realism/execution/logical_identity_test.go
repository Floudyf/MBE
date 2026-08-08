package execution

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func directAccessLogicalIdentityTransaction(t *testing.T, logicalID, routeShard string) tx.SignedTransaction {
	t.Helper()
	publicKey, privateKey := tx.DeterministicKeyPair("direct-access-logical")
	item := tx.SignedTransaction{
		LogicalTxID:      logicalID,
		Sender:           tx.AddressFromPublicKey(publicKey),
		Receiver:         "receiver-direct",
		Nonce:            0,
		Value:            1,
		StateKeys:        []string{"asset:1"},
		AccessList:       []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "stable_rmw"}},
		AccessListSchema: "mbe_workload_record_v3",
		AccessListSource: "dataset_static_access_list",
		Payload:          "dataset-op",
		Timestamp:        10,
	}
	if routeShard != "" {
		routing := tx.ExecutionRoutingMetadata{
			SenderID:        item.Sender,
			ReceiverID:      item.Receiver,
			RoutingEpoch:    1,
			RoutingOrdinal:  1,
			ExecutionShard:  routeShard,
			RoutingReason:   "logical-identity-test",
			RoutePlanDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		}
		digest, err := tx.ComputeExecutionRoutingDigest(item, routing)
		if err != nil {
			t.Fatal(err)
		}
		routing.RouteEntryDigest = digest
		item.ExecutionRouting = &routing
	}
	if err := tx.Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	return item
}

func TestDirectAccessStateUsesLogicalIdentityAcrossPhysicalEnvelopes(t *testing.T) {
	left := directAccessLogicalIdentityTransaction(t, "logical-asset-update", "s0")
	right := directAccessLogicalIdentityTransaction(t, "logical-asset-update", "s1")
	if left.TxID == right.TxID {
		t.Fatal("expected different physical transaction ids")
	}

	executor := NewSerialExecutor()
	leftBlock := block.Block{ShardID: "s0", Height: 1, BlockHash: "left", TxList: []tx.SignedTransaction{left}, TxIDs: []string{left.TxID}}
	rightBlock := block.Block{ShardID: "s0", Height: 1, BlockHash: "right", TxList: []tx.SignedTransaction{right}, TxIDs: []string{right.TxID}}
	leftResult := executor.ExecuteBlock(leftBlock, map[string]string{})
	rightResult := executor.ExecuteBlock(rightBlock, map[string]string{})
	if !reflect.DeepEqual(leftResult.StateDelta, rightResult.StateDelta) {
		t.Fatalf("logical operation produced method-dependent state delta:\nleft=%v\nright=%v", leftResult.StateDelta, rightResult.StateDelta)
	}
	if leftResult.StateRootAfter != rightResult.StateRootAfter {
		t.Fatalf("logical operation produced method-dependent state root: %s != %s", leftResult.StateRootAfter, rightResult.StateRootAfter)
	}
}

func TestGroundhogDirectAccessBytesUseLogicalIdentity(t *testing.T) {
	left := directAccessLogicalIdentityTransaction(t, "logical-groundhog-update", "s0")
	right := directAccessLogicalIdentityTransaction(t, "logical-groundhog-update", "s1")
	executor := NewGroundhogExecutor(1)
	leftAttempt := executor.interpretTransaction(block.Block{ShardID: "s0", Height: 1}, map[string]string{}, 0, left)
	rightAttempt := executor.interpretTransaction(block.Block{ShardID: "s0", Height: 1}, map[string]string{}, 0, right)
	if leftAttempt.TerminalError != "" || rightAttempt.TerminalError != "" {
		t.Fatalf("unexpected groundhog interpretation error: %q %q", leftAttempt.TerminalError, rightAttempt.TerminalError)
	}
	if !reflect.DeepEqual(leftAttempt.Modifications, rightAttempt.Modifications) {
		t.Fatalf("groundhog logical state modifications changed with physical envelope:\nleft=%v\nright=%v", leftAttempt.Modifications, rightAttempt.Modifications)
	}
}
