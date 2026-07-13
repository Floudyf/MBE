package v5

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestCrossShardEventsAreUniqueByLogicalTxID(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "leader", ShardID: "s0", Leader: true}, crossEventSeen: map[string]bool{}}
	runtime.recordEvent("logical-1", "s0", "s1", "SourceLock", true, "")
	runtime.recordEvent("logical-1", "s0", "s1", "SourceLock", true, "duplicate_validator_copy")
	runtime.recordEvent("logical-1", "s0", "s1", "TargetCommit", true, "")
	runtime.recordEvent("logical-1", "s0", "s1", "TargetCommit", true, "duplicate_validator_copy")
	if len(runtime.lifecycle) != 2 {
		t.Fatalf("expected one event per cross-shard stage, got %d", len(runtime.lifecycle))
	}
}

func TestNonLeaderCannotEmitCrossShardSideEffects(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "validator", ShardID: "s0", Leader: false}, crossEventSeen: map[string]bool{}}
	item := tx.SignedTransaction{TxID: "logical-1", Payload: "v5_cross:s1"}
	runtime.onCommittedTx(context.Background(), item, Relay{})
	if len(runtime.events) != 0 || len(runtime.lifecycle) != 0 {
		t.Fatal("non-leader emitted a cross-shard side effect")
	}
}

func TestRelayDerivedTransactionCannotStartAnotherSourceLock(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "leader", ShardID: "s1", Leader: true}, crossEventSeen: map[string]bool{}}
	item := tx.SignedTransaction{TxID: "relay-tx", SourceKind: "cross_shard_relay", Payload: "v5_cross:s0"}
	runtime.onCommittedTx(context.Background(), item, Relay{})
	if len(runtime.events) != 0 || len(runtime.lifecycle) != 0 {
		t.Fatal("relay-derived transaction started a new SourceLock")
	}
}

func TestCatchUpCommitSuppressesCrossShardSideEffects(t *testing.T) {
	runtime := &NodeRuntime{node: NodePlan{NodeID: "leader", ShardID: "s0", Leader: true}, crossEventSeen: map[string]bool{}}
	item := tx.SignedTransaction{TxID: "logical-1", Payload: "v5_cross:s1"}
	runtime.onCommittedTxWithOrigin(context.Background(), item, Relay{}, CommitOriginCatchUp)
	if len(runtime.events) != 0 || len(runtime.lifecycle) != 0 {
		t.Fatal("catch-up replay emitted cross-shard side effects")
	}
}
