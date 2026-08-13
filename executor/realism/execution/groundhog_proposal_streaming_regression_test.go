package execution

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestGroundhogProposalStopsDrawingWhenBlockIsFull(t *testing.T) {
	executor := NewGroundhogExecutor(4)
	candidates := make([]tx.SignedTransaction, 10)
	for i := range candidates {
		id := string(rune('a' + i))
		candidates[i] = tx.SignedTransaction{
			TxID:             id,
			AccessListSchema: "direct_v1",
			AccessListSource: "fixture",
			AccessList:       []tx.AccessItem{{Key: "key:" + id, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		}
	}
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 1, candidates, map[string]string{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 3 {
		t.Fatalf("selected=%d want=3", len(selection.Selected))
	}
	if selection.Metrics.ExecutionAttemptCount != 3 {
		t.Fatalf("executed candidates=%d want=3; proposal must stop drawing once B is full", selection.Metrics.ExecutionAttemptCount)
	}
	if len(selection.Deferred) != 7 {
		t.Fatalf("deferred=%d want=7", len(selection.Deferred))
	}
	for _, item := range selection.Deferred {
		if selection.DeferredReasons[item.TxID] != "groundhog_candidate_limit" {
			t.Fatalf("untouched candidate %s reason=%q", item.TxID, selection.DeferredReasons[item.TxID])
		}
	}
	if selection.Metrics.ReservationEngine != "object_key_parallel_streaming_proposal_reserve_revert_commit" {
		t.Fatalf("reservation_engine=%q", selection.Metrics.ReservationEngine)
	}
}

func TestGroundhogProposalContinuesStreamAfterConstraintConflict(t *testing.T) {
	executor := NewGroundhogExecutor(2)
	write := func(id, key string) tx.SignedTransaction {
		return tx.SignedTransaction{
			TxID:             id,
			AccessListSchema: "direct_v1",
			AccessListSource: "fixture",
			AccessList:       []tx.AccessItem{{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"}},
		}
	}
	candidates := []tx.SignedTransaction{
		write("hot-a", "hot"),
		write("hot-b", "hot"),
		write("cold-a", "cold:a"),
		write("cold-b", "cold:b"),
	}
	selection, err := executor.SelectCandidateTransactions(context.Background(), "s0", 2, candidates, map[string]string{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Selected) != 3 {
		t.Fatalf("selected=%d want=3; selector must continue drawing after one conflicting candidate", len(selection.Selected))
	}
	if selection.Metrics.ExecutionAttemptCount != 4 {
		t.Fatalf("execution_attempt_count=%d want=4", selection.Metrics.ExecutionAttemptCount)
	}
	if selection.Metrics.ConstraintConflictCount != 1 || len(selection.Deferred) != 1 {
		t.Fatalf("conflicts=%d deferred=%d want=1/1", selection.Metrics.ConstraintConflictCount, len(selection.Deferred))
	}
}
