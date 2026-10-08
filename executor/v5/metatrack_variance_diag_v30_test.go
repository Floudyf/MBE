package v5

import (
	"errors"
	"testing"
	"time"

	"metaverse-chainlab/executor/realism/tx"
)

func diagTxV30(id string, seq uint64, expected int) tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID: id,
		ExecutionRouting: &tx.ExecutionRoutingMetadata{
			RouteBatchSequence:              seq,
			RouteBatchShardTransactionCount: expected,
		},
	}
}

func TestMetaTrackVarianceDiagV30FindsCompletePrefixAndIncompleteBatch(t *testing.T) {
	items := []tx.SignedTransaction{
		diagTxV30("a", 10, 2), diagTxV30("b", 10, 2),
		diagTxV30("c", 11, 2),
		diagTxV30("d", 12, 1),
	}
	obs := inspectMetaTrackProposalV30(time.UnixMilli(1234), 7, 4, items)
	if obs.CompleteRouteBatchCount != 1 || obs.CompleteRouteBatchSequences != "10" {
		t.Fatalf("complete prefix mismatch: %+v", obs)
	}
	if obs.FirstIncompleteRouteBatch != 11 || obs.FirstIncompleteHave != 1 || obs.FirstIncompleteExpected != 2 {
		t.Fatalf("incomplete projection mismatch: %+v", obs)
	}
}

func TestMetaTrackVarianceDiagV30RecordsSelectedSequencesAndError(t *testing.T) {
	obs := metaTrackProposalObservationV30{}
	selected := []tx.SignedTransaction{diagTxV30("a", 4, 1), diagTxV30("b", 5, 1), diagTxV30("c", 5, 1)}
	finishMetaTrackProposalV30(&obs, selected, errors.New("probe"))
	if obs.SelectedTransactionCount != 3 || obs.SelectedRouteBatchCount != 2 || obs.SelectedRouteBatchSequences != "4|5" {
		t.Fatalf("selected sequence summary mismatch: %+v", obs)
	}
	if obs.SelectionError != "probe" {
		t.Fatalf("selection error not recorded: %+v", obs)
	}
}
