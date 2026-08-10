package execution

import (
	"reflect"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func paperTx(id string, reads, writes []string) tx.SignedTransaction {
	access := make([]tx.AccessItem, 0, len(reads)+len(writes))
	writeSet := map[string]bool{}
	for _, key := range writes {
		writeSet[key] = true
	}
	for _, key := range reads {
		mode := tx.AccessRead
		if writeSet[key] {
			mode = tx.AccessReadWrite
		}
		access = append(access, tx.AccessItem{Key: key, Mode: mode, UpdateSemantics: "set"})
	}
	for _, key := range writes {
		seen := false
		for _, read := range reads {
			if read == key {
				seen = true
				break
			}
		}
		if !seen {
			access = append(access, tx.AccessItem{Key: key, Mode: tx.AccessWrite, UpdateSemantics: "set"})
		}
	}
	return tx.SignedTransaction{TxID: id, AccessList: access, AccessListSchema: "batch_si_paper_fixture_v1", AccessListSource: "paper_fixture"}
}

func TestBatchSIWRBPFigure7Golden(t *testing.T) {
	items := []tx.SignedTransaction{
		paperTx("T1", []string{"k2"}, []string{"k1", "k4"}),
		paperTx("T2", []string{"k4"}, []string{"k1", "k3"}),
		paperTx("T3", []string{"k3"}, []string{"k2"}),
		paperTx("T4", []string{"k1"}, []string{"k4"}),
		paperTx("T5", []string{"k7"}, []string{"k1", "k2"}),
		paperTx("T6", []string{"k2"}, []string{"k3", "k4"}),
		paperTx("T7", []string{"k1"}, []string{"k2"}),
		paperTx("T8", []string{"k4"}, []string{"k3"}),
	}
	descriptors, _, err := batchSIDescriptors(items, nil, "s0")
	if err != nil {
		t.Fatal(err)
	}
	parts, _ := batchSIWRBPPartition(descriptors)
	got := map[int][]string{}
	for _, part := range parts {
		for _, item := range part.Txs {
			got[part.Number] = append(got[part.Number], item.TxID)
		}
	}
	want := map[int][]string{1: {"T1", "T3", "T8"}, 2: {"T2", "T4", "T7"}, 3: {"T5", "T6"}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Figure 7 WRBP mismatch: got=%v want=%v", got, want)
	}
}

func TestBatchSIOFASFigure9Golden(t *testing.T) {
	items := []tx.SignedTransaction{
		paperTx("T1", []string{"k1", "k6"}, []string{"k2"}),
		paperTx("T2", []string{"k3", "k5"}, []string{"k1"}),
		paperTx("T3", []string{"k3"}, []string{"k3"}),
		paperTx("T4", []string{"k4"}, []string{"k5", "k6"}),
		paperTx("T5", []string{"k2"}, []string{"k4"}),
	}
	descriptors, _, err := batchSIDescriptors(items, nil, "s0")
	if err != nil {
		t.Fatal(err)
	}
	ordered, aborted, _ := batchSIOFASOrder(descriptors, BatchSIPriorityPaper)
	gotOrder := []string{}
	for _, item := range ordered {
		gotOrder = append(gotOrder, item.TxID)
	}
	gotAborted := []string{}
	for _, item := range aborted {
		gotAborted = append(gotAborted, item.TxID)
	}
	if want := []string{"T5", "T1", "T2", "T3"}; !reflect.DeepEqual(gotOrder, want) {
		t.Fatalf("Figure 9 OFAS order mismatch: got=%v want=%v", gotOrder, want)
	}
	if want := []string{"T4"}; !reflect.DeepEqual(gotAborted, want) {
		t.Fatalf("Figure 9 OFAS abort mismatch: got=%v want=%v", gotAborted, want)
	}
}
