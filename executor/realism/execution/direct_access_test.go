package execution

import (
	"strings"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func universalDirectAccessTransaction() tx.SignedTransaction {
	return tx.SignedTransaction{
		TxID:       "universal-direct-access-tx-1",
		Sender:     "dataset-sender",
		Receiver:   "dataset-receiver",
		Nonce:      0,
		Value:      1,
		Payload:    "dataset_event:alien_worlds_mine",
		Timestamp:  1,
		SourceKind: "canonical_trace_replay",
		StateKeys: []string{
			"world/read",
			"world/rmw",
			"world/write",
		},
		AccessListSchema: "alien_worlds_static_rmw_v1",
		AccessListSource: "real_template_controlled_rw",
		AccessList: []tx.AccessItem{
			{Key: "world/read", Mode: tx.AccessRead, UpdateSemantics: "alien_worlds_contract_semantic_state"},
			{Key: "world/rmw", Mode: tx.AccessReadWrite, UpdateSemantics: "alien_worlds_contract_semantic_state"},
			{Key: "world/write", Mode: tx.AccessWrite, UpdateSemantics: "alien_worlds_contract_semantic_state"},
		},
	}
}

func assertUniversalDirectAccessResult(t *testing.T, result Result) {
	t.Helper()
	if result.SuccessfulTxs != 1 || result.FailedTxs != 0 || len(result.Receipts) != 1 || !result.Receipts[0].Success {
		t.Fatalf("direct access transaction did not succeed: %+v", result)
	}
	if result.StateUpdates["s0::world/read"] != "seed-read" {
		t.Fatalf("read-only state changed or disappeared: %#v", result.StateUpdates)
	}
	if result.StateUpdates["s0::world/rmw"] == "" || result.StateUpdates["s0::world/rmw"] == "seed-rmw" {
		t.Fatalf("read_write key was not updated: %#v", result.StateUpdates)
	}
	if result.StateUpdates["s0::world/write"] == "" {
		t.Fatalf("write key was not updated: %#v", result.StateUpdates)
	}
	for key := range result.StateUpdates {
		if strings.Contains(key, "balance:dataset-") || strings.Contains(key, "nonce:dataset-") || strings.Contains(key, "groundhog:replay:dataset-") {
			t.Fatalf("direct dataset access created transfer/replay state %q: %#v", key, result.StateUpdates)
		}
	}
}

func TestUniversalDirectAccessRecordRunsAcrossExecutionMethods(t *testing.T) {
	item := universalDirectAccessTransaction()
	b := blockForExecutionTest([]tx.SignedTransaction{item})
	base := map[string]string{
		"s0::world/read": "seed-read",
		"s0::world/rmw":  "seed-rmw",
	}

	serial := NewSerialExecutor().ExecuteBlock(b, base)
	assertUniversalDirectAccessResult(t, serial)

	blockSTM, err := NewBlockSTMExecutor(2).ExecuteBlock(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}
	assertUniversalDirectAccessResult(t, blockSTM)
	if blockSTM.StateRootAfter != serial.StateRootAfter || blockSTM.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("block-stm direct access execution diverged from serial\nserial=%+v\nblock-stm=%+v", serial, blockSTM)
	}

	aria, err := NewAriaExecutor(2).ExecuteBlock(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}
	assertUniversalDirectAccessResult(t, aria)
	if aria.StateRootAfter != serial.StateRootAfter || aria.ReceiptRoot != serial.ReceiptRoot {
		t.Fatalf("aria direct access execution diverged from serial\nserial=%+v\naria=%+v", serial, aria)
	}

	groundhog, err := NewGroundhogExecutor(2).ExecuteBlock(testContext(t), b, base)
	if err != nil {
		t.Fatal(err)
	}
	assertUniversalDirectAccessResult(t, groundhog)
}
