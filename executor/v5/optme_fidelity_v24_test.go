package v5

import (
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestOptMEV24HistoricalUnknownFidelityIsEvidenceOnly(t *testing.T) {
	block := realblock.Block{TxList: []tx.SignedTransaction{{
		AccessList: []tx.AccessItem{
			{Key: "asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "layered_v2_replay_projection_unknown"},
			{Key: "asset:2", Mode: tx.AccessRead, UpdateSemantics: "none"},
		},
	}}}
	beforeMode := block.TxList[0].AccessList[0].Mode
	label, count := optmeInputAccessFidelity(block)
	if label != "historical_static_unknown_promoted_to_rmw_projection" || count != 1 {
		t.Fatalf("label=%q count=%d", label, count)
	}
	if block.TxList[0].AccessList[0].Mode != beforeMode || beforeMode != tx.AccessReadWrite {
		t.Fatalf("fidelity evidence mutated runtime semantics: before=%v after=%v", beforeMode, block.TxList[0].AccessList[0].Mode)
	}
}

func TestOptMEV24DeclaredRWHasNoUnknownProjection(t *testing.T) {
	block := realblock.Block{TxList: []tx.SignedTransaction{{
		AccessList: []tx.AccessItem{{Key: "asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "rmw"}},
	}}}
	label, count := optmeInputAccessFidelity(block)
	if label != "declared_runtime_rw_no_unknown_projection" || count != 0 {
		t.Fatalf("label=%q count=%d", label, count)
	}
}
