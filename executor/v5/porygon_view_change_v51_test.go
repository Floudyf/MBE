package v5

import (
	"testing"

	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/mempool"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonV51SignedReservationTx(t *testing.T) tx.SignedTransaction {
	t.Helper()
	publicKey, privateKey := tx.DeterministicKeyPair("porygon-v51-reservation")
	item := tx.SignedTransaction{
		LogicalTxID:      "porygon-v51-reservation",
		Sender:           tx.AddressFromPublicKey(publicKey),
		Receiver:         "receiver",
		Nonce:            0,
		Value:            1,
		StateKeys:        []string{"asset:v51"},
		AccessList:       []tx.AccessItem{{Key: "asset:v51", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
		AccessListSchema: "direct_v1",
		AccessListSource: "porygon_v51_test",
	}
	if err := tx.Sign(&item, privateKey); err != nil {
		t.Fatal(err)
	}
	return item
}

func TestPorygonV51DemoteProposalReservationReleasesLocalBitsButKeepsLedger(t *testing.T) {
	pool := mempool.New("n0", "porygon-global", mempool.DefaultPolicy(), nil)
	item := porygonV51SignedReservationTx(t)
	if result := pool.Admit(item); !result.Accepted {
		t.Fatalf("fixture transaction rejected: %+v", result)
	}
	reserved := pool.ReserveReady(1)
	if len(reserved) != 1 || pool.ReservedCount() != 1 {
		t.Fatalf("fixture reservation mismatch: len=%d reserved=%d", len(reserved), pool.ReservedCount())
	}
	runtime := &NodeRuntime{pool: pool, runtimeMetricCounts: map[string]int64{}}
	runtime.porygonRememberProposalReservation("selected-h13", 13, reserved)
	if got := runtime.porygonProposalReservationCount(); got != 1 {
		t.Fatalf("reservation ledger count=%d want=1", got)
	}
	if !runtime.porygonDemoteProposalReservation("selected-h13") {
		t.Fatal("same selected proposal reservation was not demoted")
	}
	if got := pool.ReservedCount(); got != 0 {
		t.Fatalf("former-primary local reservation bits=%d want=0", got)
	}
	if got := runtime.porygonProposalReservationCount(); got != 1 {
		t.Fatalf("demotion dropped selected-proposal ledger: count=%d", got)
	}
	if got := pool.Len(); got != 1 {
		t.Fatalf("demotion removed transaction before durable commit: pool=%d", got)
	}
	runtime.porygonCommitProposalReservation("selected-h13", nil)
	if got := pool.Len(); got != 0 {
		t.Fatalf("durable selected proposal did not remove demoted transaction: pool=%d", got)
	}
	if got := runtime.porygonProposalReservationCount(); got != 0 {
		t.Fatalf("durable selected proposal retained reservation ledger: count=%d", got)
	}
}

func TestPorygonV51SelectedPrepareDeferredStateIsViewSafe(t *testing.T) {
	runtime := &NodeRuntime{}
	defer porygonV51SelectedPrepareStates.Delete(runtime)
	block := pbftUnitBlock()
	block.Height = 13
	block.BlockHash = "selected-h13"
	if err := runtime.porygonV51RememberSelectedPrepare(
		pbftNewViewForV51Test(1, "n1", 13), block,
	); err != nil {
		t.Fatal(err)
	}
	if _, ok, stale := runtime.porygonV51TakeSelectedPrepare(2, 13); ok || !stale {
		t.Fatalf("old-view selected proposal was not discarded: ok=%t stale=%t", ok, stale)
	}
}

func pbftNewViewForV51Test(view uint64, leader string, height uint64) pbft.NewView {
	return pbft.NewView{View: view, LeaderID: leader, Height: height}
}
