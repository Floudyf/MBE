package v5

import (
	"errors"
	"fmt"
	"testing"

	"metaverse-chainlab/executor/realism/tx"
)

func TestPorygonV50ShardedExecutionUsesStrictMajority(t *testing.T) {
	cases := map[int]int{1: 1, 2: 2, 3: 2, 4: 3, 5: 3, 6: 4}
	for members, want := range cases {
		if got := porygonShardedExecutionResultThreshold(members); got != want {
			t.Fatalf("members=%d threshold=%d want=%d", members, got, want)
		}
	}
}

func TestPorygonV50ThreeECSlotsRotateByHeight(t *testing.T) {
	want := []int{0, 1, 2, 0, 1, 2, 0}
	for i, expected := range want {
		height := uint64(i + 1)
		if got := porygonV50ECSlotForHeight(height); got != expected {
			t.Fatalf("height=%d slot=%d want=%d", height, got, expected)
		}
	}
}

func TestPorygonV50RecoverySequenceIsCrossRound(t *testing.T) {
	mode, failures, next := porygonV50NextRecoveryStep("commit", 0, 12)
	if mode != "retry" || failures != 0 || next != 13 {
		t.Fatalf("initial failed U got mode=%s failures=%d next=%d", mode, failures, next)
	}
	mode, failures, next = porygonV50NextRecoveryStep("retry", failures, next)
	if mode != "retry" || failures != 1 || next != 14 {
		t.Fatalf("retry1 failure got mode=%s failures=%d next=%d", mode, failures, next)
	}
	mode, failures, next = porygonV50NextRecoveryStep("retry", failures, next)
	if mode != "rollback" || failures != 2 || next != 15 {
		t.Fatalf("retry2 failure got mode=%s failures=%d next=%d", mode, failures, next)
	}
}

func TestPorygonV50ProposalUpdateOrderingKeepsRecoveryKinds(t *testing.T) {
	mk := func(kind string) PorygonProposalUpdate {
		row := PorygonProposalUpdate{
			TxID: "tx", OriginHeight: 3, Kind: kind, InvolvedShards: []int{1},
			Updates: []PorygonStateUpdate{{TxID: "tx", OriginalIndex: 7, Key: "key", Value: kind}},
		}
		row.UpdateDigest = porygonProposalUpdateDigest(row)
		return row
	}
	rows := porygonV50MergeProposalUpdates([]PorygonProposalUpdate{mk("commit")}, []PorygonProposalUpdate{mk("rollback"), mk("retry"), mk("retry")})
	if len(rows) != 3 {
		t.Fatalf("merged rows=%d want=3", len(rows))
	}
	for i, want := range []string{"commit", "retry", "rollback"} {
		if rows[i].Kind != want {
			t.Fatalf("rows[%d].kind=%s want=%s", i, rows[i].Kind, want)
		}
	}
}

func TestPorygonV50RecoveryProposalIsHeightBound(t *testing.T) {
	r := &NodeRuntime{}
	defer porygonV50RecoveryStates.Delete(r)
	row := PorygonProposalUpdate{TxID: "tx", OriginHeight: 2, Kind: "retry", InvolvedShards: []int{0}, Updates: []PorygonStateUpdate{{TxID: "tx", OriginalIndex: 1, Key: "k", Value: "v"}}}
	row.UpdateDigest = porygonProposalUpdateDigest(row)
	state := r.porygonV50RecoveryState()
	state.mu.Lock()
	state.pending["2|tx|s0"] = &porygonV50RecoveryRecord{TxID: "tx", OriginHeight: 2, PartitionID: "s0", Mode: "retry", NextProposalHeight: 5, RetryUpdate: row}
	state.mu.Unlock()
	if got := r.porygonV50RecoveryProposalUpdates(4); len(got) != 0 {
		t.Fatalf("recovery leaked into wrong height: %+v", got)
	}
	got := r.porygonV50RecoveryProposalUpdates(5)
	if len(got) != 1 || got[0].Kind != "retry" || got[0].TxID != "tx" {
		t.Fatalf("missing height-bound retry: %+v", got)
	}
}

func TestPorygonV50DeferredCommitSkipsDurableWriteButRollbackDoesNot(t *testing.T) {
	key := ""
	for i := 0; i < 10000; i++ {
		candidate := fmt.Sprintf("v50-key-%d", i)
		if porygonStateShard(candidate, 2) == 0 {
			key = candidate
			break
		}
	}
	if key == "" {
		t.Fatal("could not find shard-0 key")
	}
	mk := func(kind, value string) PorygonProposalUpdate {
		row := PorygonProposalUpdate{TxID: "ctx", OriginHeight: 1, Kind: kind, InvolvedShards: []int{0}, Updates: []PorygonStateUpdate{{TxID: "ctx", OriginalIndex: 0, Key: key, Value: value}}}
		row.UpdateDigest = porygonProposalUpdateDigest(row)
		return row
	}
	proposal := PorygonProposalBody{U: []PorygonProposalUpdate{mk("commit", "new")}}
	got, err := porygonPaperDurableStateDeltaV50(proposal, nil, nil, "porygon-global", "s0", 2, map[string]bool{"s0": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("deferred commit materialized unexpectedly: %+v", got)
	}
	proposal.U = []PorygonProposalUpdate{mk("rollback", "old")}
	got, err = porygonPaperDurableStateDeltaV50(proposal, nil, nil, "porygon-global", "s0", 2, map[string]bool{"s0": true})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Value != "old" {
		t.Fatalf("rollback was incorrectly deferred: %+v", got)
	}
}

func TestPorygonV50RetryableRootFailureIsNarrow(t *testing.T) {
	for _, err := range []error{errors.New("quorum timeout"), errors.New("replica not ready"), errors.New("context deadline exceeded")} {
		if !porygonV50RetryablePaperRootFailure(err) {
			t.Fatalf("expected retryable: %v", err)
		}
	}
	if porygonV50RetryablePaperRootFailure(errors.New("partition root mismatch")) {
		t.Fatal("deterministic mismatch must fail closed, not enter future retry")
	}
}

func TestPorygonV503UncommittedITxProtectsFollowingProposal(t *testing.T) {
	runtime := &NodeRuntime{}
	defer porygonPaperRuntimeStates.Delete(runtime)
	defer porygonV50RecoveryStates.Delete(runtime)
	state := &porygonPaperRuntimeState{txs: map[string]*PorygonTxRoundLifecycle{
		"itx-h2": {
			TxID: "itx-h2", OriginHeight: 2, CrossShard: false, Status: porygonPaperTxITxExecuted,
			CommitProposalHeight: 4,
			Accesses:             []tx.AccessItem{{Key: "m.federation/pools/magor.world", Mode: tx.AccessReadWrite}},
			LockedKeys:           []string{"m.federation/pools/magor.world"},
		},
	}}
	porygonPaperRuntimeStates.Store(runtime, state)
	b3 := runtime.porygonPaperPendingEvidenceForProposal(3)
	if len(b3) != 1 || b3[0].TxID != "itx-h2" {
		t.Fatalf("B3 did not protect uncommitted H2 ITx: %#v", b3)
	}
	b4 := runtime.porygonPaperPendingEvidenceForProposal(4)
	if len(b4) != 0 {
		t.Fatalf("B4 retained H2 ITx after its deterministic commit proposal: %#v", b4)
	}
}
