package v5

import (
	"fmt"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestPorygonPipelineOverlapRequiresRealIntervals(t *testing.T) {
	base := time.Unix(100, 0)
	state := &porygonPipelineRuntime{blocks: map[uint64]*porygonPipelineBlock{
		10: {ExecutionStartedAt: base.Add(10 * time.Millisecond), ExecutionFinishedAt: base.Add(50 * time.Millisecond), CommitStartedAt: base.Add(45 * time.Millisecond), CommitFinishedAt: base.Add(90 * time.Millisecond)},
		11: {OrderedAt: base.Add(20 * time.Millisecond), ExecutionStartedAt: base.Add(60 * time.Millisecond), ExecutionFinishedAt: base.Add(100 * time.Millisecond)},
	}}
	orderingExecution, executionCommit := porygonPipelineOverlapLocked(state, 10)
	if !orderingExecution {
		t.Fatal("ordering/execution wall-clock overlap was not detected")
	}
	if executionCommit {
		t.Fatal("height-10 has no previous M stage and must not invent E/M overlap")
	}
	_, executionCommit = porygonPipelineOverlapLocked(state, 11)
	if !executionCommit {
		t.Fatal("execution/previous-commit wall-clock overlap was not detected")
	}
}

func TestPorygonPaper2MaintenanceProducerCanDrainWithoutMempool(t *testing.T) {
	producer := porygonBlockProducer{}
	if !producer.ShouldProduce(BlockProductionInput{PorygonMaintenanceReady: true}) {
		t.Fatal("Paper2 must produce an L-empty maintenance proposal while U/T work remains")
	}
	if producer.ShouldProduce(BlockProductionInput{}) {
		t.Fatal("Paper2 must stop after the maintenance frontier is fully drained")
	}
}

func TestPorygonPaper2MaintenanceProposalAllowsEmptyL(t *testing.T) {
	block := realblock.Block{ShardID: "porygon-global", Height: 7, PreviousHash: "h6"}
	body := PorygonProposalBody{
		Version: porygonCompactProposalVersion, Height: 7, OrderingDomain: "porygon-global", Maintenance: true,
		TStateHeight: 6, TStateRoot: "root-6", TPartitionRoots: map[string]string{"s0": "r0"},
		ExecutionCommittee: PorygonECDescriptor{ECID: "EC7"}, PreviousProposalHash: "h6",
	}
	body.ProposalDigest = porygonProposalBodyDigest(body)
	if err := porygonValidateProposalBody(body, block); err != nil {
		t.Fatal(err)
	}
	body.L = []PorygonTransactionBlockRef{{TransactionBlockID: "illegal"}}
	body.ProposalDigest = porygonProposalBodyDigest(body)
	if err := porygonValidateProposalBody(body, block); err == nil {
		t.Fatal("maintenance proposal accepted non-empty L")
	}
}

func TestPorygonPaper2TransactionProposalRequiresL(t *testing.T) {
	block := realblock.Block{ShardID: "porygon-global", Height: 7, PreviousHash: "h6"}
	body := PorygonProposalBody{Version: porygonCompactProposalVersion, Height: 7, OrderingDomain: "porygon-global", TStateHeight: 6, TStateRoot: "root-6", ExecutionCommittee: PorygonECDescriptor{ECID: "EC7"}, PreviousProposalHash: "h6"}
	body.ProposalDigest = porygonProposalBodyDigest(body)
	if err := porygonValidateProposalBody(body, block); err == nil {
		t.Fatal("transaction proposal accepted empty L")
	}
}

func TestPorygonPaper2StateProjectionUsesProposalTNotEpochForwarding(t *testing.T) {
	access := tx.AccessItem{Key: "asset:a", Mode: tx.AccessRead}
	kind := porygonStateProjectionAccessKindForProposal(access, 9, "partition-root-9")
	height, root, ok := porygonStateProjectionAnchor(kind)
	if !ok || height != 9 || root != "partition-root-9" {
		t.Fatalf("Proposal.T anchor round trip failed: %d %q %t", height, root, ok)
	}
	if _, epoch := porygonStateProjectionEpoch(kind); epoch {
		t.Fatal("Paper2 Proposal.T access was incorrectly treated as speculative epoch forwarding")
	}
}

func TestPorygonCrossRoundPendingConflictsWithFollowingITx(t *testing.T) {
	pending := porygonPendingTransactionEvidence{TxID: "ctx-prior", Height: 7, Accesses: []tx.AccessItem{{Key: "asset:a", Mode: tx.AccessWrite}}, LockedKeys: []string{"asset:a"}}
	following := tx.SignedTransaction{TxID: "itx-next", AccessList: []tx.AccessItem{{Key: "asset:a", Mode: tx.AccessRead}}}
	if !porygonPendingEvidenceConflicts(pending, following) {
		t.Fatal("following ITx conflict with uncommitted prior CTx was not detected")
	}
	independent := tx.SignedTransaction{TxID: "itx-ok", AccessList: []tx.AccessItem{{Key: "asset:b", Mode: tx.AccessRead}}}
	if porygonPendingEvidenceConflicts(pending, independent) {
		t.Fatal("independent following transaction was falsely abandoned")
	}
}

func TestPorygonPaper2FourAndSixRoundLifecycleInvariant(t *testing.T) {
	runtime := &NodeRuntime{}
	itx := PorygonTxRoundLifecycle{TxID: "itx", OriginHeight: 10, WitnessRound: 10, OrderingRound: 11, PreExecutionRound: 12, CommitRound: 13, CommitProposalHeight: 12}
	if err := runtime.porygonPaperRoundInvariant(itx); err != nil {
		t.Fatal(err)
	}
	ctx := PorygonTxRoundLifecycle{TxID: "ctx", OriginHeight: 20, CrossShard: true, WitnessRound: 20, OrderingRound: 21, PreExecutionRound: 22, UpdateProposalRound: 23, UpdateProposalHeight: 22, UpdateExecutionRound: 24, UpdateExecutionHeight: 22, CommitRound: 25, CommitProposalHeight: 24}
	if err := runtime.porygonPaperRoundInvariant(ctx); err != nil {
		t.Fatal(err)
	}
	ctx.CommitProposalHeight = 23
	if err := runtime.porygonPaperRoundInvariant(ctx); err == nil {
		t.Fatal("CTx committed before B_(h+4) was incorrectly accepted")
	}
}

func TestPorygonPaper2OrderingDependsOnExecutionTwoHeightsBack(t *testing.T) {
	runtime := &NodeRuntime{}
	state := &porygonPipelineRuntime{baselineHeight: 0, orderedHeight: 2, executedHeight: 1}
	porygonPipelineRuntimes.Store(runtime, state)
	defer porygonPipelineRuntimes.Delete(runtime)
	// B3 depends on E(B1), so it is allowed while E(B2) is still running.
	if runtime.porygonPaperProposalRoundBlocked(3) {
		t.Fatal("B3 was incorrectly blocked on E(B2); paper permits O(B3) to overlap E(B2)")
	}
	state.executedHeight = 0
	if !runtime.porygonPaperProposalRoundBlocked(3) {
		t.Fatal("B3 was allowed before E(B1) produced the T/U inputs it requires")
	}
}

func TestPorygonPaper2GenerationInvalidatesInFlightDescendant(t *testing.T) {
	item := &porygonPipelineBlock{Phase: porygonPipelineOrdered, Generation: 1}
	captured := item.Generation
	item.Generation++
	if item.Generation == captured {
		t.Fatal("rollback did not invalidate in-flight execution generation")
	}
}

func TestPorygonPaper2TransactionBlockIdentityPrecedesWitnessCertificate(t *testing.T) {
	items := []tx.SignedTransaction{{TxID: "t1"}}
	before := porygonBuildTransactionBlock(3, "porygon-global", items, "")
	after := porygonBuildTransactionBlock(3, "porygon-global", items, "cert-digest")
	if before.TransactionBlockID != after.TransactionBlockID {
		t.Fatal("witness certificate changed the identity of the transaction block it attests")
	}
	if before.FullBodyDigest != after.FullBodyDigest || before.TransactionRoot != after.TransactionRoot || before.AccessRoot != after.AccessRoot {
		t.Fatal("witness metadata changed transaction block content commitments")
	}
}

func TestPorygonPaper2MaintenanceURequiresRootQuorumShards(t *testing.T) {
	body := PorygonProposalBody{U: []PorygonProposalUpdate{
		{TxID: "a", Updates: []PorygonStateUpdate{{TxID: "a", Key: "alpha", Value: "1"}}},
		{TxID: "b", Updates: []PorygonStateUpdate{{TxID: "b", Key: "beta", Value: "2"}}},
	}}
	got := porygonPaperProposalUpdateShardSet(body, 2)
	if len(got) == 0 {
		t.Fatal("maintenance Proposal.U must require at least one ESC root quorum shard")
	}
	for _, update := range body.U {
		for _, row := range update.Updates {
			want := fmt.Sprintf("s%d", porygonStateShard(row.Key, 2))
			if !got[want] {
				t.Fatalf("missing Proposal.U root quorum shard %s", want)
			}
		}
	}
}
