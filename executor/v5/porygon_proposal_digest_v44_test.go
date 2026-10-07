package v5

import (
	"encoding/json"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

func TestPorygonV44ProposalDigestDoesNotMutateProtocolOrder(t *testing.T) {
	body := PorygonProposalBody{
		Version:              porygonCompactProposalVersion,
		Height:               5,
		OrderingDomain:       "porygon-global",
		PreviousProposalHash: "prev",
		TStateRoot:           "root",
		ExecutionCommittee:   PorygonECDescriptor{ECID: "ec"},
		Maintenance:          true,
		L: []PorygonTransactionBlockRef{
			{TransactionBlockID: "z-block"},
			{TransactionBlockID: "a-block"},
		},
		U: []PorygonProposalUpdate{
			{TxID: "z-tx", OriginHeight: 3, Kind: "commit", Updates: []PorygonStateUpdate{{TxID: "z-tx", OriginalIndex: 1, Key: "k/z", Value: "1"}}},
			{TxID: "a-tx", OriginHeight: 3, Kind: "commit", Updates: []PorygonStateUpdate{{TxID: "a-tx", OriginalIndex: 2, Key: "k/a", Value: "2"}}},
		},
	}
	for i := range body.U {
		body.U[i].UpdateDigest = porygonProposalUpdateDigest(body.U[i])
	}
	before, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	_ = porygonProposalBodyDigest(body)
	after, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("ProposalDigest mutated protocol order/body\nbefore=%s\nafter=%s", before, after)
	}
	if body.U[0].TxID != "z-tx" || body.U[1].TxID != "a-tx" {
		t.Fatalf("Proposal.U protocol order changed: %#v", body.U)
	}
	if body.L[0].TransactionBlockID != "z-block" || body.L[1].TransactionBlockID != "a-block" {
		t.Fatalf("Proposal.L order changed: %#v", body.L)
	}
}

func TestPorygonV44ProposalDigestCommitsProtocolUSequence(t *testing.T) {
	u1 := PorygonProposalUpdate{TxID: "z-tx", OriginHeight: 3, Kind: "commit", Updates: []PorygonStateUpdate{{TxID: "z-tx", OriginalIndex: 1, Key: "k/z", Value: "1"}}}
	u2 := PorygonProposalUpdate{TxID: "a-tx", OriginHeight: 3, Kind: "commit", Updates: []PorygonStateUpdate{{TxID: "a-tx", OriginalIndex: 2, Key: "k/a", Value: "2"}}}
	u1.UpdateDigest = porygonProposalUpdateDigest(u1)
	u2.UpdateDigest = porygonProposalUpdateDigest(u2)
	base := PorygonProposalBody{Version: porygonCompactProposalVersion, Height: 5, OrderingDomain: "porygon-global", PreviousProposalHash: "prev", TStateRoot: "root", ExecutionCommittee: PorygonECDescriptor{ECID: "ec"}, Maintenance: true}
	left := base
	left.U = []PorygonProposalUpdate{u1, u2}
	right := base
	right.U = []PorygonProposalUpdate{u2, u1}
	if porygonProposalBodyDigest(left) == porygonProposalBodyDigest(right) {
		t.Fatal("ProposalDigest normalized away semantically meaningful Proposal.U sequence")
	}
}

func porygonV44CertifiedOriginBlock(t *testing.T, height uint64) (realblock.Block, BlockExecutionResult) {
	t.Helper()
	assignments := []porygonTxAssignment{
		{TxID: "z-ctx", OriginalIndex: 1, ExecutionShard: 0, CrossShard: true, InvolvedShards: []int{0, 1}},
		{TxID: "a-ctx", OriginalIndex: 2, ExecutionShard: 1, CrossShard: true, InvolvedShards: []int{0, 1}},
	}
	plan := porygonExecutionPlan{Version: "v44-test", AlgorithmID: porygonPlanAlgorithmID, BlockHeight: height, OrderingDomain: "porygon-global", ExecutionShardCount: 2, Assignments: assignments}
	plan.PlanDigest = stableJSONDigest(porygonPlanDigestProjection(plan))
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "porygon-global", Height: height, PreviousHash: "prev", ProposerID: "n0", ExecutionPlan: &realblock.ExecutionPlanEnvelope{AlgorithmID: porygonPlanAlgorithmID, Payload: payload, PayloadDigest: stableTextDigest(string(payload)), PlanDigest: plan.PlanDigest}}
	realblock.AssignHash(&block)
	result := BlockExecutionResult{ExecutionResult: execution.Result{BlockHash: block.BlockHash, Height: height, TxDeltas: []execution.TxDelta{
		{TxID: "z-ctx", OriginalIndex: 1, WriteSet: map[string]string{"state/z": "1"}, Success: true},
		{TxID: "a-ctx", OriginalIndex: 2, WriteSet: map[string]string{"state/a": "2"}, Success: true},
	}}}
	return block, result
}

func TestPorygonV44Height5MultiUProposalDigestRoundTripPreservesCertifiedOrder(t *testing.T) {
	r := porygonV38TruthRuntime("n7", "s1")
	defer porygonV38CleanupRuntime(r)
	r.plan = Plan{NodeConfigs: porygonV40FixedNodes()}
	origin, executed := porygonV44CertifiedOriginBlock(t, 3)
	porygonV38InstallPaperState(r, &porygonPaperRuntimeState{baselineHeight: 0, txs: map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{}}, &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 3, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}})
	porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineExecuted)

	updates, err := r.porygonPaperCertifiedUpdatesForProposal(5)
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 || updates[0].TxID != "z-ctx" || updates[1].TxID != "a-ctx" {
		t.Fatalf("fixture did not create protocol order opposite lexical order: %#v", updates)
	}
	body := PorygonProposalBody{Version: porygonCompactProposalVersion, Height: 5, OrderingDomain: "porygon-global", U: append([]PorygonProposalUpdate(nil), updates...), TStateRoot: "root", ExecutionCommittee: PorygonECDescriptor{ECID: "ec"}, PreviousProposalHash: "prev", Maintenance: true}
	body.ProposalDigest = porygonProposalBodyDigest(body)
	if body.U[0].TxID != "z-ctx" || body.U[1].TxID != "a-ctx" {
		t.Fatalf("ProposalDigest reordered certified Proposal.U: %#v", body.U)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	var decoded PorygonProposalBody
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := r.porygonPaperVerifyCertifiedProposalU(5, decoded.U); err != nil {
		t.Fatalf("backup rejected round-tripped certified height-5 multi-U sequence: %v", err)
	}
}
