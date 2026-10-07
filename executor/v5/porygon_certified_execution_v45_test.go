package v5

import (
	"fmt"
	"testing"

	"metaverse-chainlab/executor/realism/execution"
)

func TestPorygonV45FrozenCertifiedTruthOverridesReplicaLocalExecution(t *testing.T) {
	nodes := porygonV40FixedNodes()
	ctxKey := porygonV392KeyForShard(t, "v45-ctx", 1, 2)
	itxKey := porygonV392KeyForShard(t, "v45-itx", 0, 2)
	origin, executed := porygonV41CertifiedOriginBlock(t, 3, ctxKey, itxKey)
	plan, err := porygonPipelinePlan(origin)
	if err != nil {
		t.Fatal(err)
	}
	certified := map[string]porygonWaveResult{}
	for _, delta := range executed.ExecutionResult.TxDeltas {
		certified[delta.TxID] = porygonWaveResult{Delta: delta}
	}
	truth, err := porygonBuildCertifiedExecutionTruth(origin.BlockHash, origin.Height, plan, plan.Assignments, certified, map[string]string{"s0": "esc-s0", "s1": "esc-s1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(truth.ProposalU) != 1 || truth.ProposalU[0].TxID != "ctx-origin" || truth.FutureProposalHeight != 5 {
		t.Fatalf("unexpected certified future U: %+v", truth)
	}

	var want string
	for i, node := range nodes {
		r := porygonV38TruthRuntime(node.NodeID, node.ExecutionShardID)
		r.plan = Plan{NodeConfigs: nodes}
		state := &porygonPaperRuntimeState{baselineHeight: 0, txs: map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{}}
		pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 3, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
		porygonV38InstallPaperState(r, state, pipeline)

		local := executed
		local.ExecutionResult.TxDeltas = append([]execution.TxDelta(nil), executed.ExecutionResult.TxDeltas...)
		local.ExecutionResult.TxDeltas[0].WriteSet = map[string]string{ctxKey: fmt.Sprintf("replica-local-drift-%d", i)}
		local.PorygonCertifiedExecution = &truth
		porygonV41InstallOriginExecution(r, origin, local, porygonPipelineExecuted)

		rows, err := r.porygonPaperCertifiedUpdatesForProposal(5)
		if err != nil {
			porygonV38CleanupRuntime(r)
			t.Fatalf("node=%s frozen certified U failed: %v", node.NodeID, err)
		}
		digest := stableJSONDigest(rows)
		if i == 0 {
			want = digest
		} else if digest != want {
			porygonV38CleanupRuntime(r)
			t.Fatalf("node=%s replica-local execution drift changed Proposal.U", node.NodeID)
		}
		if rows[0].Updates[0].Value == fmt.Sprintf("replica-local-drift-%d", i) {
			porygonV38CleanupRuntime(r)
			t.Fatalf("node=%s Proposal.U read replica-local Execution instead of frozen truth", node.NodeID)
		}
		porygonV38CleanupRuntime(r)
	}
}

func TestPorygonV45CertifiedTruthExcludesPostExecutionAbandonedCTx(t *testing.T) {
	plan := porygonExecutionPlan{BlockHeight: 3, ExecutionShardCount: 2, Assignments: []porygonTxAssignment{
		{TxID: "ctx-retained", OriginalIndex: 1, ExecutionShard: 0, CrossShard: true, InvolvedShards: []int{0, 1}},
		{TxID: "ctx-abandoned", OriginalIndex: 2, ExecutionShard: 1, CrossShard: true, InvolvedShards: []int{0, 1}},
	}}
	final := append([]porygonTxAssignment(nil), plan.Assignments...)
	final[1].Abandoned = true
	final[1].ConflictReason = "oc_post_execution_ctx_itx_conflict_abandoned"
	certified := map[string]porygonWaveResult{
		"ctx-retained":  {Delta: execution.TxDelta{TxID: "ctx-retained", OriginalIndex: 1, Success: true, WriteSet: map[string]string{"asset:a": "A"}}},
		"ctx-abandoned": {Delta: execution.TxDelta{TxID: "ctx-abandoned", OriginalIndex: 2, Success: true, WriteSet: map[string]string{"asset:b": "B"}}},
	}
	truth, err := porygonBuildCertifiedExecutionTruth("block-h3", 3, plan, final, certified, map[string]string{"s0": "r0", "s1": "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if len(truth.ProposalU) != 1 || truth.ProposalU[0].TxID != "ctx-retained" {
		t.Fatalf("post-execution abandoned CTx leaked into certified U: %+v", truth.ProposalU)
	}
}

func TestPorygonV45BatchSemanticDigestIgnoresLeaderAndVoterIdentity(t *testing.T) {
	base := PorygonESCBatchCertificate{BlockHash: "block", Height: 3, CommitteeEpochDigest: "epoch", LeaderNodeID: "n0", Entries: []PorygonESCBatchCertificateEntry{
		{ExecutionShardID: "s1", ResultDigest: "r1", Voters: []string{"n4", "n5"}},
		{ExecutionShardID: "s0", ResultDigest: "r0", Voters: []string{"n0", "n1"}},
	}}
	other := base
	other.LeaderNodeID = "n1"
	other.Entries = append([]PorygonESCBatchCertificateEntry(nil), base.Entries...)
	other.Entries[0].Voters = []string{"n6", "n7"}
	other.Entries[1].Voters = []string{"n2", "n3"}
	if porygonBatchCertificateSemanticDigest(base) != porygonBatchCertificateSemanticDigest(other) {
		t.Fatal("leader/voter-only certificate variation changed semantic digest")
	}
	other.Entries[0].ResultDigest = "different"
	if porygonBatchCertificateSemanticDigest(base) == porygonBatchCertificateSemanticDigest(other) {
		t.Fatal("different ESC result digest did not change semantic digest")
	}
}

func TestPorygonV45MaintenanceUsesCertifiedFutureUNotReplicaLocalProposalUpdates(t *testing.T) {
	r := porygonV38TruthRuntime("n0", "s0")
	state := &porygonPaperRuntimeState{
		baselineHeight: 0,
		txs:            map[string]*PorygonTxRoundLifecycle{},
		proposalUpdates: map[uint64][]PorygonProposalUpdate{
			5: {{TxID: "replica-local-only", OriginHeight: 3, Kind: "commit"}},
		},
	}
	pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 3, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
	porygonV38InstallPaperState(r, state, pipeline)
	defer porygonV38CleanupRuntime(r)
	if r.porygonPaperMaintenanceReady(5) {
		t.Fatal("replica-local proposalUpdates incorrectly triggered maintenance")
	}
	truth := PorygonCertifiedExecutionTruth{
		Version: porygonCertifiedExecutionTruthVersion, BlockHash: "b3", Height: 3,
		ESCResultDigests: map[string]string{"s0": "r0"}, CommitteeEpochDigest: porygonCommitteeEpochSeed(3, "porygon-global"), FinalAssignmentsDigest: "assignments",
		FutureProposalHeight: 5,
		ProposalU:            []PorygonProposalUpdate{{TxID: "certified", OriginHeight: 3, Kind: "commit", UpdateDigest: "u"}},
	}
	truth.ESCSemanticDigest = porygonCertifiedESCMapSemanticDigest(truth.BlockHash, truth.Height, truth.CommitteeEpochDigest, truth.ESCResultDigests)
	truth.FutureObligations = []PorygonFutureProtocolObligation{{TxID: "certified", Kind: "proposal_u", TargetHeight: 5}}
	truth.ProposalUDigest = porygonProposalUSemanticDigest(truth.ProposalU)
	truth.SemanticDigest = porygonCertifiedExecutionSemanticDigest(truth)
	pipeline.blocks[3] = &porygonPipelineBlock{Execution: BlockExecutionResult{PorygonCertifiedExecution: &truth}}
	if !r.porygonPaperMaintenanceReady(5) {
		t.Fatal("certified future Proposal.U did not trigger maintenance")
	}
}
