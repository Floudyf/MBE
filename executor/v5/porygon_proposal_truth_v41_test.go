package v5

import (
	"encoding/json"
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
)

func porygonV41CertifiedOriginBlock(t *testing.T, height uint64, ctxKey, itxKey string) (realblock.Block, BlockExecutionResult) {
	t.Helper()
	assignments := []porygonTxAssignment{
		{TxID: "ctx-origin", OriginalIndex: 5, ExecutionShard: 0, CrossShard: true, InvolvedShards: []int{0, 1}},
		{TxID: "itx-origin", OriginalIndex: 6, ExecutionShard: 1, CrossShard: false, InvolvedShards: []int{1}},
	}
	plan := porygonExecutionPlan{
		Version:             "v41-test",
		AlgorithmID:         porygonPlanAlgorithmID,
		BlockHeight:         height,
		OrderingDomain:      "porygon-global",
		ExecutionShardCount: 2,
		Assignments:         assignments,
	}
	plan.PlanDigest = stableJSONDigest(porygonPlanDigestProjection(plan))
	payload, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{
		ShardID:      "porygon-global",
		Height:       height,
		PreviousHash: "genesis",
		ProposerID:   "n0",
		ExecutionPlan: &realblock.ExecutionPlanEnvelope{
			AlgorithmID:   porygonPlanAlgorithmID,
			Payload:       payload,
			PayloadDigest: stableTextDigest(string(payload)),
			PlanDigest:    plan.PlanDigest,
		},
	}
	realblock.AssignHash(&block)
	result := BlockExecutionResult{ExecutionResult: execution.Result{
		BlockHash: block.BlockHash,
		Height:    height,
		TxDeltas: []execution.TxDelta{
			{TxID: "ctx-origin", OriginalIndex: 5, WriteSet: map[string]string{ctxKey: "ctx-v1"}, Success: true},
			{TxID: "itx-origin", OriginalIndex: 6, WriteSet: map[string]string{itxKey: "itx-v1"}, Success: true},
		},
	}}
	return block, result
}

func porygonV41InstallOriginExecution(r *NodeRuntime, block realblock.Block, executed BlockExecutionResult, phase porygonPipelinePhase) {
	pipeline := r.porygonPipelineRuntime()
	pipeline.mu.Lock()
	if pipeline.blocks == nil {
		pipeline.blocks = map[uint64]*porygonPipelineBlock{}
	}
	pipeline.blocks[block.Height] = &porygonPipelineBlock{Block: block, Execution: executed, Phase: phase}
	if block.Height > pipeline.executedHeight && phase != porygonPipelineOrdered {
		pipeline.executedHeight = block.Height
	}
	pipeline.mu.Unlock()
}

func TestPorygonV41Height3ProposalUUsesCertifiedB1ExecutionAcrossEightValidators(t *testing.T) {
	nodes := porygonV40FixedNodes()
	ctxKey := porygonV392KeyForShard(t, "v41-ctx", 1, 2)
	itxKey := porygonV392KeyForShard(t, "v41-itx", 0, 2)
	origin, executed := porygonV41CertifiedOriginBlock(t, 1, ctxKey, itxKey)

	var leaderTruth []PorygonProposalUpdate
	var leaderDigest string
	for i, node := range nodes {
		r := porygonV38TruthRuntime(node.NodeID, node.ExecutionShardID)
		r.plan = Plan{NodeConfigs: nodes}
		state := &porygonPaperRuntimeState{
			baselineHeight: 0,
			txs:            map[string]*PorygonTxRoundLifecycle{},
			proposalUpdates: map[uint64][]PorygonProposalUpdate{
				3: {{TxID: fmt.Sprintf("replica-local-drift-%d", i), OriginHeight: 1, Kind: "commit"}},
			},
			certifiedSnapshots:      map[uint64]map[string]string{0: {}},
			certifiedPartitionRoots: map[uint64]map[string]string{0: {"s0": "r0", "s1": "r1"}},
			certifiedRoots:          map[uint64]string{0: stableJSONDigest(map[string]string{"s0": "r0", "s1": "r1"})},
		}
		pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 2, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
		porygonV38InstallPaperState(r, state, pipeline)
		porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineDurable)

		rows, err := r.porygonPaperCertifiedUpdatesForProposal(3)
		if err != nil {
			t.Fatalf("node=%s certified B1->B3 U derivation failed: %v", node.NodeID, err)
		}
		if len(rows) != 1 || rows[0].TxID != "ctx-origin" || rows[0].OriginHeight != 1 {
			t.Fatalf("node=%s derived U=%#v want exactly certified CTx from B1", node.NodeID, rows)
		}
		if rows[0].Updates[0].OriginalIndex != 5 || rows[0].Updates[0].Key != ctxKey {
			t.Fatalf("node=%s derived U lost OC order/write identity: %#v", node.NodeID, rows[0])
		}
		digest := stableJSONDigest(rows)
		if i == 0 {
			leaderTruth = rows
			leaderDigest = digest
		} else if digest != leaderDigest {
			t.Fatalf("node=%s derived Proposal.U diverged across replicas", node.NodeID)
		}
		if err := r.porygonPaperVerifyCertifiedProposalU(3, leaderTruth); err != nil {
			t.Fatalf("node=%s rejected leader B3 Proposal.U reconstructed from certified B1 execution: %v", node.NodeID, err)
		}
		legacy := r.porygonPaperUpdatesForProposal(3)
		if stableJSONDigest(legacy) == leaderDigest {
			t.Fatalf("node=%s fixture failed to create replica-local lifecycle drift", node.NodeID)
		}
		porygonV38CleanupRuntime(r)
	}
}

func TestPorygonV41Height3ReadinessRequiresCertifiedB1Execution(t *testing.T) {
	nodes := porygonV40FixedNodes()
	r := porygonV38TruthRuntime("n4", "s1")
	r.plan = Plan{NodeConfigs: nodes}
	defer porygonV38CleanupRuntime(r)
	ctxKey := porygonV392KeyForShard(t, "v41-ready-ctx", 1, 2)
	itxKey := porygonV392KeyForShard(t, "v41-ready-itx", 0, 2)
	origin, executed := porygonV41CertifiedOriginBlock(t, 1, ctxKey, itxKey)
	roots1 := map[string]string{"s0": "s0-h1", "s1": "s1-h1"}
	state := &porygonPaperRuntimeState{
		baselineHeight: 0,
		txs:            map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{},
		certifiedSnapshots:      map[uint64]map[string]string{1: {}},
		certifiedPartitionRoots: map[uint64]map[string]string{1: roots1},
		certifiedRoots:          map[uint64]string{1: stableJSONDigest(roots1)},
		latestCertifiedHeight:   1,
	}
	pipeline := &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 2, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}}
	porygonV38InstallPaperState(r, state, pipeline)

	if ready, _ := r.porygonPaperProposalValidationReady(3); ready {
		t.Fatal("B3 became proposal-ready without certified B1 execution record for Proposal.U")
	}
	porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineOrdered)
	if ready, _ := r.porygonPaperProposalValidationReady(3); ready {
		t.Fatal("B3 became proposal-ready while B1 was ordered but not executed")
	}
	porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineExecuted)
	if ready, reason := r.porygonPaperProposalValidationReady(3); !ready {
		t.Fatalf("B3 remained blocked after certified B1 execution became available: %s", reason)
	}
}

func TestPorygonV41ProposalURejectsLeaderValueNotDerivedFromCertifiedExecution(t *testing.T) {
	r := porygonV38TruthRuntime("n7", "s1")
	defer porygonV38CleanupRuntime(r)
	nodes := porygonV40FixedNodes()
	r.plan = Plan{NodeConfigs: nodes}
	ctxKey := porygonV392KeyForShard(t, "v41-reject", 1, 2)
	itxKey := porygonV392KeyForShard(t, "v41-reject-itx", 0, 2)
	origin, executed := porygonV41CertifiedOriginBlock(t, 1, ctxKey, itxKey)
	porygonV38InstallPaperState(r, &porygonPaperRuntimeState{baselineHeight: 0, txs: map[string]*PorygonTxRoundLifecycle{}, proposalUpdates: map[uint64][]PorygonProposalUpdate{}}, &porygonPipelineRuntime{baselineHeight: 0, executedHeight: 1, blocks: map[uint64]*porygonPipelineBlock{}, rollbackDigests: map[uint64]string{}})
	porygonV41InstallOriginExecution(r, origin, executed, porygonPipelineExecuted)
	good, err := r.porygonPaperCertifiedUpdatesForProposal(3)
	if err != nil {
		t.Fatal(err)
	}
	bad := append([]PorygonProposalUpdate(nil), good...)
	bad[0].Updates = append([]PorygonStateUpdate(nil), bad[0].Updates...)
	bad[0].Updates[0].Value = "tampered"
	bad[0].UpdateDigest = porygonProposalUpdateDigest(bad[0])
	if err := r.porygonPaperVerifyCertifiedProposalU(3, bad); err == nil {
		t.Fatal("backup accepted Proposal.U not derived from certified B1 execution")
	}
}
