package v5

import (
	"context"
	"testing"

	"metaverse-chainlab/executor/realism/state"
	"metaverse-chainlab/executor/realism/tx"
)

func TestPorygonStateOwnerGroupsRelatedAccountObjectState(t *testing.T) {
	keys := []string{
		"m.federation/miners/alice",
		"m.federation/bags/alice",
		"userpoints.worlds/userpoints/alice",
	}
	owner := porygonStateOwnerIdentity(keys[0])
	if owner != "alice" {
		t.Fatalf("owner=%q want alice", owner)
	}
	for _, key := range keys[1:] {
		if got := porygonStateOwnerIdentity(key); got != owner {
			t.Fatalf("related key %q owner=%q want %q", key, got, owner)
		}
		if porygonStateShard(key, 4) != porygonStateShard(keys[0], 4) {
			t.Fatalf("related account state split across Porygon state shards: %q", key)
		}
	}
}

func TestPorygonStateProofMatchesMBECommitmentAndRejectsTamper(t *testing.T) {
	snapshot := map[string]string{
		"s0::m.federation/miners/alice": "miner-1",
		"s0::m.federation/bags/alice":   "bag-1",
		"s0::planet/global":             "planet-1",
	}
	key := "s0::m.federation/bags/alice"
	proof, ok := porygonGenerateStateProof(snapshot, key)
	if !ok {
		t.Fatal("expected inclusion proof")
	}
	root := state.Root(snapshot)
	if proof.Root != root {
		t.Fatalf("proof root=%s MBE state root=%s", proof.Root, root)
	}
	if !porygonVerifyStateProof(proof, key, snapshot[key], root) {
		t.Fatal("valid Porygon state proof rejected")
	}
	tampered := proof
	tampered.Value = "tampered"
	if porygonVerifyStateProof(tampered, key, "tampered", root) {
		t.Fatal("tampered Porygon state proof accepted")
	}
	missing, ok := porygonGenerateStateProof(snapshot, "s0::missing")
	if !ok || missing.Exists {
		t.Fatal("expected verifiable non-membership proof")
	}
	if !porygonVerifyStateProof(missing, "s0::missing", "", root) {
		t.Fatal("valid Porygon non-membership proof rejected")
	}
}

func TestPorygonProtocolThresholdsRemainDistinct(t *testing.T) {
	if got := porygonExecutionThreshold(4); got != 2 {
		t.Fatalf("Te=%d want f+1=2", got)
	}
	if got := porygonMultiShardUpdateThreshold(4); got != 3 {
		t.Fatalf("Tmsu=%d want strict majority=3", got)
	}
	if got := porygonWitnessFaultThreshold(4); got != 2 {
		t.Fatalf("Tw floor=%d want f+1=2", got)
	}
}

func TestPorygonPlanAbandonsCrossESCConflictButKeepsSameESCSequential(t *testing.T) {
	const shardCount = 4
	shared := porygonFixtureKeyOnShard(t, "paper-conflict", 3, shardCount)
	left := porygonFixtureTx("paper-left", "alice", tx.AccessItem{Key: shared, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	right := porygonFixtureTx("paper-right", "bob", tx.AccessItem{Key: shared, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	left.ExecutionRouting = &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", RoutingReason: "porygon_initiating_account_route"}
	right.ExecutionRouting = &tx.ExecutionRoutingMetadata{ExecutionShard: "s2", RoutingReason: "porygon_initiating_account_route"}
	block := porygonFixtureBlock(t, left, right)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Assignments) != 2 || !plan.Assignments[1].Abandoned || plan.Assignments[1].ConflictReason != porygonCrossESCConflictPolicy {
		t.Fatalf("cross-ESC conflict was not abandoned by OC policy: %#v", plan.Assignments)
	}

	sameLeft := porygonFixtureTx("same-left", "alice", tx.AccessItem{Key: shared, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	sameRight := porygonFixtureTx("same-right", "bob", tx.AccessItem{Key: shared, Mode: tx.AccessWrite, UpdateSemantics: "set"})
	sameLeft.ExecutionRouting = &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", RoutingReason: "porygon_initiating_account_route"}
	sameRight.ExecutionRouting = &tx.ExecutionRoutingMetadata{ExecutionShard: "s0", RoutingReason: "porygon_initiating_account_route"}
	sameBlock := porygonFixtureBlock(t, sameLeft, sameRight)
	samePlan, err := buildPorygonPlan(sameBlock, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if samePlan.Assignments[0].Abandoned || samePlan.Assignments[1].Abandoned || samePlan.Assignments[1].Wave <= samePlan.Assignments[0].Wave {
		t.Fatalf("same-ESC conflict must remain sequential inside the ESC: %#v", samePlan.Assignments)
	}
}
func TestPorygonAccountShardUsesSignedInitiatingAccountRoute(t *testing.T) {
	item := tx.SignedTransaction{TxID: "aw-route", Sender: "runtime-address", Receiver: "receiver", AccessList: []tx.AccessItem{{Key: "m.federation/miners/alice", Mode: tx.AccessReadWrite}, {Key: "federation/landregs/42", Mode: tx.AccessRead}, {Key: "m.federation/landcomms/owner", Mode: tx.AccessReadWrite}}, ExecutionRouting: &tx.ExecutionRoutingMetadata{ExecutionShard: "s1", RoutingReason: "porygon_initiating_account_route"}}
	if got := porygonAccountShard(item, 2); got != 1 {
		t.Fatalf("Porygon initiating-account route shard=%d want 1", got)
	}
}

func TestPorygonPipelineTruthClaimRequiresRuntimeOverlapEvidence(t *testing.T) {
	item := porygonFixtureTx("pipeline-overlap-evidence", "sender", tx.AccessItem{Key: "asset:pipeline-overlap", Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, item)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	withoutEvidence, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if withoutEvidence.ActualMetrics["porygon_wall_clock_pipeline_overlap_claimed"] != false {
		t.Fatalf("direct executor falsely claimed runtime overlap: %#v", withoutEvidence.ActualMetrics)
	}
	withEvidence, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4, PorygonCrossBatchWitnessOverlap: true})
	if err != nil {
		t.Fatal(err)
	}
	if withEvidence.ActualMetrics["porygon_wall_clock_pipeline_overlap_claimed"] != true {
		t.Fatalf("runtime overlap evidence was not reflected in metrics: %#v", withEvidence.ActualMetrics)
	}
	if withEvidence.ActualMetrics["porygon_pipeline_timing_truth_boundary"] != "real_cross_batch_witness_wall_clock_overlap;shared_pbft_ordering_heights_remain_sequential" {
		t.Fatalf("runtime overlap truth boundary missing: %#v", withEvidence.ActualMetrics)
	}
}
