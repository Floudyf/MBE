package v5

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/tx"
)

func porygonFixtureTx(id, sender string, accesses ...tx.AccessItem) tx.SignedTransaction {
	keys := make([]string, 0, len(accesses))
	for _, access := range accesses {
		keys = append(keys, access.Key)
	}
	return tx.SignedTransaction{
		TxID: id, LogicalTxID: id, Sender: sender, Receiver: "fixture-receiver", Value: 1,
		StateKeys: keys, AccessList: accesses, AccessListSchema: "direct_v1", AccessListSource: "porygon_fixture",
	}
}

func porygonFixtureBlock(t *testing.T, items ...tx.SignedTransaction) realblock.Block {
	t.Helper()
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1, TxList: append([]tx.SignedTransaction(nil), items...), TxIDs: transactionIDs(items)}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, buildPorygonTransactionBlockEvidence(block, 1)); err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&block)
	return block
}

func porygonPlanConfig() map[string]any {
	return map[string]any{"execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true}
}

func porygonExecutorConfig() map[string]any {
	return map[string]any{"worker_count": 4, "execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true}
}

func TestPorygonPluginsRegisterThroughBuiltinRegistry(t *testing.T) {
	registry := BuiltinRegistry()
	for _, item := range []struct{ category, id string }{
		{"routing", porygonRoutingID},
		{"block_producer", porygonBlockProducerID},
		{"execution", porygonExecutionID},
		{"scheduler", porygonSchedulerID},
		{"block_executor", porygonBlockExecutorID},
		{"state_access", porygonStateAccessID},
		{"cross_shard", porygonCrossShardID},
	} {
		if _, err := registry.Create(item.category, item.id, map[string]any{}); err != nil {
			t.Fatalf("create %s:%s: %v", item.category, item.id, err)
		}
	}
}

func TestPorygonRoutingCannotActivateMetaTrackRemoteStateControlPlane(t *testing.T) {
	routing := porygonStatelessRouting{makeBasic("routing", porygonRoutingID, nil)}
	var candidate any = routing
	if _, ok := candidate.(BatchRoutingPlugin); ok {
		t.Fatal("Porygon routing must not implement BatchRoutingPlugin")
	}
	if _, ok := candidate.(RoutingRuntimeCapabilities); ok {
		t.Fatal("Porygon routing must not implement RoutingRuntimeCapabilities")
	}
	decision := routing.Route(RoutingInput{ShardIDs: []string{"s0"}})
	if decision.ShardID != "s0" {
		t.Fatalf("Porygon must stay in the single physical ordering domain: %#v", decision)
	}
}

func TestPorygonLogicalCrossShardDoesNotTriggerPhysicalRelay(t *testing.T) {
	item := porygonFixtureTx("logical-cross", "sender-a",
		tx.AccessItem{Key: "state-a", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
		tx.AccessItem{Key: "state-b", Mode: tx.AccessReadWrite, UpdateSemantics: "set"},
	)
	plugin := porygonCrossShard{makeBasic("cross_shard", porygonCrossShardID, map[string]any{"execution_shard_count": 4})}
	if plugin.IsCrossShard(item) {
		t.Fatal("logical Porygon cross-ESC transaction must not enter MBE physical Relay/Finalize")
	}
}

func TestPorygonPlanBindsSignedAccessNotSchedulingOverlay(t *testing.T) {
	item := porygonFixtureTx("layer-boundary", "sender-a", tx.AccessItem{Key: "runtime:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	item.SchedulingAccessList = []tx.AccessItem{{Key: "metatrack:planning:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	first := porygonAccessRoot([]tx.SignedTransaction{item})
	item.SchedulingAccessList = []tx.AccessItem{{Key: "metatrack:changed:key", Mode: tx.AccessRead, UpdateSemantics: "none"}}
	second := porygonAccessRoot([]tx.SignedTransaction{item})
	if first != second {
		t.Fatalf("SchedulingAccessList contaminated Porygon access root: %s != %s", first, second)
	}
	accesses := porygonCanonicalAccesses(item)
	if len(accesses) != 1 || accesses[0].Key != "runtime:key" {
		t.Fatalf("Porygon must use signed AccessList only: %#v", accesses)
	}
}

func TestPorygonExecutionShardAssignmentUsesSenderAndSpreadsAcrossESCs(t *testing.T) {
	items := make([]tx.SignedTransaction, 0, 64)
	for i := 0; i < 64; i++ {
		items = append(items, porygonFixtureTx(fmt.Sprintf("t%02d", i), fmt.Sprintf("sender-%02d", i), tx.AccessItem{Key: fmt.Sprintf("key-%02d", i), Mode: tx.AccessReadWrite, UpdateSemantics: "set"}))
	}
	block := porygonFixtureBlock(t, items...)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	seen := map[int]bool{}
	for _, assignment := range plan.Assignments {
		seen[assignment.ExecutionShard] = true
	}
	if len(seen) < 2 {
		t.Fatalf("Porygon ESC assignment collapsed to one shard: %#v", seen)
	}
}

func porygonFixtureSenderOnShard(t *testing.T, prefix string, targetShard, shardCount int) string {
	t.Helper()
	for i := 0; i < 1024; i++ {
		sender := fmt.Sprintf("%s-sender-%d", prefix, i)
		probe := porygonFixtureTx("probe", sender, tx.AccessItem{Key: "probe", Mode: tx.AccessRead, UpdateSemantics: "none"})
		if porygonAccountShard(probe, shardCount) == targetShard {
			return sender
		}
	}
	t.Fatalf("unable to construct sender on ESC %d", targetShard)
	return ""
}

func porygonFixtureKeyOnShard(t *testing.T, prefix string, targetShard, shardCount int) string {
	t.Helper()
	for i := 0; i < 1024; i++ {
		key := fmt.Sprintf("%s-key-%d", prefix, i)
		if porygonStateShard(key, shardCount) == targetShard {
			return key
		}
	}
	t.Fatalf("unable to construct state key on shard %d", targetShard)
	return ""
}

func porygonCrossESCFixture(t *testing.T, id string, executionShard, stateShard int) tx.SignedTransaction {
	t.Helper()
	const shardCount = 4
	if executionShard == stateShard {
		t.Fatalf("cross-ESC fixture requires distinct execution/state shards: exec=%d state=%d", executionShard, stateShard)
	}
	sender := porygonFixtureSenderOnShard(t, id, executionShard, shardCount)
	key := porygonFixtureKeyOnShard(t, id, stateShard, shardCount)
	item := porygonFixtureTx(id, sender, tx.AccessItem{Key: key, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	if got := porygonAccountShard(item, shardCount); got != executionShard {
		t.Fatalf("fixture sender mapped to ESC %d, want %d", got, executionShard)
	}
	if got := porygonStateShard(key, shardCount); got != stateShard {
		t.Fatalf("fixture key mapped to state shard %d, want %d", got, stateShard)
	}
	return item
}

func findDisjointCrossESCPair(t *testing.T) (tx.SignedTransaction, tx.SignedTransaction) {
	t.Helper()
	// Construct, rather than search for, two cross-ESC transactions with
	// disjoint execution ESCs and disjoint state shards. The old fixture used
	// sender/key strings whose stableKey sums were congruent modulo four, so a
	// cross-ESC sample was mathematically impossible regardless of loop count.
	left := porygonCrossESCFixture(t, "cross-a", 0, 1)
	right := porygonCrossESCFixture(t, "cross-b", 2, 3)
	return left, right
}

func TestPorygonDisjointCrossESCTransactionsCanShareWave(t *testing.T) {
	left, right := findDisjointCrossESCPair(t)
	block := porygonFixtureBlock(t, left, right)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Assignments) != 2 || !plan.Assignments[0].CrossShard || !plan.Assignments[1].CrossShard {
		t.Fatalf("fixtures are not logical cross-ESC transactions: %#v", plan.Assignments)
	}
	if plan.Assignments[0].ExecutionShard == plan.Assignments[1].ExecutionShard {
		t.Fatalf("fixtures unexpectedly share execution ESC: %#v", plan.Assignments)
	}
	if plan.Assignments[0].Wave != plan.Assignments[1].Wave {
		t.Fatalf("disjoint cross-ESC transactions were globally barriered: %#v", plan.Assignments)
	}
}

func TestPorygonSharedStatePreservesGlobalConflictOrder(t *testing.T) {
	shared := "shared-hot-state"
	var left, right tx.SignedTransaction
	for i := 0; i < 5000 && right.TxID == ""; i++ {
		candidate := porygonFixtureTx(fmt.Sprintf("t%d", i), fmt.Sprintf("sender-%d", i), tx.AccessItem{Key: shared, Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
		if left.TxID == "" {
			left = candidate
			continue
		}
		if porygonAccountShard(left, 4) != porygonAccountShard(candidate, 4) {
			right = candidate
		}
	}
	if right.TxID == "" {
		t.Fatal("unable to find sender pair on distinct ESCs")
	}
	block := porygonFixtureBlock(t, left, right)
	plan, err := buildPorygonPlan(block, porygonPlanConfig())
	if err != nil {
		t.Fatal(err)
	}
	if plan.Assignments[1].Wave <= plan.Assignments[0].Wave {
		t.Fatalf("shared-state global order not enforced: %#v", plan.Assignments)
	}
}

func TestPorygonCrossESCExecutesOnceAndMatchesSerialStateRoot(t *testing.T) {
	left, right := findDisjointCrossESCPair(t)
	items := []tx.SignedTransaction{left, right}
	block := porygonFixtureBlock(t, items...)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	result, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.BusinessAttempts) != len(items) {
		t.Fatalf("business execution attempts=%d want=%d", len(result.BusinessAttempts), len(items))
	}
	for _, attempt := range result.BusinessAttempts {
		if attempt.Attempt != 1 || !attempt.FinalCompletion {
			t.Fatalf("Porygon transaction was not exactly-once terminal execution: %#v", attempt)
		}
	}
	serial := execution.NewSerialExecutor().ExecuteBlock(planned.Block, map[string]string{})
	if result.ExecutionResult.StateRootAfter != serial.StateRootAfter {
		t.Fatalf("state root mismatch: porygon=%s serial=%s", result.ExecutionResult.StateRootAfter, serial.StateRootAfter)
	}
	if result.ActualMetrics["porygon_meta_remote_state_control_plane_used"] != false || result.ActualMetrics["porygon_physical_relay_protocol_used"] != false {
		t.Fatalf("Porygon leaked into shared remote/relay path: %#v", result.ActualMetrics)
	}
}

func TestPorygonFailsClosedOnAccessListViolation(t *testing.T) {
	// AccessList intentionally omits the balance/nonce keys used by the legacy
	// transfer execution path. The Porygon executor must reject this rather
	// than silently expanding execution with undeclared future information.
	item := tx.SignedTransaction{
		TxID: "bad-access", LogicalTxID: "bad-access", Sender: "alice", Receiver: "bob", Nonce: 0, Value: 1,
		StateKeys: []string{"declared-only"}, AccessList: []tx.AccessItem{{Key: "declared-only", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
	}
	block := porygonFixtureBlock(t, item)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	if _, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4}); err == nil {
		t.Fatal("undeclared execution access was accepted")
	}
}

func TestPorygonProposalAndPlanEvidenceRejectTamper(t *testing.T) {
	item := porygonFixtureTx("evidence", "sender", tx.AccessItem{Key: "asset:1", Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, item)
	producer := porygonBlockProducer{makeBasic("block_producer", porygonBlockProducerID, nil)}
	if err := producer.VerifyProposalEvidence(block); err != nil {
		t.Fatalf("exact proposal rejected: %v", err)
	}
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.VerifyBlockPlan(planned.Block); err != nil {
		t.Fatalf("exact plan rejected: %v", err)
	}
	tampered := block
	tampered.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	tampered.TxList[0].AccessList = []tx.AccessItem{{Key: "tampered", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}}
	if err := producer.VerifyProposalEvidence(tampered); err == nil {
		t.Fatal("tampered signed-access body was accepted")
	}
	var plan porygonExecutionPlan
	if err := json.Unmarshal(planned.Block.ExecutionPlan.Payload, &plan); err != nil {
		t.Fatal(err)
	}
	plan.Assignments[0].ExecutionShard = (plan.Assignments[0].ExecutionShard + 1) % plan.ExecutionShardCount
	raw, _ := json.Marshal(plan)
	tamperedPlan := planned.Block
	tamperedPlan.ExecutionPlan = &realblock.ExecutionPlanEnvelope{AlgorithmID: porygonPlanAlgorithmID, PayloadDigest: stableTextDigest(string(raw)), PlanDigest: plan.PlanDigest, Payload: raw}
	if err := scheduler.VerifyBlockPlan(tamperedPlan); err == nil {
		t.Fatal("tampered Porygon execution assignment was accepted")
	}
}

func TestPorygonPipelineTruthBoundaryDoesNotClaimWallClockOverlap(t *testing.T) {
	item := porygonFixtureTx("pipeline", "sender", tx.AccessItem{Key: "asset:pipeline", Mode: tx.AccessReadWrite, UpdateSemantics: "set"})
	block := porygonFixtureBlock(t, item)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, porygonPlanConfig())}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, porygonExecutorConfig())}
	result, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if result.ActualMetrics["porygon_wall_clock_pipeline_overlap_claimed"] != false {
		t.Fatalf("wall-clock pipeline overlap was falsely claimed: %#v", result.ActualMetrics)
	}
	if result.ActualMetrics["porygon_pipeline_timing_truth_boundary"] != "logical_protocol_slots;wall_clock_overlap_not_claimed" {
		t.Fatalf("pipeline truth boundary missing: %#v", result.ActualMetrics)
	}
}
