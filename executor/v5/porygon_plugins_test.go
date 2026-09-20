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

func TestPorygonPlanBindsTransactionAndAccessRoots(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 3, Sender: "porygon-a", Receiver: "porygon-b", StartNonce: 0, Value: 1, Seed: "porygon-plan", StartTimeMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxList: items, TxIDs: transactionIDs(items)}
	evidence := buildPorygonTransactionBlockEvidence(block, 1)
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, evidence); err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&block)
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, map[string]any{"execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true})}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	if err := scheduler.VerifyBlockPlan(planned.Block); err != nil {
		t.Fatal(err)
	}
	var plan porygonExecutionPlan
	if err := jsonUnmarshalForPorygonTest(planned.Block.ExecutionPlan.Payload, &plan); err != nil {
		t.Fatal(err)
	}
	if plan.TransactionRoot != evidence.TransactionRoot || plan.AccessRoot != evidence.AccessRoot {
		t.Fatalf("porygon roots not bound: plan=%+v evidence=%+v", plan, evidence)
	}
	if !plan.CrossBatchWitness || len(plan.Pipeline) < 5 {
		t.Fatalf("cross-batch witness pipeline missing: %+v", plan.Pipeline)
	}
}

func TestPorygonCrossShardTransactionExecutesOnceAndUpdatesAtomically(t *testing.T) {
	left, right := "", ""
	for i := 0; i < 1000 && right == ""; i++ {
		candidate := fmt.Sprintf("porygon-key-%d", i)
		if left == "" {
			left = candidate
			continue
		}
		if porygonExecutionShard(candidate, 4) != porygonExecutionShard(left, 4) {
			right = candidate
		}
	}
	if right == "" {
		t.Fatal("failed to find deterministic cross-shard key pair")
	}
	items, _, _, err := tx.Generate(tx.GenerateOptions{
		Count: 1, Sender: "porygon-x", Receiver: "porygon-y", StartNonce: 0, Value: 1, Seed: "porygon-cross", StartTimeMS: 1,
		AccessList: []tx.AccessItem{{Key: left, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}, {Key: right, Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxList: items, TxIDs: transactionIDs(items)}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, buildPorygonTransactionBlockEvidence(block, 1)); err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&block)
	schedulerConfig := map[string]any{"execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true}
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, schedulerConfig)}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	executor := porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, map[string]any{"worker_count": 4, "execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true})}
	result, err := executor.ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	if intValue(result.ActualMetrics["porygon_cross_shard_transaction_count"]) != 1 || intValue(result.ActualMetrics["porygon_single_shard_execution_count"]) != 1 {
		t.Fatalf("cross-shard execution evidence mismatch: %#v", result.ActualMetrics)
	}
	if intValue(result.ActualMetrics["porygon_multi_shard_update_count"]) < 2 {
		t.Fatalf("multi-shard update evidence missing: %#v", result.ActualMetrics)
	}
}

func TestPorygonStateRootMatchesSerialOracle(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 4, Sender: "porygon-s", Receiver: "porygon-r", StartNonce: 0, Value: 1, Seed: "porygon-oracle", StartTimeMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxList: items, TxIDs: transactionIDs(items)}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, buildPorygonTransactionBlockEvidence(block, 1)); err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&block)
	config := map[string]any{"execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true}
	scheduler := porygonScheduler{makeBasic("scheduler", porygonSchedulerID, config)}
	planned, err := scheduler.PlanBlock(block)
	if err != nil {
		t.Fatal(err)
	}
	realblock.AssignHash(&planned.Block)
	porygonResult, err := (porygonBlockExecutor{makeBasic("block_executor", porygonBlockExecutorID, map[string]any{"worker_count": 4, "execution_shard_count": 4, "execution_committee_count": 3, "pipeline_enabled": true, "cross_batch_witness": true})}).ExecuteBlock(context.Background(), BlockExecutionInput{Block: planned.Block, BaseStateSnapshot: map[string]string{}, WorkerCount: 4})
	if err != nil {
		t.Fatal(err)
	}
	serial := execution.NewSerialExecutor().ExecuteBlock(planned.Block, map[string]string{})
	if porygonResult.ExecutionResult.StateRootAfter != serial.StateRootAfter {
		t.Fatalf("state root mismatch: porygon=%s serial=%s", porygonResult.ExecutionResult.StateRootAfter, serial.StateRootAfter)
	}
	if porygonResult.ExecutionResult.SuccessfulTxs != serial.SuccessfulTxs || porygonResult.ExecutionResult.FailedTxs != serial.FailedTxs {
		t.Fatalf("terminal outcome mismatch: porygon=(%d,%d) serial=(%d,%d)", porygonResult.ExecutionResult.SuccessfulTxs, porygonResult.ExecutionResult.FailedTxs, serial.SuccessfulTxs, serial.FailedTxs)
	}
}

func jsonUnmarshalForPorygonTest(raw []byte, target any) error {
	return json.Unmarshal(raw, target)
}

func TestPorygonIgnoresMetaTrackSchedulingAccessOverlay(t *testing.T) {
	item := tx.SignedTransaction{
		TxID:                 "porygon-layer-boundary",
		AccessList:           []tx.AccessItem{{Key: "runtime:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
		SchedulingAccessList: []tx.AccessItem{{Key: "metatrack:planning:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
	}
	first := porygonAccessRoot([]tx.SignedTransaction{item})
	item.SchedulingAccessList = []tx.AccessItem{{Key: "metatrack:changed:key", Mode: tx.AccessRead, UpdateSemantics: "none"}}
	second := porygonAccessRoot([]tx.SignedTransaction{item})
	if first != second {
		t.Fatalf("Porygon access root was contaminated by SchedulingAccessList: first=%s second=%s", first, second)
	}
	accesses := porygonCanonicalAccesses(item)
	if len(accesses) != 1 || accesses[0].Key != "runtime:key" {
		t.Fatalf("Porygon must bind real AccessList, got %#v", accesses)
	}

	routing := porygonStatelessRouting{makeBasic("routing", porygonRoutingID, nil)}
	if routingBindsExecutionMetadata(routing) || routingUsesStatelessVersionAdmission(routing) {
		t.Fatal("Porygon must not enter MetaTrack/stateless version-chain execution control")
	}
	plan := routing.PlanBatch(BatchRoutingInput{
		BatchIndex: 1,
		Records: []WorkloadRecord{{
			Index:                0,
			LogicalID:            "porygon-layer-boundary",
			StateKeys:            []string{"runtime:key"},
			AccessList:           []tx.AccessItem{{Key: "runtime:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			SchedulingAccessList: []tx.AccessItem{{Key: "metatrack:planning:key", Mode: tx.AccessReadWrite, UpdateSemantics: "set"}},
			SourceShard:          "s0",
		}},
		ShardIDs: []string{"s0", "s1"},
		Sharding: builtinSharding{makeBasic("sharding", "deterministic_state_key_sharding", nil)},
	})
	if len(plan.AccessMatrix) != 1 || plan.AccessMatrix[0].Key != "runtime:key" {
		t.Fatalf("Porygon routing plan used scheduling overlay: %#v", plan.AccessMatrix)
	}
}

func TestPorygonProposalEvidenceVerifierAcceptsExactBodyAndRejectsTamper(t *testing.T) {
	items, _, _, err := tx.Generate(tx.GenerateOptions{Count: 2, Sender: "porygon-v", Receiver: "porygon-w", Value: 1, Seed: "porygon-evidence", StartTimeMS: 1})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{ShardID: "s0", Height: 1, TxList: items, TxIDs: transactionIDs(items)}
	if err := attachProposalEvidence(&block, porygonProposalEvidenceID, buildPorygonTransactionBlockEvidence(block, 1)); err != nil {
		t.Fatal(err)
	}
	producer := porygonBlockProducer{makeBasic("block_producer", porygonBlockProducerID, nil)}
	if err := producer.VerifyProposalEvidence(block); err != nil {
		t.Fatalf("exact Porygon proposal evidence rejected: %v", err)
	}
	tampered := block
	tampered.TxList = append([]tx.SignedTransaction(nil), block.TxList...)
	tampered.TxList[0].Payload += ":tampered"
	if err := producer.VerifyProposalEvidence(tampered); err == nil {
		t.Fatal("tampered Porygon transaction body was accepted")
	}
}
