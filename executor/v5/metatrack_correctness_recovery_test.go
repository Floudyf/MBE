package v5

import (
	"encoding/json"
	"strings"
	"testing"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/tx"
)

func TestNonMetaTrackBatchPlanDoesNotRequireSignedMetaTrackRouting(t *testing.T) {
	runtime := &NodeRuntime{
		node: NodePlan{NodeID: "n0", ShardID: "s0"},
		plugins: RuntimePlugins{
			Routing: statelessHashRouting{basicPlugin: makeBasic("routing", "stateless_hash_routing", nil)},
		},
	}
	payload, err := json.Marshal(map[string]any{
		"transaction_placements": []TransactionPlacement{{LogicalID: "tx-1", ExecutionShard: "s0"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	block := realblock.Block{
		ShardID: "s0",
		TxList:  []tx.SignedTransaction{{TxID: "tx-1"}},
		ExecutionPlan: &realblock.ExecutionPlanEnvelope{
			AlgorithmID: "stateless_hash_batch_execution_plan_v1",
			Payload:     payload,
		},
	}
	if err := runtime.validateMetaTrackPlanDrivesExecution(block); err != nil {
		t.Fatalf("stateless hash plan was incorrectly subjected to MetaTrack signed-routing validation: %v", err)
	}
}

func TestMetaTrackRejectsWrongExecutionPlanAlgorithm(t *testing.T) {
	runtime := &NodeRuntime{
		node: NodePlan{NodeID: "n0", ShardID: "s0"},
		plugins: RuntimePlugins{
			Routing: &metaTrackRouting{basicPlugin: makeBasic("routing", "metatrack_coaccess_routing", nil)},
		},
	}
	block := realblock.Block{
		ShardID: "s0",
		TxList:  []tx.SignedTransaction{{TxID: "tx-1"}},
		ExecutionPlan: &realblock.ExecutionPlanEnvelope{
			AlgorithmID: "stateless_hash_batch_execution_plan_v1",
			Payload:     json.RawMessage(`{"transaction_placements":[]}`),
		},
	}
	err := runtime.validateMetaTrackPlanDrivesExecution(block)
	if err == nil || !strings.Contains(err.Error(), "algorithm mismatch") {
		t.Fatalf("expected MetaTrack algorithm mismatch, got %v", err)
	}
}
