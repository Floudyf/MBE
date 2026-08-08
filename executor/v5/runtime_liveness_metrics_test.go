package v5

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	realblock "metaverse-chainlab/executor/realism/block"
	"metaverse-chainlab/executor/realism/consensus/pbft"
	"metaverse-chainlab/executor/realism/execution"
	"metaverse-chainlab/executor/realism/p2p"
)

func TestFuturePrePrepareBecomingCurrentIsAcceptedInsteadOfDropped(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n2", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n3", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
	}}
	future := realblock.Block{
		ShardID:      "s0",
		Height:       2,
		PreviousHash: "height-1",
		ProposerID:   "n0",
		Timestamp:    1,
	}
	realblock.AssignHash(&future)

	sent := map[string]p2p.MessageEnvelope{}
	runtime := &NodeRuntime{
		node:                plan.NodeConfigs[1],
		plan:                plan,
		committedHeight:     0,
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		runtimeMetricCounts: map[string]int64{},
		sendToNodeHook: func(_ context.Context, nodeID string, envelope p2p.MessageEnvelope) error {
			sent[nodeID] = envelope
			return nil
		},
	}

	accepted, catchup, err := runtime.validatePrePrepare("n0", future)
	if err != nil || accepted || !catchup {
		t.Fatalf("future proposal disposition mismatch before parent commit: accepted=%t catchup=%t err=%v", accepted, catchup, err)
	}

	// Reproduce the observed race: the parent commits after validation classified
	// height 2 as future but before the deferral path acquires the runtime lock.
	runtime.mu.Lock()
	runtime.committedHeight = 1
	runtime.committedHash = "height-1"
	runtime.mu.Unlock()

	if err := runtime.handleDeferredPrePrepare(context.Background(), "n0", future, false); err != nil {
		t.Fatal(err)
	}
	if _, ok := runtime.deferredPrePrepares[future.Height]; ok {
		t.Fatal("proposal that became current was left in the future map")
	}
	if runtime.proposals[future.BlockHash].BlockHash != future.BlockHash {
		t.Fatal("proposal that became current was dropped instead of remembered")
	}
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		envelope, ok := sent[nodeID]
		if !ok || envelope.MessageType != p2p.MessagePBFTPrepare {
			t.Fatalf("proposal did not broadcast PREPARE to validator %s: %+v", nodeID, envelope)
		}
		vote, err := p2p.DecodePayload[pbft.Prepare](envelope)
		if err != nil {
			t.Fatal(err)
		}
		if vote.BlockHash != future.BlockHash || vote.Height != future.Height || vote.NodeID != "n1" {
			t.Fatalf("unexpected PREPARE vote after race recovery: %+v", vote)
		}
	}
}

func TestTimedOutProposalPreservesSameDigestAndVotes(t *testing.T) {
	block := realblock.Block{
		ShardID:      "s0",
		Height:       62,
		PreviousHash: "height-61",
		ProposerID:   "n0",
		Timestamp:    62,
	}
	realblock.AssignHash(&block)

	runtime := &NodeRuntime{
		node: NodePlan{
			NodeID:     "n0",
			ShardID:    "s0",
			Leader:     true,
			Validators: []string{"n0", "n1", "n2", "n3"},
		},
		plugins: RuntimePlugins{
			Consensus: builtinConsensus{makeBasic("consensus", "pbft_style_consensus", nil)},
		},
		proposals:            map[string]realblock.Block{block.BlockHash: block},
		votes:                map[string]map[string]bool{block.BlockHash: {"n0": true}},
		committed:            map[string]bool{},
		committing:           map[string]bool{},
		runtimeMetricCounts:  map[string]int64{},
		proposalInFlight:     true,
		proposalInFlightHash: block.BlockHash,
		proposalStartedAt:    time.Now().Add(-time.Minute),
	}

	runtime.expireStaleProposal(time.Second)

	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if !runtime.proposalInFlight || runtime.proposalInFlightHash != block.BlockHash {
		t.Fatal("same-view timeout replaced the in-flight proposal")
	}
	if runtime.proposals[block.BlockHash].BlockHash != block.BlockHash {
		t.Fatal("same-view timeout deleted the proposal")
	}
	if !runtime.votes[block.BlockHash]["n0"] {
		t.Fatal("same-view timeout deleted collected PREPARE votes")
	}
	if runtime.runtimeMetricCounts["pbft_same_digest_timeout_preserved_count"] != 1 {
		t.Fatalf("timeout preservation metric mismatch: %d", runtime.runtimeMetricCounts["pbft_same_digest_timeout_preserved_count"])
	}
}

func TestPBFTPrepareBroadcastTargetsOnlyShardValidators(t *testing.T) {
	plan := Plan{NodeConfigs: []NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n1", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n2", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n3", ShardID: "s0", Validators: []string{"n0", "n1", "n2", "n3"}},
		{NodeID: "n4", ShardID: "s1", Leader: true, Validators: []string{"n4"}},
	}}
	block := realblock.Block{ShardID: "s0", Height: 1, PreviousHash: "genesis", ProposerID: "n0", Timestamp: 1}
	realblock.AssignHash(&block)

	targets := map[string]bool{}
	runtime := &NodeRuntime{
		node:                plan.NodeConfigs[1],
		plan:                plan,
		committedHash:       "genesis",
		proposals:           map[string]realblock.Block{},
		deferredPrePrepares: map[uint64]deferredPrePrepare{},
		votes:               map[string]map[string]bool{},
		runtimeMetricCounts: map[string]int64{},
		sendToNodeHook: func(_ context.Context, nodeID string, _ p2p.MessageEnvelope) error {
			targets[nodeID] = true
			return nil
		},
	}
	if err := runtime.acceptPrePrepare(context.Background(), "n0", block, false); err != nil {
		t.Fatal(err)
	}
	for _, nodeID := range []string{"n0", "n2", "n3"} {
		if !targets[nodeID] {
			t.Fatalf("missing in-shard validator target %s: %#v", nodeID, targets)
		}
	}
	if targets["n4"] {
		t.Fatalf("PBFT message leaked to another shard: %#v", targets)
	}
}

func TestBlockSTMNodeAggregationPreservesAbortDecomposition(t *testing.T) {
	dataDir := t.TempDir()
	runtime := &NodeRuntime{node: NodePlan{NodeID: "n0", ShardID: "s0", DataDir: dataDir}}
	blocks := []map[string]any{
		{
			"block_hash":        "block-1",
			"height":            1,
			"block_executor_id": execution.BlockSTMExecutorID,
			"serial_equivalent": true,
			"block_stm_metrics": map[string]any{
				"worker_count":             4,
				"abort_count":              7,
				"dependency_abort_count":   3,
				"validation_abort_count":   4,
				"reexecution_count":        7,
				"validation_failure_count": 4,
			},
		},
		{
			"block_hash":        "block-2",
			"height":            2,
			"block_executor_id": execution.BlockSTMExecutorID,
			"serial_equivalent": true,
			"block_stm_metrics": map[string]any{
				"worker_count":             4,
				"abort_count":              5,
				"dependency_abort_count":   2,
				"validation_abort_count":   3,
				"reexecution_count":        5,
				"validation_failure_count": 3,
			},
		},
	}

	if err := runtime.writeBlockSTMArtifacts(blocks); err != nil {
		t.Fatal(err)
	}

	raw, err := os.ReadFile(filepath.Join(dataDir, "block_stm_summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	var summary struct {
		Metrics execution.BlockSTMMetrics `json:"block_stm_metrics"`
	}
	if err := json.Unmarshal(raw, &summary); err != nil {
		t.Fatal(err)
	}
	if summary.Metrics.AbortCount != 12 || summary.Metrics.DependencyAbortCount != 5 || summary.Metrics.ValidationAbortCount != 7 {
		t.Fatalf("abort decomposition was lost during node aggregation: %+v", summary.Metrics)
	}
	if summary.Metrics.AbortCount != summary.Metrics.DependencyAbortCount+summary.Metrics.ValidationAbortCount {
		t.Fatalf("aggregated abort decomposition is inconsistent: %+v", summary.Metrics)
	}

	file, err := os.Open(filepath.Join(dataDir, "block_stm_abort_trace.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	rows, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 3 {
		t.Fatalf("unexpected abort trace row count: %d", len(rows))
	}
	wantHeader := []string{"node_id", "shard_id", "block_hash", "height", "abort_count", "reexecution_count", "maximum_incarnation", "dependency_abort_count", "validation_abort_count"}
	if len(rows[0]) != len(wantHeader) {
		t.Fatalf("unexpected abort trace header width: %v", rows[0])
	}
	for index := range wantHeader {
		if rows[0][index] != wantHeader[index] {
			t.Fatalf("abort trace header mismatch at %d: got=%q want=%q", index, rows[0][index], wantHeader[index])
		}
	}
	if rows[1][7] != "3" || rows[1][8] != "4" || rows[2][7] != "2" || rows[2][8] != "3" {
		t.Fatalf("abort trace lost decomposition values: %#v", rows)
	}
}
