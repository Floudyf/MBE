package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"

	v5 "metaverse-chainlab/executor/v5"
)

func porygonTruthProfile(blockExecutor string) map[string]v5.PluginConfig {
	return map[string]v5.PluginConfig{"block_executor": {PluginID: blockExecutor, Config: map[string]any{}}}
}

func TestPorygonConsistencyShardUsesExecutionPartitionWithoutChangingOtherMethods(t *testing.T) {
	p := v5.NodePlan{ShardID: "porygon-global", ExecutionShardID: "s1", PluginProfile: porygonTruthProfile("porygon_block_executor")}
	if got := calvinConsistencyShardID(p); got != "s1" {
		t.Fatalf("Porygon consistency shard=%q want s1", got)
	}
	c := v5.NodePlan{ShardID: "calvin-global", ExecutionShardID: "s2", PluginProfile: porygonTruthProfile("calvin_block_executor")}
	if got := calvinConsistencyShardID(c); got != "s2" {
		t.Fatalf("Calvin consistency shard changed: %q", got)
	}
	other := v5.NodePlan{ShardID: "s0", ExecutionShardID: "s1", PluginProfile: porygonTruthProfile("block_stm_block_executor")}
	if got := calvinConsistencyShardID(other); got != "s0" {
		t.Fatalf("non-Porygon/Calvin consistency shard changed: %q", got)
	}
}

func writePorygonTruthChain(t *testing.T, dir, nodeID, state1, receipt1, state2, receipt2 string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.Create(filepath.Join(dir, "committed_chain.csv"))
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	// writeHeightRootMatrix consumes indexes 0,1,2,4,5,7,9,10.
	rows := [][]string{
		{"node_id", "shard_id", "height", "unused", "block_hash", "parent_hash", "unused2", "tx_root", "unused3", "state_root", "receipt_root"},
		{nodeID, "porygon-global", "1", "", "block-1", "parent-0", "", "tx-1", "", state1, receipt1},
		{nodeID, "porygon-global", "2", "", "block-2", "block-1", "", "tx-2", "", state2, receipt2},
	}
	if err := w.WriteAll(rows); err != nil {
		t.Fatal(err)
	}
	w.Flush()
	if err := w.Error(); err != nil {
		t.Fatal(err)
	}
}

func TestPorygonHeightRootMatrixAcceptsDifferentPartitionRootsWithReplicaAgreement(t *testing.T) {
	root := t.TempDir()
	nodes := []v5.NodePlan{
		{NodeID: "n0", ShardID: "porygon-global", ExecutionShardID: "s0", DataDir: filepath.Join(root, "n0"), PluginProfile: porygonTruthProfile("porygon_block_executor")},
		{NodeID: "n1", ShardID: "porygon-global", ExecutionShardID: "s0", DataDir: filepath.Join(root, "n1"), PluginProfile: porygonTruthProfile("porygon_block_executor")},
		{NodeID: "n4", ShardID: "porygon-global", ExecutionShardID: "s1", DataDir: filepath.Join(root, "n4"), PluginProfile: porygonTruthProfile("porygon_block_executor")},
		{NodeID: "n5", ShardID: "porygon-global", ExecutionShardID: "s1", DataDir: filepath.Join(root, "n5"), PluginProfile: porygonTruthProfile("porygon_block_executor")},
	}
	writePorygonTruthChain(t, nodes[0].DataDir, "n0", "s0-state-1", "s0-receipt-1", "s0-state-2", "s0-receipt-2")
	writePorygonTruthChain(t, nodes[1].DataDir, "n1", "s0-state-1", "s0-receipt-1", "s0-state-2", "s0-receipt-2")
	writePorygonTruthChain(t, nodes[2].DataDir, "n4", "s1-state-1", "s1-receipt-1", "s1-state-2", "s1-receipt-2")
	writePorygonTruthChain(t, nodes[3].DataDir, "n5", "s1-state-1", "s1-receipt-1", "s1-state-2", "s1-receipt-2")
	stateOK, receiptOK, err := writeHeightRootMatrix(root, nodes)
	if err != nil {
		t.Fatal(err)
	}
	if !stateOK {
		t.Fatal("Porygon partition-replica state roots should be consistent")
	}
	if !receiptOK {
		t.Fatal("Porygon partition-replica receipt roots should be consistent without Calvin global-receipt rule")
	}
}
