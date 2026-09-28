package main

import (
	"os"
	"path/filepath"
	"testing"

	"metaverse-chainlab/executor/v5"
)

func writeExecutionLogV36(t *testing.T, dir string, txIDs ...string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	text := "timestamp,node_id,shard_id,tx_id,height,execution_plugin,track,reason\n"
	for _, txID := range txIDs {
		text += "0,n,s," + txID + ",1,e,t,r\n"
	}
	if err := os.WriteFile(filepath.Join(dir, "execution_log.csv"), []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestV36RepresentativeExecutionLogTruthDeduplicatesAcrossPartitions(t *testing.T) {
	root := t.TempDir()
	s0 := filepath.Join(root, "n0")
	s1 := filepath.Join(root, "n1")
	writeExecutionLogV36(t, s0, "tx-1", "tx-2")
	writeExecutionLogV36(t, s1, "tx-2", "tx-3")
	plan := v5.Plan{NodeConfigs: []v5.NodePlan{
		{NodeID: "n0", ShardID: "s0", Leader: true, DataDir: s0},
		{NodeID: "n1", ShardID: "s1", Leader: true, DataDir: s1},
	}}
	unique, instances, available, err := representativeExecutionLogTruth(plan)
	if err != nil {
		t.Fatal(err)
	}
	if !available || unique != 3 || instances != 4 {
		t.Fatalf("unexpected truth counts available=%v unique=%d instances=%d", available, unique, instances)
	}
}
