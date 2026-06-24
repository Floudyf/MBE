package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadReplayConfigKeepsStateAndExecutionShardCountsSeparate(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	contents := []byte("state_sharding: {plugin: hash_state_sharding, shard_count: 3}\nexecution_sharding: {plugin: hash_execution_sharding, shard_count: 5}\n")
	if err := os.WriteFile(path, contents, 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadReplayConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.StateShardCount != 3 || config.ExecutionShardCount != 5 {
		t.Fatalf("got state=%d execution=%d", config.StateShardCount, config.ExecutionShardCount)
	}
}
