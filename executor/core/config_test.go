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

func TestLoadReplayConfigReadsV15RoutingPolicy(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("routing:\n  policy: co_access\n  co_access_min_weight: 2\n  co_access_max_group_size: 9\n  co_access_balance_weight: 1.5\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	config, err := LoadReplayConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if config.RoutingPolicy != "co_access" || config.CoAccessMinWeight != 2 || config.CoAccessMaxGroupSize != 9 || config.CoAccessBalanceWeight != 1.5 {
		t.Fatalf("unexpected V1.5 config: %+v", config)
	}
}
