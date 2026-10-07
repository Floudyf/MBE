package v5

import "testing"

func txalloNodeRatioModeProfileV21(stateless bool) map[string]PluginConfig {
	routing := txalloRoutingID
	crossShard := "relay_certificate_protocol"
	if stateless {
		routing = txalloStatelessRoutingID
		crossShard = "txallo_no_relay"
	}
	return map[string]PluginConfig{
		"workload":              {PluginID: "deterministic_signed_synthetic", Config: map[string]any{"cross_shard_ratio": 0.0, "timeout_every": 0}},
		"transaction_admission": {PluginID: "signature_nonce_admission", Config: map[string]any{}},
		"txpool":                {PluginID: "fifo_per_node_mempool", Config: map[string]any{"capacity": 10000}},
		"sharding":              {PluginID: txalloShardingID, Config: map[string]any{"eta": 2.0, "lambda": 0.0, "epsilon": 0.0, "history_ratio": 0.10, "allocation_mode": "paper_g_ratio_snapshot", "g_cache_enabled": true}},
		"routing":               {PluginID: routing, Config: map[string]any{}},
		"block_producer":        {PluginID: "time_or_count_block_producer", Config: map[string]any{"block_size": 1000, "interval_ms": 100}},
		"consensus":             {PluginID: "pbft_style_consensus", Config: map[string]any{}},
		"network":               {PluginID: "localhost_tcp_typed_network", Config: map[string]any{}},
		"execution":             {PluginID: "serial_execution_baseline", Config: map[string]any{}},
		"scheduler":             {PluginID: "fifo_serial_scheduler", Config: map[string]any{}},
		"block_executor":        {PluginID: "serial_block_executor", Config: map[string]any{"worker_count": 1}},
		"state_access":          {PluginID: "direct_state_access", Config: map[string]any{}},
		"state_storage":         {PluginID: "persistent_local_state_store", Config: map[string]any{}},
		"cross_shard":           {PluginID: crossShard, Config: map[string]any{}},
		"commit":                {PluginID: "normal_commit", Config: map[string]any{}},
		"fault_injection":       {PluginID: "faults_disabled", Config: map[string]any{}},
		"metrics":               {PluginID: "runtime_core_metrics", Config: map[string]any{}},
		"observability":         {PluginID: "node_network_consensus_observer", Config: map[string]any{}},
	}
}

func TestTxAlloNodeRatioModeV21StatefulInstantiate(t *testing.T) {
	if _, err := InstantiatePlugins(txalloNodeRatioModeProfileV21(false)); err != nil {
		t.Fatalf("stateful TxAllo node profile rejected: %v", err)
	}
}
func TestTxAlloNodeRatioModeV21StatelessInstantiate(t *testing.T) {
	if _, err := InstantiatePlugins(txalloNodeRatioModeProfileV21(true)); err != nil {
		t.Fatalf("stateless TxAllo node profile rejected: %v", err)
	}
}
