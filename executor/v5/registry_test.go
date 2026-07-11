package v5

import "testing"

func TestBuiltinRegistryCoversEveryCategory(t *testing.T) {
	r := BuiltinRegistry()
	for _, category := range Categories {
		if _, err := r.Create(category, firstPlugin(category), map[string]any{}); err != nil {
			t.Fatalf("%s: %v", category, err)
		}
	}
}

func TestRegistryRejectsDuplicateAndUnknown(t *testing.T) {
	r := NewRegistry()
	if err := r.Register("routing", "test", func(map[string]any) (Plugin, error) { return builtinPlugin{category: "routing", id: "test"}, nil }); err != nil {
		t.Fatal(err)
	}
	if err := r.Register("routing", "test", func(map[string]any) (Plugin, error) { return nil, nil }); err == nil {
		t.Fatal("expected duplicate rejection")
	}
	if _, err := r.Create("routing", "missing", nil); err == nil {
		t.Fatal("expected unknown rejection")
	}
}

func firstPlugin(category string) string {
	for _, candidate := range BuiltinRegistry().factories {
		_ = candidate
	}
	defaults := map[string]string{"workload": "deterministic_signed_synthetic", "transaction_admission": "signature_nonce_admission", "txpool": "fifo_per_node_mempool", "sharding": "deterministic_state_key_sharding", "routing": "hash_routing_baseline", "block_producer": "time_or_count_block_producer", "consensus": "pbft_style_consensus", "network": "localhost_tcp_typed_network", "execution": "serial_execution_baseline", "scheduler": "fifo_serial_scheduler", "state_access": "direct_state_access", "state_storage": "persistent_local_state_store", "cross_shard": "relay_certificate_protocol", "commit": "normal_commit", "fault_injection": "faults_disabled", "metrics": "runtime_core_metrics", "observability": "node_network_consensus_observer"}
	return defaults[category]
}
