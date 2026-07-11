package v5

import "fmt"

// Plugin is intentionally small: the runtime owns lifecycle while plugins declare behavior.
type Plugin interface {
	ID() string
	Category() string
	Validate(map[string]any) error
}

type Factory func(map[string]any) (Plugin, error)

type Registry struct{ factories map[string]Factory }

func NewRegistry() *Registry { return &Registry{factories: map[string]Factory{}} }

func (r *Registry) Register(category, id string, factory Factory) error {
	key := category + ":" + id
	if _, exists := r.factories[key]; exists {
		return fmt.Errorf("duplicate plugin %s", key)
	}
	r.factories[key] = factory
	return nil
}

func (r *Registry) Create(category, id string, config map[string]any) (Plugin, error) {
	factory, ok := r.factories[category+":"+id]
	if !ok {
		return nil, fmt.Errorf("unknown plugin %s:%s", category, id)
	}
	return factory(config)
}

type builtinPlugin struct{ category, id string }

func (p builtinPlugin) ID() string                    { return p.id }
func (p builtinPlugin) Category() string              { return p.category }
func (p builtinPlugin) Validate(map[string]any) error { return nil }

var Categories = []string{"workload", "transaction_admission", "txpool", "sharding", "routing", "block_producer", "consensus", "network", "execution", "scheduler", "state_access", "state_storage", "cross_shard", "commit", "fault_injection", "metrics", "observability"}

func BuiltinRegistry() *Registry {
	r := NewRegistry()
	items := map[string][]string{
		"workload": {"deterministic_signed_synthetic"}, "transaction_admission": {"signature_nonce_admission"}, "txpool": {"fifo_per_node_mempool"}, "sharding": {"deterministic_state_key_sharding"}, "routing": {"hash_routing_baseline", "metatrack_coaccess_routing"}, "block_producer": {"time_or_count_block_producer"}, "consensus": {"pbft_style_consensus"}, "network": {"localhost_tcp_typed_network"}, "execution": {"serial_execution_baseline", "dual_track_execution"}, "scheduler": {"fifo_serial_scheduler", "fast_first_scheduler"}, "state_access": {"direct_state_access"}, "state_storage": {"persistent_local_state_store"}, "cross_shard": {"relay_certificate_protocol"}, "commit": {"normal_commit", "commutative_hot_update_aggregation"}, "fault_injection": {"faults_disabled", "network_delay_drop"}, "metrics": {"runtime_core_metrics"}, "observability": {"node_network_consensus_observer"},
	}
	for category, ids := range items {
		for _, id := range ids {
			category, id := category, id
			_ = r.Register(category, id, func(config map[string]any) (Plugin, error) {
				p := builtinPlugin{category: category, id: id}
				return p, p.Validate(config)
			})
		}
	}
	return r
}
