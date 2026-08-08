from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_aria_manifests_lock_official_source_and_one_batch_truth_boundary():
    producer = STORE.get("aria_block_producer")
    executor = STORE.get("aria_block_executor")
    assert producer.category == "block_producer"
    assert executor.category == "block_executor"
    assert producer.supported_backends == executor.supported_backends == ["real_cluster"]
    assert executor.default_config == {
        "worker_count": 4,
        "reordering": True,
        "read_only_optimization": True,
        "retry_nonce_gaps": True,
    }
    assert producer.default_config["candidate_scan_multiplier"] == 4
    assert "deterministic_reordering_rule2" in executor.capabilities
    assert "fifo_deferred_transactions" in producer.capabilities
    assert producer.truth_boundary == executor.truth_boundary == "aria_rule2_one_consensus_block_per_batch_fallback_disabled"
    assert executor.source["source_repository"] == "https://github.com/luyi0619/aria"
    assert executor.source["source_commit"] == "d0508c393ec084582c12e6f3abadab63501eaedd"


def test_aria_builtin_method_pairs_batch_producer_and_fixed_block_executor():
    method = BUILTIN_METHODS["hash_aria"]
    assert method.role == "baseline"
    assert method.plugin_overrides == {
        "routing": "hash_routing_baseline",
        "block_producer": "aria_block_producer",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "aria_block_executor",
        "commit": "normal_commit",
    }
    assert method.plugin_config_overrides["block_producer"]["reordering"] is True
    assert method.plugin_config_overrides["block_executor"]["reordering"] is True
    assert "maximum_epochs" not in method.plugin_config_overrides["block_executor"]


def test_aria_compatibility_requires_paired_plugins_and_matching_semantics():
    method = BUILTIN_METHODS["hash_aria"]
    categories = {
        "workload": "deterministic_signed_synthetic",
        "transaction_admission": "signature_nonce_admission",
        "txpool": "fifo_per_node_mempool",
        "sharding": "deterministic_state_key_sharding",
        "routing": "hash_routing_baseline",
        "block_producer": "aria_block_producer",
        "consensus": "pbft_style_consensus",
        "network": "localhost_tcp_typed_network",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "aria_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "cross_shard": "relay_certificate_protocol",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
        "metrics": "runtime_core_metrics",
        "observability": "node_network_consensus_observer",
    }
    selections = []
    for category, plugin_id in categories.items():
        config = dict(STORE.get(plugin_id).default_config)
        if category in method.plugin_config_overrides:
            config.update(method.plugin_config_overrides[category])
        selections.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    spec = V5ExperimentSpec(
        schema_version="v5_experiment_spec_v1",
        experiment_id="aria-compatibility",
        execution_backend="real_cluster",
        topology=V5Topology(nodes=4, shards=1, validators_per_shard=4),
        tx_count=100,
        duration_ms=1000,
        workload_source=None,
        plugin_selections=selections,
        fault_policy={},
    )
    assert validate(spec).valid is True
    next(item for item in spec.plugin_selections if item.category == "block_producer").config["reordering"] = False
    checked = validate(spec)
    assert checked.valid is False
    assert any("reordering must match" in reason for reason in checked.blockers)
