from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def test_stateful_calvin_is_registered_as_literature_baseline():
    method = ALL_BUILTIN_METHODS["stateful_calvin"]
    assert method.display_name == "Calvin"
    assert method.role == "baseline"
    assert method.plugin_overrides["routing"] == "calvin_global_routing"
    assert method.plugin_overrides["block_executor"] == "calvin_block_executor"
    assert method.plugin_overrides["state_access"] == "calvin_partition_state_access"
    assert method.plugin_overrides["state_storage"] == "calvin_partition_state_store"
    assert method.plugin_overrides["cross_shard"] == "calvin_no_2pc_coordinator"


def test_stateless_calvin_is_explicit_compatibility_adaptation():
    method = ALL_BUILTIN_METHODS["stateless_calvin"]
    assert method.display_name == "Stateless Calvin"
    assert method.role == "compatibility"
    assert method.plugin_overrides["routing"] == "stateless_calvin_global_routing"
    assert method.plugin_overrides["block_executor"] == "stateless_calvin_block_executor"
    assert method.plugin_overrides["state_access"] == "stateless_calvin_state_access"
    assert method.plugin_overrides["state_storage"] == "calvin_partition_state_store"


def test_calvin_plugins_are_visible_in_catalog():
    expected = {
        "calvin_global_routing",
        "stateless_calvin_global_routing",
        "calvin_declared_access_admission",
        "calvin_execution",
        "calvin_deterministic_scheduler",
        "calvin_block_executor",
        "stateless_calvin_block_executor",
        "calvin_partition_state_access",
        "stateless_calvin_state_access",
        "calvin_partition_state_store",
        "calvin_no_2pc_coordinator",
    }
    for plugin_id in expected:
        manifest = STORE.get(plugin_id)
        assert manifest.implementation_status == "implemented"
        assert "real_cluster" in manifest.supported_backends


def test_existing_builtin_profiles_keep_their_registered_identity():
    expected = {
        "hash_serial": ("hash_routing_baseline", "serial_block_executor"),
        "hash_block_stm": ("hash_routing_baseline", "block_stm_block_executor"),
        "hash_aria": ("hash_routing_baseline", "aria_block_executor"),
        "hash_groundhog": ("hash_routing_baseline", "groundhog_block_executor"),
        "hash_fabricpp_cg": ("hash_routing_baseline", "fabricpp_cg_block_executor"),
        "hash_cg": ("hash_routing_baseline", "cg_block_executor"),
        "hash_acg": ("hash_routing_baseline", "acg_block_executor"),
        "hash_bsx": ("hash_routing_baseline", "bsx_block_executor"),
        "hash_batch_si": ("hash_routing_baseline", "batch_si_block_executor"),
        "stateless_hash_block_stm": ("stateless_hash_routing", "block_stm_block_executor"),
        "stateless_porygon": ("porygon_stateless_routing", "porygon_block_executor"),
        "metatrack_serial": ("metatrack_coaccess_routing", "metatrack_block_executor"),
    }
    for method_id, (routing, executor) in expected.items():
        method = ALL_BUILTIN_METHODS[method_id]
        assert method.plugin_overrides["routing"] == routing
        assert method.plugin_overrides["block_executor"] == executor


def test_calvin_timeouts_default_to_context_driven_waits():
    for plugin_id in ("calvin_block_executor", "stateless_calvin_block_executor"):
        manifest = STORE.get(plugin_id)
        assert manifest.default_config["read_result_timeout_ms"] == 0
        assert manifest.default_config["outcome_timeout_ms"] == 0


def test_stateless_calvin_manifest_does_not_claim_zero_state_storage():
    manifest = STORE.get("stateless_calvin_state_access")
    assert "declared_state_projection" in manifest.capabilities
    storage = STORE.get("calvin_partition_state_store")
    assert "persistent_partition_state" in storage.capabilities

def test_calvin_registration_does_not_change_any_legacy_first_plugin_default():
    expected_first = {
        "workload": "deterministic_signed_synthetic",
        "transaction_admission": "signature_nonce_admission",
        "txpool": "fifo_per_node_mempool",
        "sharding": "deterministic_state_key_sharding",
        "routing": "hash_routing_baseline",
        "block_producer": "time_or_count_block_producer",
        "consensus": "pbft_style_consensus",
        "network": "localhost_tcp_typed_network",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "serial_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "cross_shard": "relay_certificate_protocol",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
        "metrics": "runtime_core_metrics",
        "observability": "node_network_consensus_observer",
    }
    first: dict[str, str] = {}
    for manifest in STORE.list():
        first.setdefault(manifest.category, manifest.plugin_id)
    assert first == expected_first


def test_non_calvin_canonical_profile_is_not_subject_to_calvin_requirements():
    selections = []
    for category in CATEGORIES:
        manifest = next(item for item in STORE.list() if item.category == category)
        selections.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id))
    spec = V5ExperimentSpec(
        execution_backend="real_cluster",
        plugin_selections=selections,
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=100,
    )
    result = validate(spec)
    assert result.valid, result.blockers
    assert not any("Calvin" in blocker for blocker in result.blockers)


def test_partial_non_executor_calvin_selection_does_not_reclassify_unrelated_executor():
    # Plugin composition may transiently contain an optional Calvin plugin while
    # the selected executor is still a legacy method. Calvin's method-wide
    # compatibility contract is activated by its executor, not substring match.
    selections = []
    for category in CATEGORIES:
        manifest = next(item for item in STORE.list() if item.category == category)
        plugin_id = "calvin_partition_state_access" if category == "state_access" else manifest.plugin_id
        selections.append(V5PluginSelection(category=category, plugin_id=plugin_id))
    spec = V5ExperimentSpec(
        execution_backend="real_cluster",
        plugin_selections=selections,
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=100,
    )
    result = validate(spec)
    assert not any("Calvin requires" in blocker for blocker in result.blockers)



def test_calvin_manifests_remain_between_legacy_defaults_and_porygon_append_only_block():
    items = STORE.list()
    first_porygon = min(i for i, item in enumerate(items) if item.plugin_id.startswith("porygon_"))
    calvin_indexes = [
        i
        for i, item in enumerate(items)
        if item.plugin_id.startswith("calvin_") or item.plugin_id.startswith("stateless_calvin_")
    ]
    assert calvin_indexes
    assert max(calvin_indexes) < first_porygon

    expected_first = {
        "workload": "deterministic_signed_synthetic",
        "transaction_admission": "signature_nonce_admission",
        "txpool": "fifo_per_node_mempool",
        "sharding": "deterministic_state_key_sharding",
        "routing": "hash_routing_baseline",
        "block_producer": "time_or_count_block_producer",
        "consensus": "pbft_style_consensus",
        "network": "localhost_tcp_typed_network",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "serial_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "cross_shard": "relay_certificate_protocol",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
        "metrics": "runtime_core_metrics",
        "observability": "node_network_consensus_observer",
    }
    first = {}
    for manifest in items:
        first.setdefault(manifest.category, manifest.plugin_id)
    assert first == expected_first
