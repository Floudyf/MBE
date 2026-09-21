from pathlib import Path

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, _builtin_method_payload_matches
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _selections_for_porygon() -> list[V5PluginSelection]:
    overrides = ALL_BUILTIN_METHODS["stateless_porygon"].plugin_overrides
    config_overrides = ALL_BUILTIN_METHODS["stateless_porygon"].plugin_config_overrides
    out: list[V5PluginSelection] = []
    for category in CATEGORIES:
        default = next(item for item in STORE.list() if item.category == category)
        plugin_id = overrides.get(category, default.plugin_id)
        manifest = STORE.get(plugin_id)
        out.append(V5PluginSelection(category=category, plugin_id=plugin_id, config={**manifest.default_config, **config_overrides.get(category, {})}))
    workload = next(item for item in out if item.category == "workload")
    workload.plugin_id = "deterministic_signed_synthetic"
    workload.config = {"cross_shard_ratio": 0.0, "timeout_every": 0}
    return out


def test_porygon_builtin_method_is_registered_with_matched_scheduler_executor_config() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    assert method.display_name == "Porygon"
    assert method.role == "baseline"
    assert method.plugin_overrides["routing"] == "porygon_stateless_routing"
    assert method.plugin_overrides["scheduler"] == "porygon_pipeline_scheduler"
    assert method.plugin_overrides["block_executor"] == "porygon_block_executor"
    scheduler = method.plugin_config_overrides["scheduler"]
    executor = method.plugin_config_overrides["block_executor"]
    # MBE_PORYGON_UNIFIED_SHARD_V14_TEST_ALIGNMENT_20260921: the frontend
    # topology is authoritative for the execution-shard count; the builtin
    # method keeps only topology-independent Porygon protocol settings.
    for category in ("scheduler", "block_executor", "cross_shard"):
        assert "execution_shard_count" not in method.plugin_config_overrides.get(category, {})
    for key in ("execution_committee_count", "pipeline_enabled", "cross_batch_witness"):
        assert scheduler[key] == executor[key]

def test_porygon_manifests_are_real_cluster_plugins_and_do_not_claim_meta_remote_state() -> None:
    expected = {
        "porygon_stateless_routing": "routing",
        "porygon_transaction_block_producer": "block_producer",
        "porygon_execution": "execution",
        "porygon_pipeline_scheduler": "scheduler",
        "porygon_block_executor": "block_executor",
        "porygon_remote_state_access": "state_access",
        "porygon_cross_shard_coordinator": "cross_shard",
    }
    for plugin_id, category in expected.items():
        manifest = STORE.get(plugin_id)
        assert manifest.category == category
        assert manifest.supported_backends == ["real_cluster"]
        assert manifest.source.get("source_type") == "paper_reimplementation"
        assert manifest.source.get("doi") == "10.1109/ICDE60146.2024.00153"
    routing = STORE.get("porygon_stateless_routing")
    assert "batch_routing" not in routing.capabilities
    assert "remote_state_writeback" not in routing.capabilities
    state_access = STORE.get("porygon_remote_state_access")
    assert "remote_state_writeback" not in state_access.capabilities


def test_porygon_frontend_shards_are_supported_as_execution_shards() -> None:
    # The public topology knob remains shared with every method. Porygon maps
    # it to ESCs internally while retaining one global ordering domain.
    for shards in (1, 2, 4, 8):
        selections = _selections_for_porygon()
        spec = V5ExperimentSpec(
            name=f"porygon-{shards}-shards", execution_backend="real_cluster",
            plugin_selections=selections,
            topology=V5Topology(nodes=8, shards=shards, validators_per_shard=8 // shards),
            tx_count=20, seed=1, duration_ms=9000,
        )
        result = validate(spec)
        assert result.valid, result.blockers


def test_porygon_cross_shard_synthetic_workload_uses_frontend_execution_shard_count() -> None:
    one = _selections_for_porygon()
    one_workload = next(item for item in one if item.category == "workload")
    one_workload.config["cross_shard_ratio"] = 0.25
    one_spec = V5ExperimentSpec(
        name="porygon-cross-one", execution_backend="real_cluster",
        plugin_selections=one,
        topology=V5Topology(nodes=8, shards=1, validators_per_shard=8),
        tx_count=20, seed=1, duration_ms=9000,
    )
    one_result = validate(one_spec)
    assert not one_result.valid
    assert any("at least 2 execution shards" in item for item in one_result.blockers)

    two = _selections_for_porygon()
    two_workload = next(item for item in two if item.category == "workload")
    two_workload.config["cross_shard_ratio"] = 0.25
    two_spec = V5ExperimentSpec(
        name="porygon-cross-two", execution_backend="real_cluster",
        plugin_selections=two,
        topology=V5Topology(nodes=8, shards=2, validators_per_shard=4),
        tx_count=20, seed=1, duration_ms=9000,
    )
    two_result = validate(two_spec)
    assert two_result.valid, two_result.blockers


def test_porygon_does_not_change_legacy_first_plugin_defaults() -> None:
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


def test_porygon_manifests_remain_appended_after_legacy_manifests() -> None:
    items = STORE.list()
    first_porygon = min(i for i, item in enumerate(items) if item.plugin_id.startswith("porygon_"))
    last_legacy = max(i for i, item in enumerate(items) if not item.plugin_id.startswith("porygon_"))
    assert first_porygon > last_legacy


# MBE_PORYGON_UNIFIED_SHARD_V15_BUILTIN_PAYLOAD_PARITY_20260921
def test_porygon_builtin_payload_tolerates_only_semantically_empty_override_maps() -> None:
    expected = ALL_BUILTIN_METHODS["stateless_porygon"]
    candidate = expected.model_copy(deep=True)
    candidate.plugin_config_overrides = {**candidate.plugin_config_overrides, "cross_shard": {}}
    assert _builtin_method_payload_matches(candidate, expected)

    candidate.plugin_config_overrides["cross_shard"] = {"execution_shard_count": 4}
    assert not _builtin_method_payload_matches(candidate, expected)


def test_porygon_frontend_builtin_payload_omits_empty_cross_shard_override() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5MethodProfile.ts").read_text(encoding="utf-8")
    line = next(item for item in source.splitlines() if 'method_id: "stateless_porygon"' in item)
    assert 'cross_shard: {}' not in line


# MBE_PORYGON_ESC_OWNERSHIP_TIMING_TRUTH_V19_20260921
def test_porygon_builtin_uses_common_block_size_without_hidden_transaction_cap() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    producer = method.plugin_config_overrides.get("block_producer", {})
    assert "transaction_block_size" not in producer
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5MethodProfile.ts").read_text(encoding="utf-8")
    line = next(item for item in source.splitlines() if 'method_id: "stateless_porygon"' in item)
    assert "transaction_block_size" not in line
