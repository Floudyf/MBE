from __future__ import annotations

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan
from backend.app.services.v5_compatibility_engine import validate, validate_materialized_workload
from backend.app.services.v5_formal_plan_validator import BUILTIN_METHODS
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _selections() -> list[V5PluginSelection]:
    preferred = {
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
    out: list[V5PluginSelection] = []
    for category in CATEGORIES:
        manifest = STORE.get(preferred[category])
        config = dict(manifest.default_config)
        if category == "workload":
            config |= {"cross_shard_ratio": 0, "timeout_every": 0}
        if category == "block_producer":
            config |= {"block_size": 500, "interval_ms": 125}
        out.append(V5PluginSelection(category=category, plugin_id=manifest.plugin_id, config=config))
    return out


def _base_spec() -> V5ExperimentSpec:
    return V5ExperimentSpec(
        name="groundhog-plugin-test",
        execution_backend="real_cluster",
        plugin_selections=_selections(),
        topology=V5Topology(nodes=4, shards=1, validators_per_shard=4),
        tx_count=100,
        seed=7,
        duration_ms=9000,
    )


def test_groundhog_manifests_lock_paper_source_and_core_capabilities() -> None:
    producer = STORE.get("groundhog_block_producer")
    executor = STORE.get("groundhog_block_executor")
    assert producer.category == "block_producer"
    assert executor.category == "block_executor"
    assert producer.supported_backends == ["real_cluster"]
    assert executor.supported_backends == ["real_cluster"]
    assert "groundhog_block_assembly" in producer.capabilities
    assert "typed_commutative_modifications" in executor.capabilities
    assert "no_serial_fallback" in executor.capabilities
    assert producer.source["source_repository"] == "https://github.com/scslab/smart-contract-scalability"
    assert executor.source["source_commit"] == "6b357bc206b73ece39fd61fe7dba655352200c0a"


def test_groundhog_builtin_method_selects_paired_plugins() -> None:
    method = BUILTIN_METHODS["hash_groundhog"]
    assert method.display_name == "Groundhog"
    assert method.role == "baseline"
    assert method.plugin_overrides == {
        "routing": "hash_routing_baseline",
        "block_producer": "groundhog_block_producer",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "groundhog_block_executor",
        "commit": "normal_commit",
    }
    assert method.plugin_config_overrides["block_executor"]["worker_count"] == 4
    assert method.plugin_config_overrides["block_producer"]["ordered_set_limit"] == 64
    assert method.plugin_config_overrides["block_executor"]["ordered_set_limit"] == 64
    assert STORE.get("groundhog_block_producer").default_config["ordered_set_limit"] == 64
    assert STORE.get("groundhog_block_executor").default_config["ordered_set_limit"] == 64
    assert STORE.get("groundhog_block_producer").config_schema["properties"]["ordered_set_limit"]["maximum"] == 65535


def test_groundhog_method_preserves_experiment_block_size_and_interval() -> None:
    plan = V5FormalExperimentPlan(
        name="groundhog-fairness",
        base_spec=_base_spec(),
        methods=[BUILTIN_METHODS["hash_groundhog"]],
        suites=["main_experiment"],
        seeds=[7],
        repeats=1,
    )
    rows = expand(plan, "real_cluster")
    assert len(rows) == 1
    spec = _spec_for(plan, rows[0])
    producer = next(item for item in spec.plugin_selections if item.category == "block_producer")
    assert producer.plugin_id == "groundhog_block_producer"
    assert producer.config["block_size"] == 500
    assert producer.config["interval_ms"] == 125
    assert producer.config["candidate_scan_multiplier"] == 4


def test_groundhog_compatibility_accepts_core_profile_and_rejects_unsafe_combinations() -> None:
    plan = V5FormalExperimentPlan(
        name="groundhog-compatibility",
        base_spec=_base_spec(),
        methods=[BUILTIN_METHODS["hash_groundhog"]],
        suites=["main_experiment"],
        seeds=[7],
        repeats=1,
    )
    row = expand(plan, "real_cluster")[0]
    spec = _spec_for(plan, row)
    result = validate(spec)
    assert result.valid, result.blockers
    assert any("comparison-limited" in item for item in result.warnings)

    unpaired = spec.model_copy(deep=True)
    next(item for item in unpaired.plugin_selections if item.category == "block_executor").plugin_id = "serial_block_executor"
    unpaired_result = validate(unpaired)
    assert not unpaired_result.valid
    assert any("must be selected together" in item for item in unpaired_result.blockers)

    mismatched_limits = spec.model_copy(deep=True)
    next(item for item in mismatched_limits.plugin_selections if item.category == "block_producer").config["ordered_set_limit"] = 32
    next(item for item in mismatched_limits.plugin_selections if item.category == "block_executor").config["ordered_set_limit"] = 64
    mismatched_result = validate(mismatched_limits)
    assert not mismatched_result.valid
    assert any("ordered_set_limit must match" in item for item in mismatched_result.blockers)

    over_limit = spec.model_copy(deep=True)
    next(item for item in over_limit.plugin_selections if item.category == "block_producer").config["ordered_set_limit"] = 65536
    next(item for item in over_limit.plugin_selections if item.category == "block_executor").config["ordered_set_limit"] = 65536
    over_limit_result = validate(over_limit)
    assert not over_limit_result.valid
    assert any("between 1 and 65535" in item for item in over_limit_result.blockers)

    for topology in (
        V5Topology(nodes=8, shards=2, validators_per_shard=4),
        V5Topology(nodes=32, shards=8, validators_per_shard=4),
        V5Topology(nodes=64, shards=2, validators_per_shard=32),
    ):
        multishard = spec.model_copy(deep=True)
        multishard.topology = topology
        multishard_result = validate(multishard)
        assert multishard_result.valid, (topology, multishard_result.blockers)

    cross_shard = spec.model_copy(deep=True)
    next(item for item in cross_shard.plugin_selections if item.category == "workload").config["cross_shard_ratio"] = 0.25
    cross_result = validate(cross_shard)
    assert not cross_result.valid
    assert any("cross_shard_ratio=0" in item for item in cross_result.blockers)


def test_groundhog_materialized_guard_uses_only_exact_cross_shard_evidence() -> None:
    plan = V5FormalExperimentPlan(
        name="groundhog-materialized-guard",
        base_spec=_base_spec(),
        methods=[BUILTIN_METHODS["hash_groundhog"]],
        suites=["main_experiment"],
        seeds=[7],
        repeats=1,
    )
    spec = _spec_for(plan, expand(plan, "real_cluster")[0])
    compatibility = validate(spec)
    assert compatibility.valid, compatibility.blockers
    profile = {item.category: {"plugin_id": item.plugin_id, "config": item.config} for item in compatibility.resolved_plugins}

    # Python preview uses source identifiers and can differ from the exact Go
    # runtime-identity mapping. It remains advisory and must not block alone.
    preview_only = {
        "source_type": "dataset",
        "tx_count": 1000,
        "topology_preview_cross_shard_count": 631,
    }
    assert validate_materialized_workload(spec, profile, preview_only) == []

    exact = {
        "source_type": "dataset",
        "tx_count": 1000,
        "actual_cross_shard_count": 465,
    }
    blockers = validate_materialized_workload(spec, profile, exact)
    assert len(blockers) == 1
    assert "465 cross-shard transactions" in blockers[0]
    assert "shard-local transactions only" in blockers[0]


def test_metatrack_manifest_exposes_paper_default_batch_routing_semantics() -> None:
    manifest = STORE.get("metatrack_coaccess_routing")
    assert manifest.default_config == {"routing_epoch": 0}
    assert manifest.truth_boundary == "metatrack_batch_execution_sharding_admissible_capacity_v2"
    assert "batch_frequency_coaccess" in manifest.capabilities
    assert "admissible_state_load_capacity" in manifest.capabilities
    assert "per_seed_top_cooccur_budget" in manifest.capabilities
    assert "majority_place_queue_tie" in manifest.capabilities
    assert "routing_epoch_evidence" in manifest.capabilities
    assert "sender_group_affinity" not in manifest.capabilities
    assert "routing_epoch_stability" not in manifest.capabilities
    assert "predicted_remote_access_accounting" in manifest.capabilities


def test_groundhog_manifest_declares_concurrent_reservation_fidelity():
    executor = STORE.get("groundhog_block_executor")
    assert "concurrent_reserve_commit_rollback" in executor.capabilities
    assert "transactional_reservation_rewind" in executor.capabilities
    assert "concurrent_transactional_reserve_revert_commit" in executor.truth_boundary
    metric_keys = {row.key for row in executor.metrics}
    assert "groundhog_reservation_parallel_width" in metric_keys
