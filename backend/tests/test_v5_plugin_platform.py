from pathlib import Path

import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE
from backend.app.services.v5_saved_config_adapter import adapt_saved_method


def _spec() -> V5ExperimentSpec:
    selections = []
    for category in CATEGORIES:
        item = next(item for item in STORE.list() if item.category == category)
        selections.append(V5PluginSelection(category=category, plugin_id=item.plugin_id))
    return V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections, topology=V5Topology(nodes=8, shards=2, validators_per_shard=4), tx_count=100)


def test_real_cluster_spec_compiles_deterministically(tmp_path: Path) -> None:
    spec = _spec()
    first = compile_plan(spec, tmp_path)
    second = compile_plan(spec, tmp_path)
    assert first.plan_digest == second.plan_digest
    assert len(first.node_configs) == 8
    assert first.no_fallback is True


def test_real_cluster_rejects_missing_category() -> None:
    spec = _spec()
    spec.plugin_selections.pop()
    result = validate(spec)
    assert not result.valid
    assert any("missing required plugin category" in item for item in result.blockers)


def test_saved_config_adapter_reuses_v3_payload() -> None:
    selections, warnings = adapt_saved_method({"payload": {"module_plugins": {"routing": "hash_routing_baseline", "execution": "serial_execution_baseline"}}})
    assert len(selections) == len(CATEGORIES)
    assert not warnings
    block_executor = next(item for item in selections if item.category == "block_executor")
    assert block_executor.plugin_id == "serial_block_executor"
    assert block_executor.config["migrated_default"] is True


def test_block_executor_manifest_and_compiled_plan(tmp_path: Path) -> None:
    manifest = STORE.get("serial_block_executor")
    assert manifest.category == "block_executor"
    assert manifest.supported_backends == ["real_cluster"]
    assert manifest.truth_boundary == "legacy_faithful_reference_baseline"
    plan = compile_plan(_spec(), tmp_path)
    assert "block_execution_summary.json" in plan.expected_artifacts
    assert "execution_plan.jsonl" in plan.expected_artifacts
    assert all(node.plugin_profile["block_executor"]["plugin_id"] == "serial_block_executor" for node in plan.node_configs)
    assert plan.node_configs[0].plugin_profile["block_executor"]["migrated_default"] is False


def test_compiler_rejects_non_committee_topology(tmp_path: Path) -> None:
    spec = _spec()
    spec.topology.nodes = 7
    with pytest.raises(ValueError, match="nodes must equal"):
        compile_plan(spec, tmp_path)
