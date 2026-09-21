from pathlib import Path

import pytest

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


def _porygon_spec(shards: int) -> V5ExperimentSpec:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    selections: list[V5PluginSelection] = []
    for category in CATEGORIES:
        manifest = next(item for item in STORE.list() if item.category == category)
        plugin_id = method.plugin_overrides.get(category, manifest.plugin_id)
        chosen = STORE.get(plugin_id)
        config = {**chosen.default_config, **method.plugin_config_overrides.get(category, {})}
        if category == "workload":
            plugin_id = "deterministic_signed_synthetic"
            chosen = STORE.get(plugin_id)
            config = {**chosen.default_config, "cross_shard_ratio": 0.0, "timeout_every": 0}
        selections.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    return V5ExperimentSpec(
        name=f"porygon-unified-{shards}", execution_backend="real_cluster",
        plugin_selections=selections,
        topology=V5Topology(nodes=8, shards=shards, validators_per_shard=8 // shards),
        tx_count=20, seed=17, duration_ms=9000,
    )


def test_porygon_frontend_two_shards_compile_to_two_execution_shards_one_ordering_domain(tmp_path: Path) -> None:
    plan = compile_plan(_porygon_spec(2), tmp_path / "two")
    nodes = plan.node_configs
    assert len(nodes) == 8
    assert {node.shard_id for node in nodes} == {"porygon-global"}
    assert {node.consensus_domain_id for node in nodes} == {"porygon-global"}
    assert {node.execution_shard_id for node in nodes} == {"s0", "s1"}
    assert [node.node_id for node in nodes if node.leader] == ["n0"]
    assert all(node.validators == [f"n{i}" for i in range(8)] for node in nodes)
    assert [node.node_id for node in nodes if node.execution_shard_id == "s0"] == ["n0", "n1", "n2", "n3"]
    assert [node.node_id for node in nodes if node.execution_shard_id == "s1"] == ["n4", "n5", "n6", "n7"]
    profile = nodes[0].plugin_profile
    assert profile["scheduler"]["config"]["execution_shard_count"] == 2
    assert profile["block_executor"]["config"]["execution_shard_count"] == 2
    assert profile["cross_shard"]["config"]["execution_shard_count"] == 2


def test_porygon_frontend_four_shards_bind_four_execution_shards(tmp_path: Path) -> None:
    plan = compile_plan(_porygon_spec(4), tmp_path / "four")
    assert {node.execution_shard_id for node in plan.node_configs} == {"s0", "s1", "s2", "s3"}
    assert {node.shard_id for node in plan.node_configs} == {"porygon-global"}
    assert plan.node_configs[0].plugin_profile["scheduler"]["config"]["execution_shard_count"] == 4


def test_porygon_builtin_no_longer_hardcodes_execution_shard_count() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    for category in ("scheduler", "block_executor", "cross_shard"):
        assert "execution_shard_count" not in method.plugin_config_overrides.get(category, {})


@pytest.mark.parametrize("shards", [1, 2, 4, 8])
def test_porygon_frontend_shard_sweep_maps_exactly_to_execution_shards(shards: int, tmp_path: Path) -> None:
    plan = compile_plan(_porygon_spec(shards), tmp_path / f"sweep-{shards}")
    expected = {f"s{i}" for i in range(shards)}
    assert {node.execution_shard_id for node in plan.node_configs} == expected
    assert {node.shard_id for node in plan.node_configs} == {"porygon-global"}
    assert {node.consensus_domain_id for node in plan.node_configs} == {"porygon-global"}
    assert [node.node_id for node in plan.node_configs if node.leader] == ["n0"]
    assert plan.node_configs[0].plugin_profile["scheduler"]["config"]["execution_shard_count"] == shards
    member_counts = {sid: sum(node.execution_shard_id == sid for node in plan.node_configs) for sid in expected}
    assert set(member_counts.values()) == {8 // shards}
