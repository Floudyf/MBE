from __future__ import annotations

from collections import defaultdict

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalRunRequest
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, validate_request
from backend.app.services.v5_formal_scheduler import _spec_for
from backend.app.services.v5_paper_exporter import GROUP_FIELDS, PAPER_TABLE_FIELDS, _paper_table_rows
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE

WORKERS = [2, 4, 8, 16, 32]
WORKER_METHOD_IDS = ["hash_batch_si", "hash_cg", "hash_acg", "hash_bsx", "hash_aria", "hash_groundhog"]
ABLATION_METHOD_IDS = [
    "hash_batch_si",
    "hash_batch_si_no_wrbp",
    "hash_batch_si_no_ofas",
    "hash_batch_si_serial_batch",
    "hash_batch_si_txid_priority",
]


def _selections() -> list[V5PluginSelection]:
    out: list[V5PluginSelection] = []
    for category in CATEGORIES:
        plugin = next(item for item in STORE.list() if item.category == category)
        config = dict(plugin.default_config)
        if category == "workload":
            config.update({"cross_shard_ratio": 0.0, "timeout_every": 0})
        out.append(V5PluginSelection(category=category, plugin_id=plugin.plugin_id, config=config))
    return out


def _base_spec() -> V5ExperimentSpec:
    return V5ExperimentSpec(
        name="worker-scaling-closure",
        execution_backend="real_cluster",
        plugin_selections=_selections(),
        topology=V5Topology(nodes=8, shards=1, validators_per_shard=8),
        tx_count=100,
        seed=11,
        duration_ms=3_600_000,
    )


def _methods(ids: list[str]):
    return [ALL_BUILTIN_METHODS[item].model_copy(deep=True) for item in ids]


def test_topology_scaling_accepts_2_4_8_16_32_for_batch_si_and_five_parallel_baselines() -> None:
    plan = V5FormalExperimentPlan(
        name="worker-scaling",
        base_spec=_base_spec(),
        suites=["topology_scaling"],
        methods=_methods(WORKER_METHOD_IDS),
        seeds=[11],
        repeats=1,
        worker_count=4,
        topology_points=[
            {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": workers}
            for workers in WORKERS
        ],
    )
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert len(checked.rows) == len(WORKER_METHOD_IDS) * len(WORKERS)
    assert all(row["runnable"] for row in checked.rows)
    assert {row["method_config_id"] for row in checked.rows} == set(WORKER_METHOD_IDS)
    assert {row["topology_point"]["worker_count"] for row in checked.rows} == set(WORKERS)

    for row in checked.rows:
        spec = _spec_for(checked.plan, row)
        block_executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
        assert block_executor.config["worker_count"] == row["topology_point"]["worker_count"]


def test_worker_scaling_manifest_boundaries_cover_the_six_selected_methods_only() -> None:
    expected = {
        "batch_si_block_executor": 32,
        "cg_block_executor": 64,
        "acg_block_executor": 64,
        "bsx_block_executor": 64,
        "aria_block_executor": 32,
        "groundhog_block_executor": 32,
    }
    for plugin_id, maximum in expected.items():
        manifest = STORE.get(plugin_id)
        assert manifest.config_schema["properties"]["worker_count"]["maximum"] == maximum
    # Unrelated methods retain their prior resource contract.
    assert STORE.get("block_stm_block_executor").config_schema["properties"]["worker_count"]["maximum"] == 8
    assert STORE.get("metatrack_block_executor").config_schema["properties"]["worker_count"]["maximum"] == 8
    assert STORE.get("serial_block_executor").config_schema["properties"]["worker_count"]["maximum"] == 1


def test_existing_batch_si_ablation_suite_stays_on_the_registered_panel_and_propagates_workers() -> None:
    plan = V5FormalExperimentPlan(
        name="batch-si-ablation",
        base_spec=_base_spec(),
        suites=["ablation_experiment"],
        methods=_methods(ABLATION_METHOD_IDS),
        seeds=[11],
        repeats=1,
        worker_count=8,
    )
    checked = validate_request(V5FormalRunRequest(execution_backend="real_cluster", plan=plan))
    assert len(checked.rows) == 5
    assert {row["method_config_id"] for row in checked.rows} == set(ABLATION_METHOD_IDS)
    assert all(row["runnable"] for row in checked.rows)

    modes = {}
    for row in checked.rows:
        spec = _spec_for(checked.plan, row)
        block_executor = next(item for item in spec.plugin_selections if item.category == "block_executor")
        assert block_executor.plugin_id == "batch_si_block_executor"
        assert block_executor.config["worker_count"] == 8
        modes[row["method_config_id"]] = block_executor.config["execution_mode"]
    assert modes["hash_batch_si"] == "snapshot_parallel"
    assert modes["hash_batch_si_serial_batch"] == "snapshot_serial"


def test_scaling_exports_preserve_worker_count_as_a_first_class_column() -> None:
    assert "worker_count" in GROUP_FIELDS
    assert "worker_count" in PAPER_TABLE_FIELDS
    row = defaultdict(lambda: None)
    row.update(
        {
            "suite_type": "topology_scaling",
            "method_config_id": "hash_batch_si",
            "method_name": "Batch-SI",
            "method_role": "main",
            "scan_variable": "topology",
            "scan_value": '{"nodes":8,"shards":1,"validators_per_shard":8,"worker_count":32}',
            "topology_nodes": 8,
            "topology_shards": 1,
            "validators_per_shard": 8,
            "worker_count": 32,
        }
    )
    exported = _paper_table_rows([row])[0]
    assert exported["worker_count"] == 32
