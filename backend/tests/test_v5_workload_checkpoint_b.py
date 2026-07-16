from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology, V5WorkloadSourceSpec
from backend.app.models.v5_formal_experiment import V5FormalExperimentPlan, V5FormalMethod
from backend.app.services import v5_workload_data_plane as plane
from backend.app.services.v5_compatibility_engine import validate as validate_compatibility
from backend.app.services.v5_experiment_compiler import compile_plan
from backend.app.services.v5_formal_scheduler import _spec_for, expand
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


FIELDS = ["id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates"]


def write_source(root: Path, count: int = 4) -> tuple[Path, str]:
    path = root / "dcl_sales_workload_chain_ready.csv"
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        writer.writeheader()
        for index in range(count):
            writer.writerow(
                {
                    "id": f"sale-{index}",
                    "tx_hash": "0x" + f"{index:064x}",
                    "buyer": "0x" + f"{index % 2 + 1:040x}",
                    "seller": "0x" + f"{index + 10:040x}",
                    "price": str(index + 1),
                    "timestamp": str(1700000000000 + index),
                    "category": "wearable" if index % 2 else "emote",
                    "raw_contract_candidates": "0x" + f"{index % 3 + 100:040x}",
                }
            )
    return path, hashlib.sha256(path.read_bytes()).hexdigest()


def write_manifest(root: Path, source_sha: str, *, count: int = 4) -> Path:
    manifest_root = root / "data" / "workloads" / "manifests"
    manifest_root.mkdir(parents=True)
    manifest = {
        "schema_version": "mbe_dataset_manifest_v1",
        "dataset_id": "dcl_sales_polygon_271868",
        "display_name": "DCL fixture",
        "description": "fixture",
        "source_platform": "decentraland_marketplace",
        "source_chain": "polygon_mainnet",
        "dataset_type": "marketplace_sales",
        "included_categories": ["wearable", "emote"],
        "excluded_categories": ["land"],
        "local_raw_relative_path": "dcl_sales_workload_chain_ready.csv",
        "source_sha256": source_sha,
        "row_count": count,
        "unique_source_tx_hash_count": count,
        "time_start_ms": 1700000000000,
        "time_end_ms": 1700000000000 + count - 1,
        "category_counts": {"emote": 2, "wearable": 2},
        "truth_label": "real_observed",
        "verification_method": "representative",
        "verification_sample_count": 1,
        "verification_results": "fixture",
        "usage_note": "fixture",
        "generator_version": plane.GENERATOR_VERSION,
    }
    path = manifest_root / "dcl_sales_polygon_271868.json"
    path.write_text(json.dumps(manifest), encoding="utf-8")
    return manifest_root


@pytest.fixture()
def fixture_registry(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> dict[str, Path | str]:
    source, sha = write_source(tmp_path)
    manifest_root = write_manifest(tmp_path, sha)
    cache_root = tmp_path / ".cache" / "workloads"
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(plane, "WORKLOAD_CACHE_ROOT", cache_root)
    monkeypatch.setattr(plane, "SUPPORTED_COUNTS", frozenset({2, 4}))
    return {"root": tmp_path, "source": source, "sha": sha, "cache": cache_root}


def selections(workload_plugin: str = "deterministic_signed_synthetic") -> list[V5PluginSelection]:
    items = []
    for category in CATEGORIES:
        plugin_id = workload_plugin if category == "workload" else next(item.plugin_id for item in STORE.list() if item.category == category)
        config = {"cross_shard_ratio": 0.0, "timeout_every": 0} if category == "workload" and workload_plugin == "deterministic_signed_synthetic" else {}
        items.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    return items


def dataset_spec(source_sha: str) -> V5ExperimentSpec:
    return V5ExperimentSpec(
        name="dataset-b",
        execution_backend="real_cluster",
        plugin_selections=selections("canonical_trace_replay"),
        topology=V5Topology(nodes=4, shards=2, validators_per_shard=2),
        tx_count=4,
        seed=9,
        workload_source=V5WorkloadSourceSpec(
            source_type="dataset",
            plugin_id="canonical_trace_replay",
            dataset_id="dcl_sales_polygon_271868",
            variant_mode="original_window",
            requested_tx_count=4,
            use_full_dataset=True,
            seed=9,
            source_sha256=source_sha,
        ),
    )


def test_workload_registry_api_preview_materialize_and_redaction(fixture_registry: dict[str, Path | str]) -> None:
    client = TestClient(app)
    listed = client.get("/api/v5/workloads/datasets").json()
    assert listed[0]["selectable"] is True
    assert "dcl_sales_workload_chain_ready.csv" not in json.dumps(listed)
    detail = client.get("/api/v5/workloads/datasets/dcl_sales_polygon_271868").json()
    assert detail["dataset_id"] == "dcl_sales_polygon_271868"
    payload = {"source_type": "dataset", "plugin_id": "canonical_trace_replay", "dataset_id": "dcl_sales_polygon_271868", "requested_tx_count": 4, "use_full_dataset": True, "seed": 9, "variant_mode": "original_window", "source_sha256": fixture_registry["sha"]}
    preview = client.post("/api/v5/workloads/preview", json=payload).json()
    assert preview["blockers"] == [] and preview["materialization_cache_status"]["required"] is True
    materialized = client.post("/api/v5/workloads/materialize", json=payload).json()
    again = client.post("/api/v5/workloads/materialize", json=payload).json()
    assert materialized["actual_tx_count"] == 4 and again["cache_hit"] is True
    assert str(fixture_registry["root"]) not in json.dumps(materialized)


def test_unavailable_dataset_is_not_selectable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    sha = "0" * 64
    manifest_root = write_manifest(tmp_path, sha)
    monkeypatch.setattr(plane, "ROOT", tmp_path)
    monkeypatch.setattr(plane, "MANIFEST_ROOT", manifest_root)
    listed = TestClient(app).get("/api/v5/workloads/datasets").json()
    assert listed[0]["available"] is False and listed[0]["selectable"] is False


def test_workload_source_normalization_conflict_and_dataset_rejections(fixture_registry: dict[str, Path | str]) -> None:
    synthetic = V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections(), topology=V5Topology(nodes=4, shards=2, validators_per_shard=2), tx_count=10, seed=3)
    assert synthetic.workload_source and synthetic.workload_source.requested_tx_count == 10
    with pytest.raises(ValueError, match="top-level tx_count"):
        V5ExperimentSpec(execution_backend="real_cluster", plugin_selections=selections(), topology=V5Topology(nodes=4, shards=2, validators_per_shard=2), tx_count=10, seed=3, workload_source=V5WorkloadSourceSpec(requested_tx_count=9, seed=3))
    bad = dataset_spec(str(fixture_registry["sha"]))
    bad.plugin_selections = [item.model_copy(update={"config": {"cross_shard_ratio": 0.5}}) if item.category == "workload" else item for item in bad.plugin_selections]
    result = validate_compatibility(bad)
    assert not result.valid and any("cross_shard_ratio" in blocker for blocker in result.blockers)
    with pytest.raises(ValueError, match="original_window"):
        V5WorkloadSourceSpec(source_type="dataset", plugin_id="canonical_trace_replay", dataset_id="dcl_sales_polygon_271868", variant_mode="original_window", requested_tx_count=4, seed=1, source_sha256=str(fixture_registry["sha"]), target_alpha=1.0)


def test_compiler_materializes_dataset_and_formal_rows_share_hash(fixture_registry: dict[str, Path | str], tmp_path: Path) -> None:
    spec = dataset_spec(str(fixture_registry["sha"]))
    plan = compile_plan(spec, tmp_path / "compile")
    assert plan.workload_plan["plugin_id"] == "canonical_trace_replay"
    assert plan.workload_plan["source_type"] == "dataset"
    assert plan.workload_plan["dataset_id"] == "dcl_sales_polygon_271868"
    assert plan.workload_plan["variant_mode"] == "original_window"
    assert plan.workload_plan["truth_label"] == "real_observed"
    assert plan.workload_plan["materialized_id"]
    assert plan.workload_plan["materialized_sha256"]
    assert plan.workload_plan["base_window_sha256"]
    assert (tmp_path / "compile" / "workload_materialization_summary.json").is_file()
    formal = V5FormalExperimentPlan(
        name="dataset-formal",
        base_spec=spec,
        suites=["comparison_experiment"],
        methods=[
            V5FormalMethod(method_id="a", display_name="A", plugin_overrides={"routing": "hash_routing_baseline"}),
            V5FormalMethod(method_id="b", display_name="B", plugin_overrides={"routing": "metatrack_coaccess_routing"}),
        ],
        seeds=[9],
    )
    rows = expand(formal, "real_cluster")
    first = compile_plan(_spec_for(formal, rows[0]), tmp_path / "first")
    second = compile_plan(_spec_for(formal, rows[1]), tmp_path / "second")
    assert first.workload_plan["materialized_sha256"] == second.workload_plan["materialized_sha256"]


def test_dataset_child_id_includes_workload_identity(fixture_registry: dict[str, Path | str]) -> None:
    original = dataset_spec(str(fixture_registry["sha"]))
    derived = dataset_spec(str(fixture_registry["sha"]))
    assert derived.workload_source
    derived.workload_source.variant_mode = "contract_zipf"
    derived.workload_source.skew_axis = "contract"
    derived.workload_source.target_alpha = 1.0
    base = {
        "name": "dataset-formal",
        "suites": ["main_experiment"],
        "methods": [V5FormalMethod(method_id="same", display_name="Same")],
        "seeds": [11],
    }
    original_rows = expand(V5FormalExperimentPlan(base_spec=original, **base), "real_cluster")
    derived_rows = expand(V5FormalExperimentPlan(base_spec=derived, **base), "real_cluster")
    assert original_rows[0]["child_run_id"] != derived_rows[0]["child_run_id"]
    assert original_rows[0]["workload_snapshot_digest"] == derived_rows[0]["workload_snapshot_digest"]
