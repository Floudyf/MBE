from __future__ import annotations

import csv
import json
from pathlib import Path

from backend.app.models.v3_composer_draft import V3RuntimeTopology
from backend.app.services.artifact_manager import ARTIFACT_ALLOWLIST
from backend.app.services.v3_benchmark_templates import benchmark_template_ids
from backend.app.services.v3_metaverse_workloads import maybe_write_metaverse_suite_artifacts
from backend.app.services.v3_realism_readiness import build_realism_readiness
from backend.app.services.v3_runtime_topology import normalize_topology, stage_metadata


def test_v3_13_artifacts_are_downloadable() -> None:
    expected = {
        "metaverse_workload_catalog.json",
        "metaverse_workload_config.json",
        "metaverse_trace_meta.json",
        "scenario_summary.csv",
        "hotspot_distribution.csv",
        "cross_scene_transfer_log.csv",
        "offchain_confirmation_log.csv",
        "cross_metaverse_transfer_log.csv",
        "metaverse_experiment_summary.json",
        "baseline_matrix.csv",
        "multi_seed_summary.csv",
        "benchmark_suite_summary.json",
        "paper_table_latency.csv",
        "paper_table_throughput.csv",
        "paper_table_cross_shard.csv",
        "paper_table_offchain_confirmation.csv",
        "paper_figure_data.csv",
        "paper_export_manifest.json",
    }

    assert expected.issubset(ARTIFACT_ALLOWLIST)


def test_metaverse_benchmark_templates_are_registered() -> None:
    assert {
        "metaverse_mixed_template",
        "metaverse_asset_transfer_template",
        "metaverse_cross_scene_template",
        "metaverse_cross_metaverse_template",
    }.issubset(benchmark_template_ids())


def test_metaverse_suite_generator_writes_deterministic_artifacts(tmp_path: Path) -> None:
    topology, errors = normalize_topology(
        V3RuntimeTopology(
            metaverse_suite_enabled=True,
            metaverse_scenario="mixed_metaverse",
            tx_count=24,
            user_count=8,
            asset_count=16,
            item_count=12,
            avatar_count=8,
            scene_count=6,
            metaverse_count=2,
            cross_scene_ratio=0.5,
            cross_shard_ratio=0.5,
            offchain_failure_ratio=0.25,
            benchmark_suite_enabled=True,
            baseline_matrix_enabled=True,
            multi_seed_enabled=True,
            paper_export_enabled=True,
            sweep_seed_count=2,
            sweep_shard_counts=[1, 2],
            sweep_cross_shard_ratios=[0.0, 0.5],
            sweep_hotspot_ratios=[0.0],
        )
    )

    assert errors == []
    metrics = maybe_write_metaverse_suite_artifacts(tmp_path, topology, {"success_count": 24, "avg_latency_ms": 1})

    assert metrics["metaverse_tx_count"] == 24
    assert metrics["metaverse_scenario_selected"] == "mixed_metaverse"
    assert metrics["baseline_count"] == 15
    assert metrics["paper_figure_data_available"] is True
    summary = json.loads((tmp_path / "metaverse_experiment_summary.json").read_text(encoding="utf-8"))
    assert summary["truth_boundary"] == "controlled_metaverse_workload_not_real_platform_trace"
    with (tmp_path / "scenario_summary.csv").open(encoding="utf-8", newline="") as stream:
        scenario_rows = list(csv.DictReader(stream))
    assert {"asset_transfer", "avatar_update", "scene_hotspot", "cross_scene_migration", "onchain_offchain_confirmation", "cross_metaverse_transfer"} <= {row["scenario"] for row in scenario_rows}
    manifest = json.loads((tmp_path / "paper_export_manifest.json").read_text(encoding="utf-8"))
    assert manifest["truth"] == "paper_export_data_scaffold_not_paper_grade_conclusion"


def test_stage_metadata_and_readiness_reflect_v3_13_boundary() -> None:
    metadata = stage_metadata()
    readiness = build_realism_readiness()

    assert metadata["current_stage"] == "V3-final Fault, Observability, and Reproducibility Closure"
    assert metadata["runtime_truth"] == "v3_final_emulator_closure_not_production_system"
    assert readiness["next_stage"] == "V3 maintenance only; do not start V4 unless explicitly requested"
    assert "not real metaverse platform trace" in readiness["not_real_chain_claims"]
    assert "not paper-grade performance conclusion" in readiness["not_real_chain_claims"]
