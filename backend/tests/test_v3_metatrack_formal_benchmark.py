from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.models.v3_metatrack_formal_benchmark import V3FormalMetatrackBenchmarkRequest
from backend.app.services import v3_metatrack_formal_benchmark_runner as runner
from backend.app.services.v3_metatrack_formal_baselines import METATRACK_FORMAL_BASELINES, validate_formal_baseline_registry


def draft(**overrides: tuple[str, str]) -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("variable", "metatrack_coaccess_routing"),
        "Execution": ("variable", "metatrack_dual_track_execution"),
        "StateAccess": ("variable", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("variable", "constraint_checked_aggregation"),
        "MetricsReport": ("output", "basic_metrics"),
    }
    plugins.update(overrides)
    return V3ComposerDraftRequest(
        template_id="metatrack_ablation",
        preset_id="metatrack_full_smoke",
        topology=V3RuntimeTopology(controlled_experiment_enabled=True),
        modules={module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin) for module_id, (status, plugin) in plugins.items()},
    )


def request(**overrides) -> V3FormalMetatrackBenchmarkRequest:
    payload = {
        "draft": draft(),
        "formal_tx_count": 10000,
        "seed_base": 42,
        "seed_count": 3,
        "baseline_ids": ["baseline_hash_serial", "metatrack_full"],
    }
    payload.update(overrides)
    return V3FormalMetatrackBenchmarkRequest(**payload)


def test_formal_baseline_registry_uses_runnable_catalog_plugins() -> None:
    validate_formal_baseline_registry()
    assert {"baseline_hash_serial", "metatrack_full"} <= set(METATRACK_FORMAL_BASELINES)


def test_ablation_matrix_run_count_and_seed_list() -> None:
    preview = runner.preview_formal_metatrack_benchmark(request(experiment_type="ablation"))

    assert preview["is_runnable"] is True
    assert preview["seed_list"] == [42, 43, 44]
    assert preview["baseline_count"] == 2
    assert preview["scan_point_count"] == 1
    assert preview["run_count"] == 6
    assert preview["total_tx_count"] == 60000


def test_hotspot_cross_shard_and_shard_scans_are_single_variable() -> None:
    hotspot = runner.preview_formal_metatrack_benchmark(request(experiment_type="hotspot_sensitivity", hotspot_ratio_points=[0.0, 0.5]))
    cross = runner.preview_formal_metatrack_benchmark(request(experiment_type="cross_shard_sensitivity", cross_shard_ratio_points=[0.0, 0.4, 0.6]))
    shard = runner.preview_formal_metatrack_benchmark(request(experiment_type="shard_scalability", shard_count_points=[1, 2, 4]))

    assert {row["scan_variable"] for row in hotspot["matrix"]} == {"hotspot_ratio"}
    assert hotspot["run_count"] == 12
    assert {row["scan_variable"] for row in cross["matrix"]} == {"cross_shard_ratio"}
    assert cross["run_count"] == 18
    assert {row["scan_variable"] for row in shard["matrix"]} == {"shard_count"}
    assert shard["run_count"] == 18


def test_resource_guards_reject_large_matrix_and_total_tx() -> None:
    too_many = runner.preview_formal_metatrack_benchmark(
        request(seed_count=10, baseline_ids=list(METATRACK_FORMAL_BASELINES), hotspot_ratio_points=[index / 100 for index in range(20)], experiment_type="hotspot_sensitivity")
    )
    too_much_tx = runner.preview_formal_metatrack_benchmark(request(formal_tx_count=1000000, seed_count=10, experiment_type="cross_shard_sensitivity"))

    assert too_many["is_runnable"] is False
    assert any("run_count" in error for error in too_many["errors"])
    assert too_much_tx["is_runnable"] is False
    assert any("total_tx_count" in error for error in too_much_tx["errors"])


def test_preview_plugin_is_rejected_for_formal_benchmark() -> None:
    preview = runner.preview_formal_metatrack_benchmark(request(draft=draft(Workload=("fixed", "existing_trace"))))

    assert preview["is_runnable"] is False
    assert preview["contains_preview_or_planned_plugin"] is True


def test_formal_experiment_profile_keeps_full_tx_count() -> None:
    req = request(formal_tx_count=25000)
    preview = runner.preview_formal_metatrack_benchmark(req)
    profile = runner.build_formal_experiment_profile(req, preview["matrix"][0])

    assert profile["tx_count"] == 25000
    assert profile["formal_tx_count"] == 25000


def test_aggregate_stats_and_missing_metrics() -> None:
    rows = [
        {"experiment_type": "ablation", "baseline_id": "metatrack_full", "scan_variable": "plugin_combination", "scan_value": "baseline", "throughput_tps": 100.0},
        {"experiment_type": "ablation", "baseline_id": "metatrack_full", "scan_variable": "plugin_combination", "scan_value": "baseline", "throughput_tps": 120.0},
    ]

    aggregate, ci_rows, missing = runner.aggregate_formal_results(rows)
    throughput = next(row for row in aggregate if row["metric"] == "throughput_tps")

    assert throughput["mean"] == 110.0
    assert throughput["count"] == 2
    assert throughput["ci95"] is not None
    assert ci_rows
    assert "avg_latency_ms" in missing


def test_paper_candidate_eligibility() -> None:
    req = request()
    preview = runner.preview_formal_metatrack_benchmark(req)
    raw_rows = [{**row, "status": "completed", "throughput_tps": 100 + row["run_index"]} for row in preview["matrix"]]
    aggregate, _, missing = runner.aggregate_formal_results(raw_rows)
    figures = runner.build_paper_figure_rows(aggregate)
    summary = runner.build_formal_summary(req, preview, raw_rows, aggregate, figures, missing)

    assert summary["paper_candidate_eligible"] is True
    assert summary["experiment_evidence_level"] == "paper_candidate"


def test_run_rejects_unrunnable_preview() -> None:
    with pytest.raises(runner.FormalBenchmarkNotRunnable):
        runner.run_formal_metatrack_benchmark(request(draft=draft(Workload=("fixed", "existing_trace"))), root=Path(".cache/test_formal_reject"))


def test_run_writes_formal_artifacts(monkeypatch, tmp_path: Path) -> None:
    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(summary={
            "throughput_tps": 100.0,
            "avg_latency_ms": 5.0,
            "p95_latency_ms": 8.0,
            "p99_latency_ms": 10.0,
            "avg_routing_overhead_ms": 1.0,
        })

    monkeypatch.setattr(runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    result = runner.run_formal_metatrack_benchmark(request(seed_count=1), root=tmp_path)

    assert result["status"] == "completed"
    assert result["summary"]["formal_tx_count"] == 10000
    names = {artifact["name"] for artifact in result["artifacts"]}
    assert "formal_benchmark_config.json" in names
    assert "formal_paper_figure_data.csv" in names
    assert "summary.json" in names
