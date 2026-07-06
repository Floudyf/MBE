from __future__ import annotations

import zipfile
import csv
import json
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from backend.app import main
from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.models.v3_metatrack_formal_benchmark import V3FormalMetatrackBenchmarkRequest
from backend.app.services.job_manager import JobManager
from backend.app.services import v3_metatrack_formal_benchmark_runner as runner


def draft(topology: V3RuntimeTopology | None = None) -> V3ComposerDraftRequest:
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
    return V3ComposerDraftRequest(
        template_id="metatrack_ablation",
        preset_id="metatrack_full_smoke",
        topology=topology or V3RuntimeTopology(),
        modules={module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin) for module_id, (status, plugin) in plugins.items()},
    )


def request(**overrides) -> V3FormalMetatrackBenchmarkRequest:
    payload = {
        "draft": draft(),
        "formal_tx_count": 10000,
        "seed_count": 2,
        "baseline_ids": ["baseline_hash_serial", "metatrack_full"],
    }
    payload.update(overrides)
    return V3FormalMetatrackBenchmarkRequest(**payload)


def test_workload_comparison_matrix_contains_scenarios() -> None:
    req = request(experiment_type="workload_comparison", workload_scenario_points=["scene_hotspot", "cross_scene_migration"])
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is True
    assert preview["run_count"] == 8
    assert {row["scan_variable"] for row in preview["matrix"]} == {"workload_scenario"}
    assert {row["workload_scenario"] for row in preview["matrix"]} == {"scene_hotspot", "cross_scene_migration"}


def test_workload_comparison_profile_sets_metaverse_workload() -> None:
    req = request(experiment_type="workload_comparison", workload_scenario_points=["mixed_metaverse"])
    row = runner.preview_formal_metatrack_benchmark(req)["matrix"][0]
    profile = runner.build_formal_experiment_profile(req, row)

    assert profile["workload_source"] == "metaverse"
    assert profile["metaverse_scenario"] == "mixed_metaverse"


def test_formal_profile_inherits_topology_details() -> None:
    topology = V3RuntimeTopology(
        node_runtime_mode="local_multi_process",
        process_runtime_mode="dry_run",
        network_adapter="localhost_tcp_preview",
        cross_shard_protocol="relay_mvp",
        state_backend="merkle_trie_mvp",
        max_local_processes=3,
        enable_committee_epoch=True,
        epoch_count=3,
        shard_count=8,
        metaverse_scenario="scene_hotspot",
        workload_source="metaverse",
    )
    req = request(draft=draft(topology))
    row = runner.preview_formal_metatrack_benchmark(req)["matrix"][0]
    profile = runner.build_formal_experiment_profile(req, row)

    assert profile["network_adapter"] == "localhost_tcp_preview"
    assert profile["cross_shard_protocol"] == "relay_mvp"
    assert profile["state_backend"] == "merkle_trie_mvp"
    assert profile["max_local_processes"] == 3
    assert profile["enable_committee_epoch"] is True
    assert profile["epoch_count"] == 3


def test_saved_method_config_can_replace_baselines(monkeypatch) -> None:
    saved = {
        "v3cfg_method": {
            "config_id": "v3cfg_method",
            "config_kind": "method",
            "name": "Saved MetaTrack full",
            "payload": {"draft": runner.model_dump(draft())},
        }
    }
    monkeypatch.setattr(runner, "get_saved_config", lambda config_id: saved[config_id])
    req = request(baseline_ids=[], method_config_ids=["v3cfg_method"], seed_count=1)
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is True
    assert preview["run_count"] == 1
    assert preview["matrix"][0]["method_config_id"] == "v3cfg_method"
    assert preview["matrix"][0]["method_config_name"] == "Saved MetaTrack full"


def test_preview_warns_when_saved_method_metrics_report_needs_go_normalization(monkeypatch) -> None:
    saved_draft = draft()
    saved_draft.modules["MetricsReport"].plugin = "metatrack_metrics"
    saved = {
        "v3cfg_method": {
            "config_id": "v3cfg_method",
            "config_kind": "method",
            "name": "Saved MetaTrack metrics",
            "payload": {"draft": runner.model_dump(saved_draft)},
        }
    }
    monkeypatch.setattr(runner, "get_saved_config", lambda config_id: saved[config_id])

    preview = runner.preview_formal_metatrack_benchmark(request(baseline_ids=[], method_config_ids=["v3cfg_method"], seed_count=1))

    assert preview["is_runnable"] is True
    assert any("MetricsReport=metatrack_metrics" in warning for warning in preview["warnings"])


def test_preview_warns_when_saved_method_consensus_needs_go_normalization(monkeypatch) -> None:
    saved_draft = draft()
    saved_draft.modules["Consensus"].plugin = "blockemulator_aligned_pbft_preview"
    saved = {
        "v3cfg_method": {
            "config_id": "v3cfg_method",
            "config_kind": "method",
            "name": "Saved PBFT preview method",
            "payload": {"draft": runner.model_dump(saved_draft)},
        }
    }
    monkeypatch.setattr(runner, "get_saved_config", lambda config_id: saved[config_id])

    preview = runner.preview_formal_metatrack_benchmark(request(baseline_ids=[], method_config_ids=["v3cfg_method"], seed_count=1))

    assert preview["is_runnable"] is True
    assert any("Consensus=blockemulator_aligned_pbft_preview" in warning for warning in preview["warnings"])


def test_formal_plugin_profile_normalizes_go_runtime_compatibility_plugins() -> None:
    plugins = {
        "Workload": "synthetic_hotspot",
        "TxPool": "fifo_pool",
        "BlockProducer": "time_or_count_block_producer",
        "Consensus": "blockemulator_aligned_pbft_preview",
        "CommitteeEpoch": "disabled",
        "Routing": "metatrack_coaccess_routing",
        "Execution": "metatrack_dual_track_execution",
        "StateAccess": "access_list_prefetch",
        "StateStorage": "hash_state_storage",
        "Commit": "constraint_checked_aggregation",
        "MetricsReport": "metatrack_metrics",
    }

    profile = runner.build_formal_plugin_profile({"baseline_id": "saved_method", "plugins": plugins})
    generated = profile["profiles"][0]

    assert generated["plugins"]["MetricsPlugin"] == "basic_metrics"
    assert generated["plugins"]["ConsensusPlugin"] == "pbft_light_model"
    assert generated["plugins"]["ConsensusRuntimePlugin"] == "pbft_light_model"
    assert generated["module_plugins"]["MetricsReport"] == "basic_metrics"
    assert generated["module_plugins"]["Consensus"] == "pbft_light_model"
    assert generated["original_module_plugins"]["MetricsReport"] == "metatrack_metrics"
    assert generated["original_module_plugins"]["Consensus"] == "blockemulator_aligned_pbft_preview"
    assert any("MetricsReport=metatrack_metrics normalized to basic_metrics" in warning for warning in generated["compatibility_warnings"])
    assert any("Consensus=blockemulator_aligned_pbft_preview normalized to pbft_light_model" in warning for warning in generated["compatibility_warnings"])


def test_failure_summary_normalizes_repeated_go_runtime_errors() -> None:
    failed_runs = [
        {"status": "failed", "error": "Go V3 runtime failed: 2026/07/05 21:34:09 V3 Go runtime requires MetricsPlugin=basic_metrics exit status 1"},
        {"status": "failed", "error": "Go V3 runtime failed: 2026/07/05 21:35:10 V3 Go runtime requires MetricsPlugin=basic_metrics exit status 1"},
    ]

    summary = runner.build_failure_summary([], failed_runs)

    assert summary["failed_run_count"] == 2
    assert summary["top_errors"][0] == {"count": 2, "message": "V3 Go runtime requires MetricsPlugin=basic_metrics"}


def test_run_writes_progress_and_child_index(monkeypatch, tmp_path) -> None:
    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.json").write_text("{}\n", encoding="utf-8")
        return SimpleNamespace(summary={"throughput_tps": 10.0})

    monkeypatch.setattr(runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    result = runner.run_formal_metatrack_benchmark(request(seed_count=1), root=tmp_path)
    names = {artifact["name"] for artifact in result["artifacts"]}

    assert "formal_run_manifest.json" in names
    assert "formal_progress.json" in names
    assert "formal_failed_runs.csv" in names
    assert "formal_child_artifact_index.csv" in names
    assert "formal_chart_preview.json" in names
    assert "formal_metric_extraction_report.csv" in names
    assert "formal_missing_metrics.csv" in names
    assert "chart_preview" in result["summary"]
    assert result["summary"]["chart_preview"]["groups"]


def test_existing_trace_preview_is_not_formal_default() -> None:
    req = request(draft=draft(V3RuntimeTopology(workload_source="existing_trace_preview")))
    preview = runner.preview_formal_metatrack_benchmark(req)

    assert preview["is_runnable"] is False
    assert any("existing_trace_preview" in error for error in preview["errors"])


def test_formal_artifact_path_and_traversal_guard(tmp_path) -> None:
    manager = JobManager(tmp_path)
    metadata = manager.create_run(source="test", experiment_name="formal_test")
    run_dir = manager.run_dir(metadata["run_id"])
    (run_dir / "summary.json").write_text("{}\n", encoding="utf-8")

    path = runner.get_formal_artifact_path(metadata["run_id"], "summary.json", root=tmp_path)

    assert path == (run_dir / "summary.json").resolve()
    with pytest.raises(Exception):
        runner.get_formal_artifact_path(metadata["run_id"], "../summary.json", root=tmp_path)


def test_formal_zip_contains_only_allowlisted_files(tmp_path) -> None:
    manager = JobManager(tmp_path)
    metadata = manager.create_run(source="test", experiment_name="formal_test")
    run_dir = manager.run_dir(metadata["run_id"])
    (run_dir / "summary.json").write_text("{}\n", encoding="utf-8")
    (run_dir / "secret.txt").write_text("nope\n", encoding="utf-8")
    child = run_dir / "run_000_test"
    child.mkdir()
    (child / "summary.json").write_text("{}\n", encoding="utf-8")
    (child / "secret.txt").write_text("nope\n", encoding="utf-8")

    zip_path = runner.build_formal_artifacts_zip(metadata["run_id"], root=tmp_path)

    assert zip_path.is_file()
    with zipfile.ZipFile(zip_path) as archive:
        names = set(archive.namelist())
    assert "summary.json" in names
    assert "child_runs/run_000/summary.json" in names
    assert "secret.txt" not in names
    assert "child_runs/run_000/secret.txt" not in names


def test_chart_preview_uses_workload_scenario_and_skips_missing_metrics() -> None:
    aggregate_rows = [
        runner._aggregate_row("workload_comparison", "metatrack_full", "MetaTrack full", "metatrack_full", "", "", "", "", "workload_scenario", "scene_hotspot", "scene_hotspot", "throughput_tps", 123.0, 1.0, 122.0, 124.0, 3, 1.13, True),
        runner._aggregate_row("workload_comparison", "metatrack_full", "MetaTrack full", "metatrack_full", "", "", "", "", "workload_scenario", "scene_hotspot", "scene_hotspot", "avg_latency_ms", None, None, None, None, 0, None, False),
    ]
    figure_rows = runner.build_paper_figure_rows(aggregate_rows)
    chart = runner.build_chart_preview(aggregate_rows, figure_rows)

    assert chart["available_metrics"] == ["throughput_tps"]
    assert chart["groups"][0]["x"] == "scene_hotspot"
    assert chart["groups"][0]["series"] == "MetaTrack full"


def test_extract_child_metrics_reads_runtime_summary(tmp_path) -> None:
    row = {"run_index": 1}
    metrics, report = runner.extract_child_run_metrics(tmp_path, {"throughput_tps": 12.5}, row)

    assert metrics["throughput_tps"] == 12.5
    throughput_row = next(item for item in report if item["metric"] == "throughput_tps")
    assert throughput_row["source_file"] == "runtime_summary"
    assert throughput_row["source_field"] == "throughput_tps"


def test_extract_child_metrics_uses_alias(tmp_path) -> None:
    metrics, report = runner.extract_child_run_metrics(tmp_path, {"tps": "42.0"}, {"run_index": 2})

    assert metrics["throughput_tps"] == 42.0
    assert next(item for item in report if item["metric"] == "throughput_tps")["source_field"] == "tps"


def test_extract_child_metrics_calculates_latency_from_tx_results(tmp_path) -> None:
    (tmp_path / "tx_results.csv").write_text("tx_id,latency_ms,status\n1,10,success\n2,20,completed\n3,30,failed\n4,40,ok\n", encoding="utf-8")

    metrics, report = runner.extract_child_run_metrics(tmp_path, {}, {"run_index": 3})

    assert metrics["avg_latency_ms"] == pytest.approx((10 + 20 + 40) / 3)
    assert metrics["p95_latency_ms"] > 20
    assert metrics["p99_latency_ms"] > 20
    latency_row = next(item for item in report if item["metric"] == "avg_latency_ms")
    assert latency_row["source_file"] == "tx_results.csv"
    assert latency_row["source_field"] == "latency_ms"


def test_extract_child_metrics_derives_throughput(tmp_path) -> None:
    metrics, report = runner.extract_child_run_metrics(tmp_path, {"success_count": 50, "elapsed_ms": 5000}, {"run_index": 4})

    assert metrics["throughput_tps"] == 10.0
    assert next(item for item in report if item["metric"] == "throughput_tps")["source_field"] == "success_count/elapsed_ms"


def test_extract_child_metrics_does_not_fill_missing_with_zero(tmp_path) -> None:
    metrics, report = runner.extract_child_run_metrics(tmp_path, {}, {"run_index": 5})

    assert "throughput_tps" not in metrics
    missing = next(item for item in report if item["metric"] == "throughput_tps")
    assert missing["status"] == "missing"
    assert missing["value"] == ""


def test_aggregate_marks_metric_available() -> None:
    rows = [
        {"experiment_type": "workload_comparison", "baseline_id": "metatrack_full", "baseline_label": "MetaTrack full", "scan_variable": "workload_scenario", "scan_value": "scene_hotspot", "workload_scenario": "scene_hotspot", "throughput_tps": 10.0},
        {"experiment_type": "workload_comparison", "baseline_id": "metatrack_full", "baseline_label": "MetaTrack full", "scan_variable": "workload_scenario", "scan_value": "scene_hotspot", "workload_scenario": "scene_hotspot", "throughput_tps": 20.0},
    ]
    aggregate, _, missing, missing_rows = runner.aggregate_formal_results(rows)
    throughput = next(row for row in aggregate if row["metric"] == "throughput_tps")
    latency = next(row for row in aggregate if row["metric"] == "avg_latency_ms")

    assert throughput["count"] == 2
    assert throughput["metric_available"] is True
    assert latency["metric_available"] is False
    assert "avg_latency_ms" in missing
    assert any(row["metric"] == "avg_latency_ms" for row in missing_rows)


def test_chart_preview_has_diagnostics_without_available_metrics() -> None:
    aggregate = [
        runner._aggregate_row("workload_comparison", "metatrack_full", "MetaTrack full", "metatrack_full", "", "", "", "", "workload_scenario", "scene_hotspot", "scene_hotspot", "throughput_tps", None, None, None, None, 0, None, False)
    ]

    chart = runner.build_chart_preview(aggregate, [])

    assert chart["groups"] == []
    assert chart["diagnostics"]["reason"] == "no_available_aggregate_metrics"


def test_formal_workload_comparison_outputs_only_available_metrics(monkeypatch, tmp_path) -> None:
    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "tx_results.csv").write_text("tx_id,latency_ms,status\n1,10,success\n2,20,success\n", encoding="utf-8")
        (output_dir / "summary.json").write_text(json.dumps({"tps": 25, "success_count": 2, "elapsed_ms": 80}), encoding="utf-8")
        return SimpleNamespace(summary={})

    monkeypatch.setattr(runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    result = runner.run_formal_metatrack_benchmark(request(experiment_type="workload_comparison", seed_count=1, workload_scenario_points=["scene_hotspot"]), root=tmp_path)
    run_dir = tmp_path / result["run_id"]

    with (run_dir / "formal_workload_comparison.csv").open(encoding="utf-8", newline="") as handle:
        workload_rows = list(csv.DictReader(handle))
    with (run_dir / "formal_metric_extraction_report.csv").open(encoding="utf-8", newline="") as handle:
        extraction_rows = list(csv.DictReader(handle))

    assert workload_rows
    assert all(row["metric_available"] == "True" for row in workload_rows)
    assert any(row["metric"] == "throughput_tps" and row["source_field"] == "tps" for row in extraction_rows)
    assert (run_dir / "formal_metric_extraction_report.json").is_file()
    assert (run_dir / "formal_missing_metrics.csv").is_file()
    assert result["summary"]["chart_preview"]["groups"]


def test_list_and_get_formal_run_result(tmp_path) -> None:
    manager = JobManager(tmp_path)
    metadata = manager.create_run(source="test", experiment_name="formal_workload_comparison", extra_metadata={"experiment_type": "workload_comparison", "formal_tx_count": 1000, "run_count": 2})
    run_dir = manager.run_dir(metadata["run_id"])
    (run_dir / "summary.json").write_text('{"experiment_type":"workload_comparison","formal_tx_count":1000,"run_count":2,"completed_run_count":2,"failed_run_count":0,"method_count":1,"workload_count":2,"topology_count":1,"runtime_evidence_mode":"logical_single_process"}\n', encoding="utf-8")
    (run_dir / "formal_matrix_preview.json").write_text('{"run_count":2}\n', encoding="utf-8")
    (run_dir / "formal_chart_preview.json").write_text('{"groups":[]}\n', encoding="utf-8")

    runs = runner.list_formal_runs(root=tmp_path)
    detail = runner.get_formal_run_result(metadata["run_id"], root=tmp_path)

    assert runs[0]["run_id"] == metadata["run_id"]
    assert runs[0]["chart_preview_available"] is True
    assert detail["summary"]["experiment_type"] == "workload_comparison"
    assert detail["preview"]["run_count"] == 2


def test_formal_history_and_download_endpoints(monkeypatch) -> None:
    client = TestClient(main.app)
    monkeypatch.setattr(main, "list_formal_runs", lambda limit=20: [{"run_id": "run_1", "summary_available": True, "chart_preview_available": True}])
    monkeypatch.setattr(main, "get_formal_run_result", lambda run_id: {"run_id": run_id, "status": "completed", "run_mode": "formal_metatrack_benchmark", "output_dir": "", "summary": {}, "preview": {}, "artifacts": []})

    runs = client.get("/api/v3/composer/formal-metatrack/runs")
    detail = client.get("/api/v3/composer/formal-metatrack/runs/run_1")

    assert runs.status_code == 200
    assert runs.json()["runs"][0]["run_id"] == "run_1"
    assert detail.status_code == 200
    assert detail.json()["run_id"] == "run_1"


def test_formal_artifact_download_endpoints_have_download_filename(monkeypatch, tmp_path) -> None:
    client = TestClient(main.app)
    summary = tmp_path / "summary.json"
    summary.write_text("{}\n", encoding="utf-8")
    artifact_zip = tmp_path / "formal_artifacts.zip"
    with zipfile.ZipFile(artifact_zip, "w") as archive:
        archive.writestr("summary.json", "{}\n")
    monkeypatch.setattr(main, "get_formal_artifact_path", lambda run_id, filename: summary)
    monkeypatch.setattr(main, "build_formal_artifacts_zip", lambda run_id: artifact_zip)

    single = client.get("/api/v3/composer/formal-metatrack/run_1/artifacts/summary.json")
    zipped = client.get("/api/v3/composer/formal-metatrack/run_1/artifacts.zip")

    assert single.status_code == 200
    assert 'filename="summary.json"' in single.headers["content-disposition"]
    assert zipped.status_code == 200
    assert 'filename="formal_metatrack_results_run_1.zip"' in zipped.headers["content-disposition"]
