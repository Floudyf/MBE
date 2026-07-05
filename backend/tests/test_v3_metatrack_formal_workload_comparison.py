from __future__ import annotations

import zipfile
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
    assert "chart_preview" in result["summary"]


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
        runner._aggregate_row("workload_comparison", "metatrack_full", "MetaTrack full", "metatrack_full", "", "", "", "", "workload_scenario", "scene_hotspot", "scene_hotspot", "throughput_tps", 123.0, 1.0, 122.0, 124.0, 3, 1.13),
        runner._aggregate_row("workload_comparison", "metatrack_full", "MetaTrack full", "metatrack_full", "", "", "", "", "workload_scenario", "scene_hotspot", "scene_hotspot", "avg_latency_ms", None, None, None, None, 0, None),
    ]
    figure_rows = runner.build_paper_figure_rows(aggregate_rows)
    chart = runner.build_chart_preview(aggregate_rows, figure_rows)

    assert chart["available_metrics"] == ["throughput_tps"]
    assert chart["groups"][0]["x"] == "scene_hotspot"
    assert chart["groups"][0]["series"] == "MetaTrack full"


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
