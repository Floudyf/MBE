from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

import backend.app.main as main
from backend.app.services import v3_draft_run_history as history


client = TestClient(main.app)


def test_empty_draft_run_history_returns_empty_list(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)

    response = client.get("/api/v3/composer/draft-runs")

    assert response.status_code == 200
    assert response.json() == {"runs": []}


def test_draft_run_history_lists_complete_run(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)
    write_run(tmp_path / "v2run_20260628_000001_abcd")

    response = client.get("/api/v3/composer/draft-runs")

    assert response.status_code == 200
    runs = response.json()["runs"]
    assert len(runs) == 1
    assert runs[0]["run_id"] == "v2run_20260628_000001_abcd"
    assert runs[0]["template_id"] == "metatrack_ablation"
    assert runs[0]["selected_plugins"]["Routing"] == "co_access_sharding"
    assert runs[0]["artifact_count"] > 0
    assert runs[0]["preset_id"] == "legacy/default smoke"


def test_draft_run_detail_returns_artifacts_and_profiles(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)
    write_run(tmp_path / "v2run_20260628_000002_abcd")

    response = client.get("/api/v3/composer/draft-runs/v2run_20260628_000002_abcd")

    assert response.status_code == 200
    payload = response.json()
    assert payload["run_id"] == "v2run_20260628_000002_abcd"
    assert payload["normalized_draft"]["plugin_selection"]["Execution"] == "dual_track_execution"
    assert payload["validation"]["is_runnable"] is True
    assert payload["generated_experiment_profile"]["type"] == "draft_smoke"
    assert payload["artifact_groups"]
    assert payload["summary_preview"]["tx_count"] == "24"


def test_draft_run_history_reads_preset_metadata(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)
    run_dir = tmp_path / "v2run_20260628_000004_abcd"
    write_run(run_dir)
    normalized = json.loads((run_dir / "normalized_draft.json").read_text(encoding="utf-8"))
    normalized.update({
        "template_id": "single_module_txpool",
        "experiment_template": "single_module_txpool",
        "preset_id": "txpool_fifo_smoke",
        "preset_name": "FIFO TxPool smoke",
        "variable_module": "TxPool",
        "fairness_validated": True,
        "expected_artifacts": ["txpool_log.csv", "summary.json"],
    })
    write_json(run_dir / "normalized_draft.json", normalized)
    write_json(run_dir / "summary.json", {
        "tx_count": 24,
        "experiment_template": "single_module_txpool",
        "preset_id": "txpool_fifo_smoke",
        "preset_name": "FIFO TxPool smoke",
        "variable_module": "TxPool",
        "fairness_validated": True,
        "expected_artifacts": ["txpool_log.csv", "summary.json"],
    })

    response = client.get("/api/v3/composer/draft-runs")

    assert response.status_code == 200
    run = response.json()["runs"][0]
    assert run["template_id"] == "single_module_txpool"
    assert run["preset_id"] == "txpool_fifo_smoke"
    assert run["variable_module"] == "TxPool"
    assert run["summary_preview"]["preset_id"] == "txpool_fifo_smoke"


def test_draft_run_detail_missing_files_does_not_crash(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)
    run_dir = tmp_path / "v2run_20260628_000003_abcd"
    run_dir.mkdir(parents=True)
    write_json(run_dir / "normalized_draft.json", {"template_id": "metatrack_ablation", "run_mode": "draft_smoke", "plugin_selection": {"Routing": "hash_sharding"}})

    response = client.get("/api/v3/composer/draft-runs/v2run_20260628_000003_abcd")

    assert response.status_code == 200
    payload = response.json()
    assert "composer_draft.json" in payload["missing_files"]
    assert "runtime.log" in payload["missing_files"]


def test_draft_run_history_rejects_path_traversal(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)

    response = client.get("/api/v3/composer/draft-runs/..%2F..%2Fsecret")

    assert response.status_code in {400, 404}


def test_draft_run_history_missing_run_returns_404(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(history, "V3_DRAFT_RUNS_ROOT", tmp_path)

    response = client.get("/api/v3/composer/draft-runs/v2run_missing")

    assert response.status_code == 404


def write_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True)
    write_json(run_dir / "composer_draft.json", {"template_id": "metatrack_ablation", "modules": {}})
    write_json(run_dir / "normalized_draft.json", {
        "template_id": "metatrack_ablation",
        "run_mode": "draft_smoke",
        "plugin_selection": {
            "Routing": "co_access_sharding",
            "Execution": "dual_track_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "hot_update_aggregation_commit",
        },
        "variable_modules": ["Routing", "Execution", "StateAccess", "Commit"],
        "fixed_modules": ["Workload", "TxPool", "Consensus"],
        "disabled_modules": ["CommitteeEpoch"],
        "output_modules": ["MetricsReport"],
    })
    write_json(run_dir / "draft_validation.json", {"is_valid": True, "is_runnable": True, "run_mode": "draft_smoke"})
    write_json(run_dir / "generated_experiment_profile.json", {"type": "draft_smoke", "tx_count": 24})
    write_json(run_dir / "generated_plugin_profile.json", {"profiles": [{"plugin_profile_id": "composer_draft_single"}]})
    (run_dir / "summary.csv").write_text("tx_count,success_count,failure_count,avg_latency_ms,p95_latency_ms,p99_latency_ms\n24,24,0,3,5,7\n", encoding="utf-8")
    (run_dir / "latency.csv").write_text("tx_id,latency_ms\n0,3\n", encoding="utf-8")
    (run_dir / "runtime.log").write_text("draft smoke complete\n", encoding="utf-8")


def write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
