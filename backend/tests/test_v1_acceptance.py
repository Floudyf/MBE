from __future__ import annotations

import json
import subprocess

from fastapi.testclient import TestClient

from backend.app import main


client = TestClient(main.app)


def test_v1_status_reports_completed_phases() -> None:
    response = client.get("/api/v1/status")

    assert response.status_code == 200
    stages = {item["id"]: item["status"] for item in response.json()["stages"]}
    assert stages["v1_4_fabric_chain_backed_trace"] == "completed_cli_only"
    assert stages["v1_5_co_access_routing"] == "completed"
    assert stages["v1_6_dual_track_execution"] == "completed"
    assert stages["v1_7_hot_update_aggregation"] == "completed"
    assert stages["v1_8_baseline_sweep_report"] == "completed"


def test_v1_sweep_summary_and_report_are_clear_before_run(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V1_SWEEP_OUT", tmp_path)

    summary = client.get("/api/v1/sweep/summary")
    report = client.get("/api/v1/sweep/report")
    files = client.get("/api/v1/sweep/files")

    assert summary.status_code == 200
    assert summary.json()["status"] == "not_run"
    assert summary.json()["rows"] == []
    assert report.status_code == 200
    assert report.json()["status"] == "not_run"
    assert files.status_code == 200
    assert files.json()["files"] == []


def test_v1_sweep_run_uses_local_sweep_script_without_fabric_or_docker(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V1_SWEEP_OUT", tmp_path)
    captured: dict[str, object] = {}

    def fake_run(command, cwd, text, capture_output):  # noqa: ANN001
        captured["command"] = command
        captured["cwd"] = cwd
        tmp_path.mkdir(parents=True, exist_ok=True)
        (tmp_path / "sweep_summary.json").write_text(json.dumps([{"name": "baseline_hash_only"}]), encoding="utf-8")
        (tmp_path / "sweep_summary.csv").write_text("name\nbaseline_hash_only\n", encoding="utf-8")
        (tmp_path / "report.md").write_text("# report\n", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")

    monkeypatch.setattr(main.subprocess, "run", fake_run)

    response = client.post("/api/v1/sweep/run")

    assert response.status_code == 200
    command = " ".join(str(part) for part in captured["command"])
    assert "scripts" in command and "v1_8_sweep.py" in command
    assert all(forbidden not in command.lower() for forbidden in ("docker", "fabric", "network.sh", "peer "))
    assert response.json()["files"]

