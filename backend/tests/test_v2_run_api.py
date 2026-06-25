from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main
from backend.app.services.job_manager import JobManager


client = TestClient(main.app)


def test_v2_run_api_lists_gets_and_downloads_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)
    manager = JobManager(tmp_path)
    run = manager.create_run("v1_custom_run", "v1_custom_interactive", "synthetic_replay")
    run_id = run["run_id"]
    (tmp_path / run_id / "summary.csv").write_text("tx_count\n1\n", encoding="utf-8")
    manager.mark_completed(run_id)

    runs = client.get("/api/v2/runs")
    latest = client.get("/api/v2/runs/latest")
    detail = client.get(f"/api/v2/runs/{run_id}")
    artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")
    download = client.get(f"/api/v2/runs/{run_id}/artifacts/summary.csv")

    assert runs.status_code == 200
    assert runs.json()["items"][0]["run_id"] == run_id
    assert latest.status_code == 200
    assert latest.json()["run_id"] == run_id
    assert detail.status_code == 200
    assert detail.json()["status"] == "completed"
    assert artifacts.status_code == 200
    assert artifacts.json()["artifacts"][0]["name"] == "summary.csv"
    assert download.status_code == 200
    assert "tx_count" in download.text


def test_v2_run_api_rejects_bad_artifact_requests(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)
    manager = JobManager(tmp_path)
    run = manager.create_run("v1_custom_run", "v1_custom_interactive", "synthetic_replay")
    run_id = run["run_id"]

    assert client.get(f"/api/v2/runs/{run_id}/artifacts/secret.txt").status_code == 403
    assert client.get(f"/api/v2/runs/{run_id}/artifacts/latency.csv").status_code == 404
    assert client.get("/api/v2/runs/missing-run").status_code == 404
