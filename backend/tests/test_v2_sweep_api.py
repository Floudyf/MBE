from fastapi.testclient import TestClient

from backend.app import main

client = TestClient(main.app)


def test_v2_sweeps_api_lists_configs() -> None:
    response = client.get("/api/v2/sweeps")

    assert response.status_code == 200
    items = {item["id"]: item for item in response.json()["items"]}
    assert items["v2_baseline_sweep"]["stage"] == "V2.8"
    assert items["v2_baseline_sweep"]["data_truth_label"] == "synthetic_replay"
    assert items["v2_baseline_sweep"]["backend_type"] == "local_virtual"
    assert items["v2_baseline_sweep"]["protocol_truth"] == "local_baseline_model"


def test_v2_sweep_detail_api() -> None:
    response = client.get("/api/v2/sweeps/v2_protocol_baseline_sweep")

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == "configs/sweeps/v2_protocol_baseline_sweep.yaml"
    assert "lock_mint_serial" in payload["summary"]["protocols"]


def test_v2_sweep_run_api_records_run_and_artifacts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/sweeps/run", json={"sweep_id": "v2_baseline_sweep"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V2.8"
    assert payload["status"] == "completed"
    assert payload["summary"]["sweep_id"] == "v2_baseline_sweep"
    assert payload["data_truth_label"] == "synthetic_replay"
    assert payload["backend_type"] == "local_virtual"
    assert payload["protocol_truth"] == "local_baseline_model"
    assert {artifact["name"] for artifact in payload["artifacts"]} >= {"sweep_summary.csv", "sweep_summary.json", "sweep_report.md", "runtime.log", "case_artifacts_index.json"}

    run_id = payload["run_id"]
    artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")
    download = client.get(f"/api/v2/runs/{run_id}/artifacts/sweep_summary.json")

    assert artifacts.status_code == 200
    assert download.status_code == 200
    assert download.json()["summary"]["sweep_id"] == "v2_baseline_sweep"


def test_v2_sweep_run_api_rejects_unknown_sweep(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/sweeps/run", json={"sweep_id": "metaflow"})

    assert response.status_code == 400
