from fastapi.testclient import TestClient
import yaml

from backend.app import main
from backend.app.services.calibration_runner_v2 import CALIBRATION_CONFIGS

client = TestClient(main.app)


def test_v2_calibration_configs_api() -> None:
    response = client.get("/api/v2/calibration/configs")

    assert response.status_code == 200
    items = {item["id"]: item for item in response.json()["items"]}
    assert items["v2_synthetic_calibration_sample"]["data_truth_label"] == "synthetic_replay"
    assert items["v2_fabric_smoke_calibration"]["data_truth_label"] == "fabric_chain_backed_trace_replay"


def test_v2_calibration_fabric_status_api_only_checks() -> None:
    response = client.get("/api/v2/calibration/fabric-smoke/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["web_starts_fabric"] is False
    assert "cli_command" in payload


def test_v2_calibration_run_api_records_job_and_artifacts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/calibration/run", json={"config_id": "v2_synthetic_calibration_sample"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V2.9"
    assert payload["status"] == "completed"
    assert payload["data_truth_label"] == "synthetic_replay"
    assert payload["backend_type"] == "local_virtual"
    assert payload["calibration_truth"] == "synthetic_observation_sample"
    assert {artifact["name"] for artifact in payload["artifacts"]} >= {"calibration_summary.csv", "calibration_summary.json", "replay_vs_observed.csv", "calibration_report.md", "runtime.log"}

    run_id = payload["run_id"]
    artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")
    download = client.get(f"/api/v2/runs/{run_id}/artifacts/calibration_summary.json")

    assert artifacts.status_code == 200
    assert download.status_code == 200
    assert download.json()["calibration_id"] == "v2_synthetic_calibration_sample"


def test_v2_fabric_calibration_run_blocks_when_missing(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)
    config = yaml.safe_load(CALIBRATION_CONFIGS["v2_fabric_smoke_calibration"].read_text(encoding="utf-8"))
    config["input"]["trace_file"] = ".cache/test_v2_api_missing_fabric/trace.jsonl.gz"
    config["input"]["meta_file"] = ".cache/test_v2_api_missing_fabric/trace_meta.json"
    path = main.ROOT / ".cache/test_v2_api_missing_fabric_config.yaml"
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    response = client.post("/api/v2/calibration/run", json={"config_path": ".cache/test_v2_api_missing_fabric_config.yaml"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] in {"blocked", "failed"}
    assert payload["reason"] == "Fabric smoke trace missing"
    assert "scripts/v1_fabric_smoke.py" in payload["cli_command"]
