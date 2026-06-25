from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main

client = TestClient(main.app)


def test_chain_backends_api_lists_v2_and_v3_ready_backends() -> None:
    response = client.get("/api/v2/chain-backends")

    assert response.status_code == 200
    items = {item["backend_type"]: item for item in response.json()["items"]}
    assert items["local_virtual"]["status"] == "runnable"
    assert items["trace_replay"]["supports_replay"] is True
    assert items["fabric_live"]["status"] == "planned"
    assert items["evm_live"]["status"] == "planned"


def test_dual_chain_sample_config_api() -> None:
    response = client.get("/api/v2/dual-chain/sample-config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == "configs/experiments/v2_dual_chain_sample.yaml"
    assert payload["config"]["stage"] == "V2.5"
    assert payload["config"]["runnable"] is True


def test_dual_chain_replay_api_records_run_and_artifacts(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/dual-chain/replay", json={"config_path": "configs/experiments/v2_dual_chain_sample.yaml"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V2.5"
    assert payload["status"] == "completed"
    assert payload["data_truth_label"] == "synthetic_replay"
    assert payload["summary"]["cross_tx_count"] == 2
    assert {artifact["name"] for artifact in payload["artifacts"]} >= {"dual_chain_summary.json", "stage_metrics.csv", "runtime.log", "report.md"}

    run_id = payload["run_id"]
    detail = client.get(f"/api/v2/runs/{run_id}")
    artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")
    download = client.get(f"/api/v2/runs/{run_id}/artifacts/dual_chain_summary.json")

    assert detail.status_code == 200
    assert detail.json()["stage"] == "V2.5"
    assert artifacts.status_code == 200
    assert download.status_code == 200
    assert download.json()["cross_tx_count"] == 2


def test_dual_chain_replay_api_rejects_planned_config(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/dual-chain/replay", json={"config_path": "configs/topologies/v2_dual_chain_planned.yaml"})

    assert response.status_code == 400
    assert "planned" in response.text
