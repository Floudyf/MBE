from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main

client = TestClient(main.app)


def test_cross_chain_protocols_api_keeps_metaflow_planned() -> None:
    response = client.get("/api/v2/cross-chain/protocols")

    assert response.status_code == 200
    items = {item["name"]: item for item in response.json()["items"]}
    assert items["lock_mint_serial"]["status"] == "runnable"
    assert items["committee_bridge_basic"]["maturity"] == "experimental"
    assert items["metaflow"]["status"] == "planned"


def test_cross_chain_sample_config_api() -> None:
    response = client.get("/api/v2/cross-chain/sample-config")

    assert response.status_code == 200
    payload = response.json()
    assert payload["path"] == "configs/experiments/v2_cross_chain_protocol_sample.yaml"
    assert payload["config"]["stage"] == "v2.6"
    assert payload["config"]["experiment"]["runnable"] is True


def test_cross_chain_protocol_replay_api_records_run(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/cross-chain/protocol-replay", json={"config_path": "configs/experiments/v2_cross_chain_protocol_sample.yaml"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["stage"] == "V2.6"
    assert payload["status"] == "completed"
    assert payload["data_truth_label"] == "synthetic_replay"
    assert payload["protocol_truth"] == "local_baseline_model"
    assert {item["name"] for item in payload["artifacts"]} >= {"protocol_summary.json", "protocol_results.csv", "protocol_events.csv"}

    run_id = payload["run_id"]
    detail = client.get(f"/api/v2/runs/{run_id}")
    artifacts = client.get(f"/api/v2/runs/{run_id}/artifacts")
    download = client.get(f"/api/v2/runs/{run_id}/artifacts/protocol_summary.json")

    assert detail.status_code == 200
    assert detail.json()["stage"] == "V2.6"
    assert artifacts.status_code == 200
    assert download.status_code == 200
    assert "items" in download.json()


def test_cross_chain_protocol_replay_api_rejects_planned_topology(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V2_JOBS_ROOT", tmp_path)

    response = client.post("/api/v2/cross-chain/protocol-replay", json={"config_path": "configs/topologies/v2_dual_chain_planned.yaml"})

    assert response.status_code == 400
