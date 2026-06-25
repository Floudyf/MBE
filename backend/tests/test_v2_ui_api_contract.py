from fastapi.testclient import TestClient

from backend.app.main import app

client = TestClient(app)


def test_trace_sources_contract_contains_ui_fields() -> None:
    response = client.get("/api/v2/trace-sources")

    assert response.status_code == 200
    source = response.json()["items"][0]
    for field in ("id", "label", "status", "data_truth_label", "entry_mode", "capabilities", "limitations"):
        assert field in source


def test_chain_backends_contract_contains_ui_fields_and_planned_live_backends() -> None:
    response = client.get("/api/v2/chain-backends")

    assert response.status_code == 200
    backends = {item["backend_type"]: item for item in response.json()["items"]}
    assert backends["local_virtual"]["status"] == "runnable"
    assert "data_truth_label" in backends["local_virtual"]
    assert backends["local_virtual"]["supports_virtual_time"] is True
    assert backends["fabric_live"]["status"] == "planned"
    assert backends["fabric_live"]["supports_real_time"] is False
    assert backends["evm_live"]["status"] == "planned"


def test_cross_chain_protocols_contract_keeps_metaflow_planned() -> None:
    response = client.get("/api/v2/cross-chain/protocols")

    assert response.status_code == 200
    protocols = {item["name"]: item for item in response.json()["items"]}
    assert protocols["lock_mint_serial"]["status"] == "runnable"
    assert protocols["committee_bridge_basic"]["maturity"] == "experimental"
    assert protocols["metaflow"]["status"] == "planned"


def test_sample_config_contracts_are_available() -> None:
    dual = client.get("/api/v2/dual-chain/sample-config")
    protocol = client.get("/api/v2/cross-chain/sample-config")

    assert dual.status_code == 200
    assert dual.json()["path"] == "configs/experiments/v2_dual_chain_sample.yaml"
    assert dual.json()["config"]["data_truth_label"] == "synthetic_replay"
    assert protocol.status_code == 200
    assert protocol.json()["path"] == "configs/experiments/v2_cross_chain_protocol_sample.yaml"
    assert protocol.json()["config"]["trace"]["data_truth_label"] == "synthetic_replay"


def test_runs_contract_is_available_and_artifact_urls_are_api_paths() -> None:
    response = client.get("/api/v2/runs")

    assert response.status_code == 200
    assert "items" in response.json()
    for run in response.json()["items"][:3]:
        artifacts = client.get(f"/api/v2/runs/{run['run_id']}/artifacts")
        assert artifacts.status_code == 200
        for artifact in artifacts.json()["artifacts"]:
            assert artifact["download_url"].startswith("/api/v2/runs/")
            assert ".cache" not in artifact["download_url"]
            assert "\\" not in artifact["download_url"]
