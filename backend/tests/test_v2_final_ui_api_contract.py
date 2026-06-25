from fastapi.testclient import TestClient

from backend.app import main

client = TestClient(main.app)


def test_v2_final_required_api_contracts_remain_available() -> None:
    endpoints = [
        "/api/v2/sweeps",
        "/api/v2/calibration/configs",
        "/api/v2/runs",
        "/api/v2/trace-sources",
        "/api/v2/chain-backends",
        "/api/v2/cross-chain/protocols",
    ]

    for endpoint in endpoints:
        response = client.get(endpoint)
        assert response.status_code == 200, endpoint


def test_v2_final_api_exposes_truth_and_backend_fields() -> None:
    sweeps = client.get("/api/v2/sweeps").json()["items"]
    calibrations = client.get("/api/v2/calibration/configs").json()["items"]
    trace_sources = client.get("/api/v2/trace-sources").json()["items"]
    backends = client.get("/api/v2/chain-backends").json()["items"]
    protocols = {item["name"]: item for item in client.get("/api/v2/cross-chain/protocols").json()["items"]}

    assert sweeps[0]["data_truth_label"]
    assert sweeps[0]["backend_type"]
    assert calibrations[0]["data_truth_label"]
    assert calibrations[0]["backend_type"]
    assert trace_sources[0]["data_truth_label"]
    assert backends[0]["backend_type"]
    assert protocols["metaflow"]["status"] == "planned"
