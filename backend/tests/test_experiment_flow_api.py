from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_experiment_flow_profiles_include_v4_default() -> None:
    response = client.get("/api/experiment-flow/profiles")
    assert response.status_code == 200
    items = response.json()["items"]
    assert any(item["profile_id"] == "v4_3_realism_default" for item in items)


def test_experiment_flow_topologies_include_recommended_topology() -> None:
    response = client.get("/api/experiment-flow/topologies")
    assert response.status_code == 200
    items = response.json()["items"]
    assert any(item["topology_id"] == "local_8_nodes_2_shards" for item in items)


def test_experiment_flow_workloads_include_small_test() -> None:
    response = client.get("/api/experiment-flow/workloads")
    assert response.status_code == 200
    items = response.json()["items"]
    assert any(item["workload_id"] == "small_test" for item in items)


def test_experiment_flow_recommended_run_matches_v4_validation_defaults() -> None:
    response = client.get("/api/experiment-flow/recommended-run")
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is True
    request = payload["recommended_v4_request"]
    assert request["nodes"] == 8
    assert request["shards"] == 2
    assert request["tx_count"] == 20
    assert request["fault_profile"] == "mixed_light"


def test_experiment_flow_preview_small_test_is_runnable() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-plan",
        json={"profile_id": "v4_3_realism_default", "topology_id": "local_8_nodes_2_shards", "workload_id": "small_test"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is True
    assert payload["warnings"] == []


def test_experiment_flow_preview_planned_real_workload_is_not_runnable() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-plan",
        json={"profile_id": "v4_3_realism_default", "topology_id": "local_8_nodes_2_shards", "workload_id": "real_skew_high"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is False
    assert payload["warnings"]
    assert "dataset not attached yet" in payload["warnings"][0]


def test_experiment_flow_preview_blockemulator_csv_requires_path() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-plan",
        json={"profile_id": "v4_3_realism_default", "topology_id": "local_8_nodes_2_shards", "workload_id": "blockemulator_csv"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is False
    assert payload["warnings"]


def test_experiment_flow_preview_four_shards_returns_warning() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-plan",
        json={"profile_id": "v4_3_realism_default", "topology_id": "local_8_nodes_4_shards", "workload_id": "small_test"},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is True
    assert payload["warnings"]

