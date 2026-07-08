from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services import experiment_flow_service


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


def test_experiment_flow_methods_include_main_baseline_and_ablation() -> None:
    response = client.get("/api/experiment-flow/methods")
    assert response.status_code == 200
    method_ids = {item["method_id"] for item in response.json()["items"]}
    assert "metatrack_full" in method_ids
    assert "baseline_hash" in method_ids
    assert "metatrack_routing_only" in method_ids


def test_experiment_flow_preview_run_matrix_default_has_row() -> None:
    response = client.post("/api/experiment-flow/preview-run-matrix", json={})
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["rows"]) >= 1
    assert payload["runnable_row_count"] >= 1


def test_experiment_flow_preview_run_matrix_planned_workload_blocks_row() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={"workload_ids": ["real_skew_high"]},
    )
    assert response.status_code == 200
    row = response.json()["rows"][0]
    assert row["runnable"] is False
    assert row["warnings"]


def test_experiment_flow_preview_run_matrix_multiplies_dimensions() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={
            "selected_method_ids": ["metatrack_full", "baseline_hash"],
            "workload_ids": ["small_test", "blockemulator_sample"],
            "topology_ids": ["local_8_nodes_2_shards"],
            "seeds": [1, 2, 3],
        },
    )
    assert response.status_code == 200
    payload = response.json()
    assert len(payload["rows"]) == 2 * 2 * 1 * 3


def test_experiment_flow_derive_v4_realism_request_uses_defaults() -> None:
    response = client.post("/api/experiment-flow/derive-v4-realism-request", json={})
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is True
    request = payload["v4_request"]
    assert request["nodes"] == 8
    assert request["shards"] == 2
    assert request["fault_profile"] == "mixed_light"


def test_experiment_flow_derive_v4_realism_request_planned_workload_not_runnable() -> None:
    response = client.post(
        "/api/experiment-flow/derive-v4-realism-request",
        json={"workload_ids": ["real_skew_high"]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is False
    assert payload["warnings"]


def test_experiment_flow_execute_selected_matrix_dry_run_quick_validation() -> None:
    response = client.post(
        "/api/experiment-flow/execute-selected-matrix",
        json={"run_mode": "dry_run", "selected_rows": [_row()]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["run_group_id"].startswith("run_suite_")
    assert payload["selected_row_count"] == 1
    assert len(payload["child_runs"]) == 1
    assert payload["child_runs"][0]["status"] in {"dry_run", "preview_only"}


def test_experiment_flow_execute_selected_matrix_blocks_planned_workload() -> None:
    row = _row(workload_id="real_skew_high", runnable=False, warnings=["real_skew_high: dataset not attached yet"])
    response = client.post(
        "/api/experiment-flow/execute-selected-matrix",
        json={"run_mode": "execute", "selected_rows": [row]},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["blocked_row_count"] >= 1
    assert payload["child_runs"][0]["status"] == "blocked"
    assert payload["child_runs"][0]["blocked_reason"]


def test_experiment_flow_execute_selected_matrix_unsupported_formal_suite_is_preview_only() -> None:
    response = client.post(
        "/api/experiment-flow/execute-selected-matrix",
        json={"run_mode": "execute", "selected_rows": [_row(suite_type="main_experiment")]},
    )
    assert response.status_code == 200
    child = response.json()["child_runs"][0]
    assert child["status"] == "preview_only"
    assert "formal matrix execution bridge is planned" in child["warnings"][0]


def test_experiment_flow_execute_selected_matrix_v4_realism_uses_existing_runner(monkeypatch) -> None:
    def fake_run_smoke(payload):
        return {
            "run_id": "v4_fake_child",
            "status": "completed",
            "summary": {"ready_to_commit": True, "nodes": payload.nodes, "shards": payload.shards},
            "artifacts": [{"name": "v4_3_realism_final_summary.json", "download_url": "/api/v4/realism/runs/v4_fake_child/artifacts/v4_3_realism_final_summary.json", "size_bytes": 2}],
        }

    monkeypatch.setattr(experiment_flow_service.v4_realism_runner, "run_smoke", fake_run_smoke)
    response = client.post(
        "/api/experiment-flow/execute-selected-matrix",
        json={"run_mode": "execute", "selected_rows": [_row(suite_type="v4_realism_validation", runtime_target="v4.3")]},
    )
    assert response.status_code == 200
    child = response.json()["child_runs"][0]
    assert child["runner"] == "v4_realism_runner"
    assert child["status"] == "completed"
    assert child["run_id"] == "v4_fake_child"
    assert child["summary"]["ready_to_commit"] is True


def _row(**overrides) -> dict:
    row = {
        "row_id": "quick_validation:metatrack_full:small_test:local_8_nodes_2_shards:seed1",
        "suite_type": "quick_validation",
        "method_id": "metatrack_full",
        "method_role": "main",
        "workload_id": "small_test",
        "topology_id": "local_8_nodes_2_shards",
        "seed": 1,
        "runtime_target": "v3-formal-preview",
        "runnable": True,
        "warnings": [],
    }
    row.update(overrides)
    return row
