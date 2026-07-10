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


def test_experiment_flow_methods_can_exclude_saved(monkeypatch) -> None:
    monkeypatch.setattr(experiment_flow_service, "list_saved_configs", lambda kind: [_saved_method_config()])
    response = client.get("/api/experiment-flow/methods?include_saved=false")
    assert response.status_code == 200
    assert all(item["config_source"] == "builtin" for item in response.json()["items"])


def test_experiment_flow_methods_include_runnable_saved_method(monkeypatch) -> None:
    saved = _saved_method_config()
    monkeypatch.setattr(experiment_flow_service, "list_saved_configs", lambda kind: [saved])
    response = client.get("/api/experiment-flow/methods")
    assert response.status_code == 200
    method = next(item for item in response.json()["items"] if item["method_id"] == saved["config_id"])
    assert method["config_source"] == "saved_config"
    assert method["config_id"] == saved["config_id"]
    assert method["role"] == "main"
    assert method["runnable"] is True
    assert method["previewable"] is True


def test_experiment_flow_saved_method_role_and_validation_semantics(monkeypatch) -> None:
    baseline = _saved_method_config(config_id="v3cfg_test_baseline", tags=["baseline"], validation_status="valid")
    blocked = _saved_method_config(config_id="v3cfg_test_blocked", tags=["ablation"], validation_status="blocked")
    monkeypatch.setattr(experiment_flow_service, "list_saved_configs", lambda kind: [baseline, blocked])
    response = client.get("/api/experiment-flow/methods")
    methods = {item["method_id"]: item for item in response.json()["items"]}
    assert methods[baseline["config_id"]]["role"] == "baseline"
    assert methods[baseline["config_id"]]["previewable"] is True
    assert methods[baseline["config_id"]]["runnable"] is False
    assert methods[blocked["config_id"]]["previewable"] is False
    assert methods[blocked["config_id"]]["runnable"] is False


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


def test_experiment_flow_preview_matrix_resolves_saved_method(monkeypatch) -> None:
    saved = _saved_method_config(name="Saved MetaTrack Full")
    monkeypatch.setattr(experiment_flow_service, "get_saved_config", lambda config_id: saved)
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={"selected_method_ids": [saved["config_id"]]},
    )
    assert response.status_code == 200
    row = response.json()["rows"][0]
    assert row["method_config_id"] == saved["config_id"]
    assert row["resolved_method_name"] == "Saved MetaTrack Full"
    assert row["config_source"] == "saved_config"
    assert row["validation_status"] == "runnable"


def test_experiment_flow_preview_matrix_preset_topology_uses_catalog_values() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={
            "conditions": {
                "topology_mode": "preset",
                "topology_id": "local_8_nodes_2_shards",
                "nodes": 16,
                "shards": 4,
                "validators_per_shard": 4,
                "tx_count": 100,
                "repeat_count": 1,
            }
        },
    )
    assert response.status_code == 200
    row = response.json()["rows"][0]
    assert row["topology_mode"] == "preset"
    assert (row["nodes"], row["shards"], row["validators_per_shard"]) == (8, 2, 4)
    assert row["tx_count"] == 100


def test_experiment_flow_preview_matrix_custom_topology_and_repeat_expansion() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={
            "selected_method_ids": ["metatrack_full"],
            "workload_ids": ["small_test"],
            "seeds": [1, 2],
            "conditions": {
                "topology_mode": "custom",
                "nodes": 16,
                "shards": 4,
                "validators_per_shard": 4,
                "tx_count": 10000,
                "repeat_count": 3,
            },
        },
    )
    assert response.status_code == 200
    rows = response.json()["rows"]
    assert len(rows) == 1 * 1 * 1 * 2 * 3
    assert {row["repeat_index"] for row in rows} == {1, 2, 3}
    assert all(row["topology_mode"] == "custom" for row in rows)
    assert all(row["topology_id"] == "custom_16n_4s_4v" for row in rows)
    assert all((row["nodes"], row["shards"], row["validators_per_shard"]) == (16, 4, 4) for row in rows)
    assert all(row["tx_count"] == 10000 for row in rows)
    assert len({row["row_id"] for row in rows}) == len(rows)


def test_experiment_flow_preview_matrix_legacy_request_still_works() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={"topology_ids": ["local_4_nodes_1_shard", "local_8_nodes_2_shards"], "seeds": [1]},
    )
    assert response.status_code == 200
    assert {row["topology_id"] for row in response.json()["rows"]} == {"local_4_nodes_1_shard", "local_8_nodes_2_shards"}


def test_experiment_flow_preview_matrix_rejects_invalid_custom_topology() -> None:
    response = client.post(
        "/api/experiment-flow/preview-run-matrix",
        json={"conditions": {"topology_mode": "custom", "nodes": 2, "shards": 4, "validators_per_shard": 1}},
    )
    assert response.status_code == 400
    assert "cannot exceed node count" in response.json()["detail"]


def test_experiment_flow_preview_matrix_unvalidated_saved_method_is_blocked(monkeypatch) -> None:
    saved = _saved_method_config(validation_status="valid")
    monkeypatch.setattr(experiment_flow_service, "get_saved_config", lambda config_id: saved)
    response = client.post("/api/experiment-flow/preview-run-matrix", json={"selected_method_ids": [saved["config_id"]]})
    row = response.json()["rows"][0]
    assert row["runnable"] is False
    assert "method template is not validated as runnable." in row["warnings"]


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


def test_experiment_flow_derive_v4_realism_request_uses_custom_conditions() -> None:
    response = client.post(
        "/api/experiment-flow/derive-v4-realism-request",
        json={
            "conditions": {
                "topology_mode": "custom",
                "nodes": 8,
                "shards": 4,
                "validators_per_shard": 4,
                "tx_count": 100,
                "repeat_count": 2,
            }
        },
    )
    assert response.status_code == 200
    request = response.json()["v4_request"]
    assert (request["nodes"], request["shards"], request["tx_count"]) == (8, 4, 100)


def test_experiment_flow_derive_v4_realism_request_blocks_out_of_range_custom_conditions() -> None:
    response = client.post(
        "/api/experiment-flow/derive-v4-realism-request",
        json={"conditions": {"topology_mode": "custom", "nodes": 16, "shards": 4, "validators_per_shard": 4, "tx_count": 10000}},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["runnable"] is False
    assert any("outside V4 realism request range" in warning for warning in payload["warnings"])


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


def test_experiment_flow_execute_selected_matrix_accepts_extended_row_fields() -> None:
    response = client.post(
        "/api/experiment-flow/execute-selected-matrix",
        json={"run_mode": "dry_run", "selected_rows": [_row(
            topology_mode="custom",
            topology_id="custom_16n_4s_4v",
            nodes=16,
            shards=4,
            validators_per_shard=4,
            tx_count=100,
            repeat_index=2,
        )]},
    )
    assert response.status_code == 200
    assert response.json()["child_runs"][0]["status"] == "dry_run"


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
        "config_source": "builtin",
        "method_config_id": None,
        "resolved_method_name": "MetaTrack full",
        "validation_status": "runnable",
        "workload_id": "small_test",
        "topology_id": "local_8_nodes_2_shards",
        "topology_mode": "preset",
        "nodes": 8,
        "shards": 2,
        "validators_per_shard": 4,
        "tx_count": 20,
        "seed": 1,
        "repeat_index": 1,
        "runtime_target": "v3-formal-preview",
        "runnable": True,
        "warnings": [],
    }
    row.update(overrides)
    return row


def _saved_method_config(
    config_id: str = "v3cfg_test_saved_method",
    name: str = "Saved MetaTrack Full",
    tags: list[str] | None = None,
    validation_status: str = "runnable",
) -> dict:
    return {
        "config_id": config_id,
        "config_kind": "method",
        "name": name,
        "description": "Saved method for experiment-flow tests.",
        "tags": tags or ["main", "runnable"],
        "payload": {"modules": {"Routing": {"plugin": "metatrack_coaccess_routing"}}},
        "validation_status": validation_status,
        "source": "user_saved",
    }
