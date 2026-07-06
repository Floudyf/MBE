from __future__ import annotations

from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_v4_realism_status_truth_labels() -> None:
    response = client.get("/api/v4/realism/status")
    assert response.status_code == 200
    payload = response.json()
    assert payload["runtime_truth"] == "v4_real_state_cross_shard_recovery"
    assert payload["production_pbft"] is False
    assert payload["full_byzantine_security"] is False


def test_v4_realism_smoke_happy_path() -> None:
    response = client.post(
        "/api/v4/realism/smoke",
        json={"nodes": 4, "shards": 1, "tx_count": 4, "enable_cross_shard": True, "enable_faults": True, "run_duration_ms": 100},
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "completed"
    assert payload["summary"]["ready_to_commit"] is True
    assert payload["summary"]["production_blockchain"] is False
    assert payload["artifacts"]
