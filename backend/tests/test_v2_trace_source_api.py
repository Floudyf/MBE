from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main


client = TestClient(main.app)


def test_trace_source_api_does_not_start_fabric_or_network(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(main, "V1_FABRIC_SMOKE_OUT", tmp_path)

    response = client.post("/api/v2/trace-sources/validate", json={"source_id": "fabric_chain_backed_trace"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "missing"
    combined = " ".join(payload["warnings"] + payload["limitations"]).lower()
    assert "never starts docker" in combined
    assert "network.sh" in combined


def test_trace_source_api_unknown_validate_source_returns_404() -> None:
    response = client.post("/api/v2/trace-sources/validate", json={"source_id": "not_real"})

    assert response.status_code == 404
