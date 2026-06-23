from fastapi.testclient import TestClient

from backend.app.main import app


def test_default_composer_returns_complete_valid_v0_composition() -> None:
    response = TestClient(app).get("/api/v0/composer/default")

    assert response.status_code == 200
    payload = response.json()
    assert payload["composer"] == "default_composer"
    assert payload["valid"] is True
    assert payload["errors"] == []
    assert {item["type"] for item in payload["components"]} >= {
        "chain_backend", "workload", "trace", "clock", "metrics", "composer",
    }
