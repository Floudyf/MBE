from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE


client = TestClient(app)


def _payload() -> dict:
    selections = [{"category": category, "plugin_id": next(item.plugin_id for item in STORE.list() if item.category == category)} for category in CATEGORIES]
    return {"execution_backend": "preview", "plan": {"name": "formal preview", "base_spec": {"execution_backend": "real_cluster", "plugin_selections": selections, "topology": {"nodes": 8, "shards": 2, "validators_per_shard": 4}, "tx_count": 100, "seed": 7, "duration_ms": 9000}, "suites": ["main_experiment"], "methods": [{"method_id": "saved", "display_name": "Saved", "plugin_overrides": {}}], "seeds": [7], "repeats": 1}}


def test_v5_formal_preview_expands_persistent_shape_without_running_cluster():
    response = client.post("/api/v5/formal/preview", json=_payload())
    assert response.status_code == 200
    body = response.json()
    assert body["execution_backend"] == "preview"
    assert len(body["rows"]) == 1
    assert body["paper_candidate"] is False
