from fastapi.testclient import TestClient

from backend.app.main import app
import json

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


def test_v5_formal_artifact_catalog_reads_only_real_manifest_and_bundle(tmp_path, monkeypatch):
    group_id = "v5grp_catalog_test"
    directory = tmp_path / group_id
    directory.mkdir()
    (directory / "run_group.json").write_text(json.dumps({"run_group_id": group_id}), encoding="utf-8")
    monkeypatch.setattr("backend.app.api.v5_formal_experiments.group_dir", lambda value: directory if value == group_id else (_ for _ in ()).throw(ValueError(value)))
    monkeypatch.setattr("backend.app.api.v5_formal_experiments.read_group", lambda value: {"run_group_id": group_id} if value == group_id else (_ for _ in ()).throw(FileNotFoundError(value)))

    pending = client.get(f"/api/v5/formal/run-groups/{group_id}/artifacts")
    assert pending.status_code == 200
    assert pending.json() == {"run_group_id": group_id, "status": "pending", "bundle_ready": False, "bundle_size_bytes": 0, "file_count": 0, "files": []}

    (directory / "artifact_manifest.json").write_text(json.dumps({"files": [{"name": "children\\record.json", "size_bytes": 7}]}), encoding="utf-8")
    (directory / "artifacts.zip").write_bytes(b"real-zip-bytes")
    ready = client.get(f"/api/v5/formal/run-groups/{group_id}/artifacts")
    assert ready.status_code == 200
    body = ready.json()
    assert body["status"] == "ready"
    assert body["bundle_ready"] is True
    assert body["bundle_size_bytes"] == len(b"real-zip-bytes")
    assert body["file_count"] == 1
    assert body["files"] == [{"name": "children/record.json", "size_bytes": 7}]
    assert "bundle_path" not in body


def test_v5_formal_artifact_catalog_filters_unsafe_manifest_items(tmp_path):
    from backend.app.services.v5_formal_artifact_catalog import read_catalog

    (tmp_path / "artifact_manifest.json").write_text(json.dumps({"files": [
        {"name": "C:\\secret.txt", "size_bytes": 1}, {"name": "/etc/passwd", "size_bytes": 1},
        {"name": "../secret", "size_bytes": 1}, {"name": "\\\\server\\share", "size_bytes": 1},
        {"name": "negative", "size_bytes": -1}, {"name": "boolean", "size_bytes": True},
        {"name": "children\\record.json", "size_bytes": 7}, {"name": "children/record.json", "size_bytes": 8},
    ]}), encoding="utf-8")
    body = read_catalog(tmp_path, "v5grp_catalog_test")
    assert body["files"] == [{"name": "children/record.json", "size_bytes": 7}]
    assert body["file_count"] == 1


def test_v5_formal_artifact_catalog_unknown_group_is_not_found():
    response = client.get("/api/v5/formal/run-groups/v5grp_missing_catalog/artifacts")
    assert response.status_code == 404
