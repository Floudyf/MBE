from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.v5_plugin_manifest_store import STORE


def test_implemented_v5_plugins_have_chinese_catalog_fields():
    for manifest in STORE.list():
        if manifest.implementation_status == "implemented":
            assert manifest.display_name_zh
            assert manifest.description_zh
            assert manifest.plugin_id


def test_catalog_api_retains_machine_id_and_returns_chinese_fields():
    response = TestClient(app).get("/api/v5/plugins?backend=real_cluster")
    assert response.status_code == 200
    item = next(row for row in response.json()["items"] if row["plugin_id"] == "dual_track_execution")
    assert item["plugin_id"] == "dual_track_execution"
    assert item["display_name_zh"] == "双轨执行"
    assert item["description_zh"]
