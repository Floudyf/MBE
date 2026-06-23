from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def test_v1_templates_and_experiments_are_read_only_declarations() -> None:
    templates = client.get("/api/v1/composer/templates")
    experiments = client.get("/api/v1/composer/experiments")

    assert templates.status_code == 200
    assert len(templates.json()) == 6
    assert experiments.status_code == 200
    payload = {item["id"]: item for item in experiments.json()}
    assert payload["v1_baseline_hash_serial"]["runnable"] is True
    assert payload["v1_baseline_hash_serial"]["implemented"] is True
    for experiment_id, item in payload.items():
        if experiment_id != "v1_baseline_hash_serial":
            assert item["runnable"] is False
            assert item["implemented"] is False


def test_v1_preview_marks_planned_experiments_as_not_runnable() -> None:
    runnable = client.post("/api/v1/composer/preview", json={"experiment_id": "v1_baseline_hash_serial"})
    planned = client.post("/api/v1/composer/preview", json={"experiment_id": "v1_ours_metatrack"})

    assert runnable.status_code == 200
    assert runnable.json()["status"] == "runnable"
    assert planned.status_code == 200
    assert planned.json()["status"] == "planned"
