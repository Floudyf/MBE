from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.trace_source_service import load_trace_sources, list_trace_sources


client = TestClient(app)


def test_v2_trace_source_config_loads_all_required_sources() -> None:
    registry = load_trace_sources()
    ids = {source["id"] for source in registry.sources}

    assert {"synthetic", "existing_trace", "fabric_chain_backed_trace", "public_chain_imported_trace"} <= ids
    for source in registry.sources:
        assert source["validation"]["allows_live_network"] is False
        assert source["validation"]["allows_docker_start"] is False


def test_v2_trace_source_list_has_truth_labels_and_limitations() -> None:
    items = list_trace_sources(load_trace_sources())
    by_id = {item["id"]: item for item in items}

    assert by_id["synthetic"]["data_truth_label"] == "synthetic_replay"
    assert by_id["existing_trace"]["data_truth_label"] == "existing_trace_replay"
    assert by_id["fabric_chain_backed_trace"]["data_truth_label"] == "fabric_chain_backed_trace_replay"
    assert by_id["public_chain_imported_trace"]["data_truth_label"] == "public_chain_imported_trace_semantic_unknown"
    assert by_id["public_chain_imported_trace"]["limitations"]


def test_v2_trace_source_api_lists_and_returns_details() -> None:
    response = client.get("/api/v2/trace-sources")
    detail = client.get("/api/v2/trace-sources/synthetic")

    assert response.status_code == 200
    assert {item["id"] for item in response.json()["items"]} >= {"synthetic", "existing_trace", "fabric_chain_backed_trace", "public_chain_imported_trace"}
    assert detail.status_code == 200
    assert detail.json()["id"] == "synthetic"
    assert detail.json()["data_truth_label"] == "synthetic_replay"
    assert "provides_fields" in detail.json()["capabilities"]


def test_v2_trace_source_api_unknown_source_is_clear() -> None:
    response = client.get("/api/v2/trace-sources/not_real")

    assert response.status_code == 404
