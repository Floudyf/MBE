from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.trace_source_service import load_trace_sources


client = TestClient(app)


def test_public_chain_imported_trace_is_semantic_unknown_and_limited() -> None:
    source = load_trace_sources().get_source("public_chain_imported_trace")
    fields = set(source["capabilities"]["provides_fields"])
    guarantees = set(source["capabilities"]["semantic_guarantees"])

    assert source["status"] == "experimental"
    assert source["data_truth_label"] == "public_chain_imported_trace_semantic_unknown"
    assert "semantic_unknown" in guarantees
    assert {"tx_id", "timestamp", "chain_id", "status", "raw_event"} <= fields
    assert not {"access_list", "read_set", "write_set", "commutative", "update_type", "delta_semantics"} & fields


def test_public_chain_validation_never_claims_strong_semantics_or_live_ingestion() -> None:
    response = client.post("/api/v2/trace-sources/validate", json={"source_id": "public_chain_imported_trace", "trace_path": "data/public_chain/sample.jsonl.gz"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "missing_file"
    assert payload["runnable"] is False
    assert payload["data_truth_label"] == "public_chain_imported_trace_semantic_unknown"
    assert any("semantic_unknown" in warning for warning in payload["warnings"])
    assert any("live public-chain ingestion is not implemented" == item for item in payload["blocked_by"])


def test_public_chain_with_v2_composer_advanced_mechanisms_remains_guarded() -> None:
    co_access = client.post("/api/v2/composer/preview", json={
        "topology": "single_chain",
        "trace_source": "public_chain_imported_trace",
        "workload": "asset_hotspot",
        "routing": "co_access",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "disabled",
    }).json()
    hot_update = client.post("/api/v2/composer/preview", json={
        "topology": "single_chain",
        "trace_source": "public_chain_imported_trace",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "hot_update_aggregation",
        "cross_chain_protocol": "disabled",
    }).json()

    assert co_access["status"] == "experimental"
    assert any("access_list" in reason for reason in co_access["reasons"])
    assert hot_update["status"] == "invalid"
    assert any("commutative_update" in reason and "update_type" in reason for reason in hot_update["reasons"])
