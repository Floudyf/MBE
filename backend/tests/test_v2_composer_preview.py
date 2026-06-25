from fastapi.testclient import TestClient

from backend.app.main import app


client = TestClient(app)


def preview(payload: dict) -> dict:
    response = client.post("/api/v2/composer/preview", json=payload)
    assert response.status_code == 200
    return response.json()


def test_single_chain_synthetic_v1_full_is_runnable() -> None:
    payload = preview({
        "version": "v2",
        "topology": "single_chain",
        "trace_source": "synthetic",
        "workload": "asset_hotspot",
        "routing": "co_access",
        "execution": "dual_track",
        "commit": "hot_update_aggregation",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] == "runnable"
    assert payload["runnable"] is True
    assert payload["data_truth_label"] == "synthetic_replay"
    assert not payload["blocked_by"]


def test_single_chain_existing_trace_disabled_protocol_is_runnable() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "existing_trace",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] == "runnable"
    assert payload["data_truth_label"] == "existing_trace_replay"


def test_fabric_chain_backed_trace_preview_uses_replay_truth_label() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "fabric_chain_backed_trace",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] in {"runnable", "experimental"}
    assert payload["data_truth_label"] == "fabric_chain_backed_trace_replay"
    assert payload["runnable"] is False if payload["status"] == "experimental" else True


def test_dual_chain_and_multi_chain_are_planned_not_runnable() -> None:
    dual_chain = preview({"topology": "dual_chain", "trace_source": "synthetic", "cross_chain_protocol": "disabled"})
    multi_chain = preview({"topology": "multi_chain", "trace_source": "synthetic", "cross_chain_protocol": "disabled"})

    assert dual_chain["status"] == "planned"
    assert dual_chain["runnable"] is False
    assert dual_chain["data_truth_label"] == "planned_cross_chain_replay"
    assert multi_chain["status"] == "planned"
    assert multi_chain["runnable"] is False


def test_cross_chain_protocol_planned_plugin_cannot_be_runnable() -> None:
    payload = preview({"topology": "dual_chain", "trace_source": "synthetic", "cross_chain_protocol": "lock_mint_serial"})

    assert payload["status"] == "planned"
    assert payload["runnable"] is False
    assert any("cross_chain_protocol:lock_mint_serial" == item for item in payload["blocked_by"])


def test_public_chain_imported_trace_defaults_to_semantic_unknown() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "public_chain_imported_trace",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] == "experimental"
    assert payload["runnable"] is False
    assert payload["data_truth_label"] == "public_chain_imported_trace_semantic_unknown"
    assert any("semantic_unknown" in warning for warning in payload["warnings"])


def test_public_chain_imported_trace_with_co_access_explains_missing_access_list() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "public_chain_imported_trace",
        "workload": "asset_hotspot",
        "routing": "co_access",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] == "experimental"
    assert any("access_list" in reason for reason in payload["reasons"])


def test_public_chain_imported_trace_with_hot_update_explains_commutative_gap() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "public_chain_imported_trace",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "hot_update_aggregation",
        "cross_chain_protocol": "disabled",
    })

    assert payload["status"] == "invalid"
    assert payload["runnable"] is False
    assert any("commutative_update" in reason and "update_type" in reason for reason in payload["reasons"])


def test_single_chain_lock_mint_serial_is_never_runnable() -> None:
    payload = preview({
        "topology": "single_chain",
        "trace_source": "synthetic",
        "workload": "asset_hotspot",
        "routing": "hash",
        "execution": "serial",
        "commit": "normal_commit",
        "cross_chain_protocol": "lock_mint_serial",
    })

    assert payload["status"] == "invalid"
    assert payload["runnable"] is False
    assert any("Cross-chain protocol" in reason for reason in payload["reasons"])


def test_unknown_plugin_returns_invalid_with_clear_blocker() -> None:
    payload = preview({"topology": "unknown_topology"})

    assert payload["status"] == "invalid"
    assert payload["runnable"] is False
    assert payload["blocked_by"] == ["topology:unknown_topology"]
