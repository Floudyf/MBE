import pytest

from backend.app.services.chain_backend import ChainProfile, LocalVirtualBackend, UnsupportedLiveBackend, list_backend_capabilities


def profile(backend_type: str = "local_virtual") -> ChainProfile:
    return ChainProfile(
        chain_id="chain_a",
        role="source",
        backend_type=backend_type,
        backend="mock_chain",
        block_interval_ms=100,
        finality_depth=3,
        data_truth_label="synthetic_replay",
    )


def test_local_virtual_backend_computes_commit_and_finality_without_live_chain() -> None:
    backend = LocalVirtualBackend(profile())
    record = {
        "stage_id": "stage_1",
        "tx_id": "tx_1",
        "submit_time_ms": 50,
        "commit_time_ms": 120,
        "status": "finalized",
    }

    event = backend.submit_stage(record)
    finality = backend.observe_finality(record)

    assert event.event_time_ms == 120
    assert finality.finality_time_ms == 420
    assert finality.observed is False
    assert backend.capability().supports_real_time is False


def test_live_backends_are_planned_placeholders_only() -> None:
    capabilities = {item["backend_type"]: item for item in list_backend_capabilities()}

    assert capabilities["fabric_live"]["status"] == "planned"
    assert capabilities["evm_live"]["status"] == "planned"
    assert capabilities["fabric_live"]["supports_real_time"] is False

    backend = UnsupportedLiveBackend(profile("fabric_live"), "fabric_live")
    with pytest.raises(NotImplementedError):
        backend.submit_stage({"stage_id": "s", "submit_time_ms": 0})
