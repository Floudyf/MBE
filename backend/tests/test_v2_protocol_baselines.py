import inspect

import pytest

from backend.app.services.cross_chain_protocols import ProtocolNotFound, create_protocol, list_cross_chain_protocols


def test_all_required_baselines_are_constructible() -> None:
    serial = create_protocol("lock_mint_serial")
    pipeline = create_protocol("lock_mint_pipeline")
    fixed_window = create_protocol("fixed_window_baseline", {"window_size": 3})
    committee = create_protocol("committee_bridge_basic", {"committee_delay_ms": 75})

    assert serial.concurrency_mode == "serial"
    assert pipeline.concurrency_mode == "pipeline"
    assert fixed_window.concurrency_mode == "fixed_window"
    assert fixed_window.window_size == 3
    assert committee.committee_delay_ms == 75


def test_metaflow_is_not_runnable() -> None:
    with pytest.raises(ProtocolNotFound, match="planned"):
        create_protocol("metaflow")

    protocols = {item["name"]: item for item in list_cross_chain_protocols()}
    assert protocols["metaflow"]["status"] == "planned"
    assert protocols["committee_bridge_basic"]["maturity"] == "experimental"


def test_protocols_do_not_depend_on_local_virtual_backend_internals() -> None:
    import backend.app.services.cross_chain_protocols as module

    source = inspect.getsource(module)
    assert "LocalVirtualBackend" not in source
    assert "time.sleep" not in source
    assert "sleep(" not in source
