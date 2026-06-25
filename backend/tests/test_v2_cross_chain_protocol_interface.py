from backend.app.services.cross_chain_protocol import (
    CrossChainProtocol,
    ProtocolAction,
    ProtocolEvent,
    ProtocolResult,
    ProtocolState,
)
from backend.app.services.cross_chain_protocols import create_protocol


def test_protocol_interface_and_data_structures_exist() -> None:
    state = ProtocolState(
        cross_tx_id="ctx_1",
        protocol_name="lock_mint_serial",
        status="created",
        current_stage="created",
        source_chain="chain_a",
        target_chain="chain_b",
        created_at_ms=0,
        updated_at_ms=0,
        deadline_ms=1000,
    )
    action = ProtocolAction("a1", "ctx_1", "submit_source_lock", "chain_a", "source_lock", 0, 1000)
    event = ProtocolEvent("e1", "ctx_1", "submit", "chain_a", "source_lock", 0, "protocol")
    result = ProtocolResult("lock_mint_serial", "ctx_1", "completed", True, False, False, False, 1, 1, 1, 2, 1, 1)

    assert state.status == "created"
    assert action.action_type == "submit_source_lock"
    assert event.source == "protocol"
    assert result.success is True


def test_baseline_protocols_implement_interface() -> None:
    for name in ("lock_mint_serial", "lock_mint_pipeline", "fixed_window_baseline", "committee_bridge_basic"):
        protocol = create_protocol(name)
        assert isinstance(protocol, CrossChainProtocol)
        assert protocol.name == name
