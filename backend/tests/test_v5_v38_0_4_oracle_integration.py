from __future__ import annotations

from backend.app.services.v5_formal_scheduler import _serial_oracle_summary
from backend.app.services.v5_stateful_serializability_oracle import matches_stateful_local_legacy


def test_v38_0_4_formal_row_contract_reaches_stateful_oracle_dispatcher() -> None:
    runtime_summary = {
        "block_executor_id": "block_stm_block_executor",
        "ready_to_commit": True,
    }
    formal_row = {
        "comparison_semantics_class": "stateful_local_legacy_v1",
        "state_home_mapping_policy": "execution_shard_local_namespace",
        "remote_fetch_policy": "none",
        "remote_writeback_policy": "none",
    }
    merged = _serial_oracle_summary(runtime_summary, formal_row)
    assert matches_stateful_local_legacy(merged) is True
    assert merged["block_executor_id"] == "block_stm_block_executor"


def test_v38_0_4_formal_row_does_not_fake_stateful_contract() -> None:
    merged = _serial_oracle_summary(
        {"block_executor_id": "block_stm_block_executor"},
        {
            "comparison_semantics_class": "stateless_remote_home_v1",
            "state_home_mapping_policy": "deterministic_state_key_sharding",
            "remote_fetch_policy": "home_leader_witness_fetch",
            "remote_writeback_policy": "home_shard_consensus_delta",
        },
    )
    assert matches_stateful_local_legacy(merged) is False
