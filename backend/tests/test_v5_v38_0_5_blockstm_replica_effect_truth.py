from __future__ import annotations

from backend.app.services.v5_stateful_serializability_oracle import (
    _normalize_effect,
    _replica_effect_source,
)


def _effect(source: str) -> dict:
    return {
        "reads": [{"key": "k", "value_digest": "a" * 64, "source": source}],
        "writes": [{"key": "x", "value_digest": "b" * 64, "source": "write_set"}],
    }


def test_v38_0_5_blockstm_replica_effect_ignores_only_incarnation_number() -> None:
    left = _normalize_effect(_effect("stm_mvmemory_tx_8_inc_0"))
    right = _normalize_effect(_effect("stm_mvmemory_tx_8_inc_7"))
    assert left == right
    assert left["reads"][0]["source"] == "stm_mvmemory_tx_8"


def test_v38_0_5_blockstm_replica_effect_preserves_producer_identity() -> None:
    left = _normalize_effect(_effect("stm_mvmemory_tx_8_inc_1"))
    right = _normalize_effect(_effect("stm_mvmemory_tx_9_inc_1"))
    assert left != right


def test_v38_0_5_blockstm_replica_effect_preserves_semantic_source_classes() -> None:
    assert _replica_effect_source("stm_mvmemory_base") == "stm_mvmemory_base"
    assert _replica_effect_source("stm_mvmemory_estimate") == "stm_mvmemory_estimate"
    assert _replica_effect_source("stm_local_write") == "stm_local_write"
    assert _replica_effect_source("state_snapshot_overlay") == "state_snapshot_overlay"
