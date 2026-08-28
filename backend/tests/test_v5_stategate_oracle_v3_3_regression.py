from __future__ import annotations

from pathlib import Path

from backend.app.services import v5_formal_scheduler, v5_serial_order_oracle


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_v3_3_oracle_matches_production_committed_chain_parent_header() -> None:
    repo = _repo_root()
    runtime = (repo / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    oracle = Path(v5_serial_order_oracle.__file__).read_text(encoding="utf-8")
    assert '"parent_hash"' in runtime
    assert 'row.get("parent_hash") or row.get("previous_hash")' in oracle


def test_v3_3_retains_v3_2_formal_timeout_closure() -> None:
    assert v5_formal_scheduler.DEFAULT_FORMAL_EXECUTION_POLICY["child_wall_timeout_seconds"] == 7200


def test_v3_3_oracle_keeps_tx_id_not_original_index_as_global_identity() -> None:
    oracle = Path(v5_serial_order_oracle.__file__).read_text(encoding="utf-8")
    assert '"serial_order_replay_identity_basis": "tx_id"' in oracle
    assert "serial_oracle_duplicate_successful_original_index" not in oracle
