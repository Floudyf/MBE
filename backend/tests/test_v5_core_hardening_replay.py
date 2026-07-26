from __future__ import annotations

import importlib.util
from pathlib import Path


def _load_report_module():
    path = Path(__file__).resolve().parents[2] / "scripts" / "v5_core_hardening_report.py"
    spec = importlib.util.spec_from_file_location("v5_core_hardening_report", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_replay_state_updates_uses_commutative_delta_not_value() -> None:
    report = _load_report_module()
    state = report.replay_state_updates(
        {},
        [
            {
                "key": "balance:receiver",
                "value": "7",
                "update_semantics": "commutative_delta",
                "delta": 1,
                "has_initial_value": True,
                "initial_value": 0,
            },
            {
                "key": "balance:receiver",
                "value": "9",
                "update_semantics": "commutative_delta",
                "delta": 1,
                "has_initial_value": True,
                "initial_value": 8,
            },
        ],
        "s0",
    )
    assert state["s0::balance:receiver"] == "2"


def test_replay_state_updates_matches_state_db_initial_value_semantics() -> None:
    report = _load_report_module()
    state = report.replay_state_delta_wal_records(
        [
            {
                "state_updates": [
                    {
                        "key": "balance:sender",
                        "value": "999999",
                        "update_semantics": "commutative_delta",
                        "delta": -1,
                        "has_initial_value": True,
                        "initial_value": 1000000,
                    },
                    {
                        "key": "nonce:sender",
                        "value": "1",
                        "update_semantics": "commutative_delta",
                        "delta": 1,
                        "has_initial_value": True,
                        "initial_value": 0,
                    },
                    {
                        "key": "object:parcel",
                        "value": "owner",
                    },
                ]
            }
        ],
        "s0",
    )
    assert state["s0::balance:sender"] == "999999"
    assert state["s0::nonce:sender"] == "1"
    assert state["s0::object:parcel"] == "owner"
