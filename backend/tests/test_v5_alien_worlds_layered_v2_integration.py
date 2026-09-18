from __future__ import annotations

import json
from pathlib import Path

import pytest

from backend.app.services import v5_workload_data_plane as plane
from backend.app.services.workload_adapters.alien_worlds_layered_v2 import AlienWorldsLayeredV2Adapter
from backend.app.services.workload_adapters.registry import get_adapter


def _source_row() -> dict:
    keys = ["m.federation/miners/alice", "federation/landregs/42", "m.federation/landcomms/owner"]
    return {
        "schema_version": "mbe_workload_record_v2",
        "record_id": "aw:00000000:test",
        "source_order": 0,
        "source_transaction_id": "a" * 64,
        "sender_id": "alice",
        "receiver_id": "m.federation",
        "operation_type": "alien_worlds_mine",
        "static_access_envelope": {
            "keys": keys,
            "complete": True,
            "policy_id": "aw_static_envelope_v1",
            "construction_method": "OFFLINE_REPLAY_DECLARATION_PROJECTION",
        },
        "declared_scheduling_evidence": {
            "policy_id": "aw_declared_scheduling_v1",
            "items": [
                {"key": keys[0], "access_mode": "RW", "update_semantic": "UNKNOWN"},
                {"key": keys[1], "access_mode": "R", "update_semantic": "NONE"},
                {"key": keys[2], "access_mode": "RW", "update_semantic": "UNKNOWN"},
            ],
        },
        "routing_source_key": keys[0],
        "routing_target_key": keys[1],
        "skew_keys": [keys[0]],
        "workload_metadata": {},
        "provenance": {"source_kind": "historical_alien_worlds", "package_version": "fixture"},
    }


def test_layered_v2_adapter_is_registered_and_emits_canonical_v4(tmp_path: Path) -> None:
    adapter = get_adapter("alien_worlds_layered_v2")
    assert isinstance(adapter, AlienWorldsLayeredV2Adapter)
    source = tmp_path / "input.jsonl"
    source.write_text(json.dumps(_source_row()) + "\n", encoding="utf-8")
    manifest = {"dataset_id": "alien_worlds_layered_v2_fixture", "row_count": 1}
    summary = adapter.validate_source(source, manifest)
    assert summary.row_count == 1
    record = next(adapter.iter_canonical_records(source, manifest))
    assert record["schema_version"] == "mbe_workload_record_v4"
    assert record["state_keys"] == _source_row()["static_access_envelope"]["keys"]
    assert {item["key"] for item in record["scheduling_access_list"]} == set(record["state_keys"])
    assert {item["key"] for item in record["access_list"]} == set(record["state_keys"])
    assert record["access_list_source"] == "static_declaration_projection_no_oracle"
    assert record["metadata"]["runtime_access_semantics"] == "static_declaration_replay_projection_no_oracle"
    plane._validate_canonical_record(record, dataset_id=manifest["dataset_id"], row_number=0)


def test_layered_v2_adapter_rejects_oracle_leakage(tmp_path: Path) -> None:
    row = _source_row()
    row["source_execution_oracle"] = {"actual_read_keys": ["secret"]}
    source = tmp_path / "bad.jsonl"
    source.write_text(json.dumps(row) + "\n", encoding="utf-8")
    adapter = AlienWorldsLayeredV2Adapter()
    with pytest.raises(ValueError, match="forbidden field"):
        adapter.validate_source(source, {"dataset_id": "fixture", "row_count": 1})


def test_layered_v2_adapter_requires_routing_inside_envelope(tmp_path: Path) -> None:
    row = _source_row()
    row["routing_source_key"] = "miner:legacy-alias"
    source = tmp_path / "bad-routing.jsonl"
    source.write_text(json.dumps(row) + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="routing_source_key is outside"):
        AlienWorldsLayeredV2Adapter().validate_source(source, {"dataset_id": "fixture", "row_count": 1})
