from __future__ import annotations

import gzip
import hashlib
import json
from pathlib import Path

import yaml

from workload.asset_hotspot_v1.generator import generate_from_config as generate_asset_hotspot_v1
from workload.reward_burst.generator import generate_from_config as generate_reward_burst


ROOT = Path(__file__).resolve().parents[1]


def _config(name: str) -> dict:
    return yaml.safe_load((ROOT / "configs" / "experiments" / name).read_text(encoding="utf-8"))


def _records(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def test_asset_hotspot_v1_is_reproducible_and_has_optional_fields(tmp_path: Path) -> None:
    config = _config("v1_asset_hotspot_v1.yaml")
    first, _ = generate_asset_hotspot_v1(config, tmp_path / "first")
    second, _ = generate_asset_hotspot_v1(config, tmp_path / "second")
    assert hashlib.sha256(first.read_bytes()).digest() == hashlib.sha256(second.read_bytes()).digest()
    record = _records(first)[0]
    assert {"primary_key", "access_size", "is_cross_shard", "hot_key_tag", "conflict_group", "dependency_hint", "delta_value"} <= record.keys()
    assert record["access_size"] == len(record["access_list"])


def test_reward_burst_generates_aggregation_preparation_fields(tmp_path: Path) -> None:
    trace, _ = generate_reward_burst(_config("v1_reward_burst.yaml"), tmp_path / "reward")
    records = _records(trace)
    assert {record["tx_type"] for record in records} == {"reward_claim", "add_reward", "batch_reward", "balance_delta", "reward_pool_delta"}
    assert any(record["commutative"] and record["update_type"] == "delta" and record["delta_value"] is not None for record in records)
    assert any(not record["commutative"] and record["update_type"] != "delta" for record in records)
    assert all(record["hot_key_tag"] != "normal" and record["conflict_group"] != "none" for record in records)
