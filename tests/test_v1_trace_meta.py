from __future__ import annotations

import json
from pathlib import Path

import yaml

from workload.asset_hotspot_v1.generator import generate_from_config


ROOT = Path(__file__).resolve().parents[1]


def test_v1_trace_meta_is_observed_from_generated_records(tmp_path: Path) -> None:
    config = yaml.safe_load((ROOT / "configs" / "experiments" / "v1_asset_hotspot_v1.yaml").read_text(encoding="utf-8"))
    _, meta_path = generate_from_config(config, tmp_path)
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert {"actual_conflict_ratio", "actual_commutative_update_ratio", "avg_access_set_size", "hot_tx_ratio", "multi_hotspot_count"} <= meta.keys()
    assert 0 <= meta["actual_conflict_ratio"] <= 1
    assert 0 <= meta["actual_commutative_update_ratio"] <= 1
    assert meta["avg_access_set_size"] >= config["workload"]["access_set_size"]
    assert 0 < meta["multi_hotspot_count"] <= config["workload"]["multi_hotspot_count"]
