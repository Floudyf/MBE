"""Streaming V1.3 asset-hotspot trace generator.

This module deliberately leaves the V0 asset_hotspot generator unchanged.
"""

from __future__ import annotations

import random
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from trace.writer import TraceJSONLWriter, write_trace_meta


def _value(config: Mapping[str, object], name: str, default: object) -> object:
    return config.get(name, default)


def _access_list(primary_key: str, read_set: list[str], write_set: list[str], target_size: int) -> list[str]:
    keys = list(dict.fromkeys([primary_key, *read_set, *write_set]))
    for index in range(len(keys), max(target_size, len(keys))):
        keys.append(f"context:{primary_key}:{index}")
    return keys


def _record(index: int, rng: random.Random, cfg: Mapping[str, object], timestamp: float) -> dict[str, object]:
    hotspot_count = max(1, int(_value(cfg, "multi_hotspot_count", 1)))
    is_hot = rng.random() < float(_value(cfg, "hot_tx_ratio", cfg.get("hot_key_ratio", 0.05)))
    hotspot = rng.randrange(hotspot_count) if is_hot else None
    primary_key = f"asset:hot:{hotspot}" if hotspot is not None else f"asset:cold:{rng.randrange(1, 1_000_001)}"
    hot_tag = f"hotspot:{hotspot}" if hotspot is not None else "normal"
    has_conflict = rng.random() < float(_value(cfg, "conflict_injection_ratio", 0.0))
    conflict_group = f"conflict:{hotspot if hotspot is not None else rng.randrange(hotspot_count)}" if has_conflict else "none"
    commutative_delta = rng.random() < float(_value(cfg, "commutative_update_ratio", 0.0))
    read_only = rng.random() < float(_value(cfg, "read_write_ratio", 0.5))
    user = f"user:{rng.randrange(1, 1001)}"
    target = f"user:{rng.randrange(1, 1001)}"
    read_set = [primary_key, user]
    write_set: list[str] = [] if read_only else [primary_key, target]
    is_cross_shard = rng.random() < float(_value(cfg, "cross_shard_ratio", 0.0))
    if is_cross_shard and not read_only:
        write_set.append(f"remote:{target}")
    access_list = _access_list(primary_key, read_set, write_set, int(_value(cfg, "access_set_size", 3)))
    delta_value = round(rng.uniform(0.1, 5.0), 4) if commutative_delta else None
    return {
        "tx_id": f"v1-asset-{index:08d}",
        "tx_type": "asset_read" if read_only else "asset_update",
        "timestamp": timestamp,
        "chain_id": "mockchain-0",
        "contract": "asset",
        "function": "read" if read_only else "update",
        "args": {"key": primary_key, "user": user},
        "read_set": read_set,
        "write_set": write_set,
        "access_list": access_list,
        "commutative": commutative_delta,
        "update_type": "delta" if commutative_delta else "replace",
        "status": "pending",
        "chain_latency_ms": 0.0,
        "primary_key": primary_key,
        "access_size": len(access_list),
        "is_cross_shard": is_cross_shard,
        "hot_key_tag": hot_tag,
        "conflict_group": conflict_group,
        "dependency_hint": conflict_group if has_conflict else primary_key,
        "delta_value": delta_value,
    }


def generate_from_config(config: Mapping[str, object], output_dir: str | Path) -> tuple[Path, Path]:
    """Generate a reproducible V1.3 trace without buffering records in memory."""
    workload = config["workload"]
    if not isinstance(workload, Mapping) or workload.get("plugin") != "asset_hotspot_v1":
        raise ValueError("expected asset_hotspot_v1 workload")
    experiment = config["experiment"]
    if not isinstance(experiment, Mapping):
        raise ValueError("expected experiment configuration")
    count, seed = int(workload["tx_count"]), int(experiment["seed"])
    if count < 0:
        raise ValueError("tx_count must be non-negative")
    rng = random.Random(seed)
    stats: dict[str, object] = {"conflicts": 0, "commutative_delta": 0, "access_total": 0, "hot": 0, "hotspots": set(), "types": Counter(), "cross": 0, "reads": 0, "writes": 0}
    arrival_rate, burst_rate = float(_value(workload, "arrival_rate", 100.0)), float(_value(workload, "burst_rate", 500.0))
    if arrival_rate <= 0 or burst_rate <= 0:
        raise ValueError("arrival_rate and burst_rate must be positive")
    timestamp = 0.0
    out = Path(output_dir)
    trace = out / "trace.jsonl.gz"
    with TraceJSONLWriter(trace) as writer:
        for index in range(count):
            rate = burst_rate if index % 10 < 2 else arrival_rate
            record = _record(index, rng, workload, timestamp)
            writer.write(record)
            timestamp += 1000.0 / rate
            stats["conflicts"] += record["conflict_group"] != "none"
            stats["commutative_delta"] += record["commutative"] and record["update_type"] == "delta"
            stats["access_total"] += record["access_size"]
            stats["hot"] += record["hot_key_tag"] != "normal"
            stats["cross"] += record["is_cross_shard"]
            stats["reads"] += len(record["read_set"])
            stats["writes"] += len(record["write_set"])
            if record["hot_key_tag"] != "normal":
                stats["hotspots"].add(record["hot_key_tag"])
            stats["types"][record["tx_type"]] += 1
    denom = count or 1
    meta = {
        "tx_count": count, "actual_tx_mix": {key: value / denom for key, value in sorted(stats["types"].items())},
        "actual_hot_key_ratio": stats["hot"] / denom, "actual_cross_shard_ratio": stats["cross"] / denom,
        "avg_read_set_size": stats["reads"] / denom, "avg_write_set_size": stats["writes"] / denom,
        "seed": seed, "trace_format": "jsonl", "compression": "gzip", "schema_version": "v1.3",
        "actual_conflict_ratio": stats["conflicts"] / denom,
        "actual_commutative_update_ratio": stats["commutative_delta"] / denom,
        "avg_access_set_size": stats["access_total"] / denom,
        "hot_tx_ratio": stats["hot"] / denom,
        "multi_hotspot_count": len(stats["hotspots"]),
    }
    meta_path = out / "trace_meta.json"
    write_trace_meta(meta, meta_path)
    return trace, meta_path
