"""Streaming reward-pool hotspot traces; it does not implement aggregation."""

from __future__ import annotations

import random
from collections import Counter
from collections.abc import Mapping
from pathlib import Path

from trace.writer import TraceJSONLWriter, write_trace_meta

TYPES = ("reward_claim", "add_reward", "batch_reward", "balance_delta", "reward_pool_delta")


def generate_from_config(config: Mapping[str, object], output_dir: str | Path) -> tuple[Path, Path]:
    workload, experiment = config["workload"], config["experiment"]
    if not isinstance(workload, Mapping) or workload.get("plugin") != "reward_burst":
        raise ValueError("expected reward_burst workload")
    if not isinstance(experiment, Mapping):
        raise ValueError("expected experiment configuration")
    count, seed = int(workload["tx_count"]), int(experiment["seed"])
    if count < 0:
        raise ValueError("tx_count must be non-negative")
    rng, pools = random.Random(seed), max(1, int(workload.get("multi_hotspot_count", 1)))
    ratio, rate = float(workload.get("commutative_update_ratio", 0.6)), float(workload.get("burst_rate", 500.0))
    if rate <= 0:
        raise ValueError("burst_rate must be positive")
    stats: dict[str, object] = {"commutative_delta": 0, "access_total": 0, "tags": set(), "types": Counter()}
    trace, timestamp = Path(output_dir) / "trace.jsonl.gz", 0.0
    with TraceJSONLWriter(trace) as writer:
        for index in range(count):
            pool, tx_type = rng.randrange(pools), TYPES[index % len(TYPES)]
            pool_key, user = f"reward_pool:{pool}", f"user:{rng.randrange(1, 1001)}"
            commutative_delta = rng.random() < ratio
            delta_value = round(rng.uniform(0.1, 10.0), 4) if commutative_delta else None
            reads, writes = [pool_key, f"balance:{user}"], [pool_key, f"balance:{user}"]
            tag, group = f"reward_hotspot:{pool}", f"reward_conflict:{pool}"
            record = {
                "tx_id": f"v1-reward-{index:08d}", "tx_type": tx_type, "timestamp": timestamp,
                "chain_id": "mockchain-0", "contract": "reward", "function": tx_type,
                "args": {"pool_id": pool, "user": user}, "read_set": reads, "write_set": writes,
                "access_list": reads, "commutative": commutative_delta,
                "update_type": "delta" if commutative_delta else "replace", "status": "pending", "chain_latency_ms": 0.0,
                "primary_key": pool_key, "access_size": len(reads), "is_cross_shard": False,
                "hot_key_tag": tag, "conflict_group": group, "dependency_hint": group, "delta_value": delta_value,
            }
            writer.write(record)
            timestamp += 1000.0 / rate
            stats["commutative_delta"] += commutative_delta
            stats["access_total"] += len(reads)
            stats["tags"].add(tag)
            stats["types"][tx_type] += 1
    denom = count or 1
    meta = {
        "tx_count": count, "actual_tx_mix": {key: value / denom for key, value in sorted(stats["types"].items())},
        "actual_hot_key_ratio": 1.0 if count else 0.0, "actual_cross_shard_ratio": 0.0,
        "avg_read_set_size": 2.0, "avg_write_set_size": 2.0, "seed": seed,
        "trace_format": "jsonl", "compression": "gzip", "schema_version": "v1.3",
        "actual_conflict_ratio": 1.0 if count else 0.0,
        "actual_commutative_update_ratio": stats["commutative_delta"] / denom,
        "avg_access_set_size": stats["access_total"] / denom,
        "hot_tx_ratio": 1.0 if count else 0.0, "multi_hotspot_count": len(stats["tags"]),
    }
    meta_path = Path(output_dir) / "trace_meta.json"
    write_trace_meta(meta, meta_path)
    return trace, meta_path
