"""Deterministic, streaming V0 asset-hotspot workload generator."""
from __future__ import annotations
import random
from collections import Counter
from collections.abc import Iterator, Mapping
from pathlib import Path
from trace.writer import TraceJSONLWriter, write_trace_meta

TYPES = ("asset_transfer", "asset_trade", "scene_join", "reward_claim")

def _record(i: int, rng: random.Random, cfg: Mapping[str, object], stats: dict[str, object]) -> dict[str, object]:
    tx_type = rng.choice(TYPES); hot = rng.random() < float(cfg["hot_key_ratio"])
    rank = 1 + int(rng.paretovariate(float(cfg["zipf_theta"])))
    key = f"asset:{rank if hot else 100 + rank}"; cross = rng.random() < float(cfg["cross_shard_ratio"])
    user = f"user:{rng.randrange(1, 1001)}"; target = f"user:{rng.randrange(1, 1001)}"
    if tx_type == "scene_join": contract, function, reads, writes, commutative, update = "scene", "join", ["scene:main"], [f"scene_member:main:{user}"], False, "replace"
    elif tx_type == "reward_claim": contract, function, reads, writes, commutative, update = "reward", "claim", ["reward_pool:default"], [f"balance:{user}"], True, "delta"
    else: contract, function, reads, writes, commutative, update = "asset", ("trade" if tx_type == "asset_trade" else "transfer"), [key, user], [key, target], False, "replace"
    if cross: writes.append("remote:" + writes[-1])
    stats["types"][tx_type] += 1; stats["hot"] += hot; stats["cross"] += cross; stats["reads"] += len(reads); stats["writes"] += len(writes)
    return {"tx_id": f"tx-{i:08d}", "tx_type": tx_type, "timestamp": i, "chain_id": "mockchain-0", "contract": contract, "function": function, "args": {"key": key, "user": user}, "read_set": reads, "write_set": writes, "access_list": reads + writes, "commutative": commutative, "update_type": update, "status": "pending", "chain_latency_ms": 0.0}

def generate_from_config(config: Mapping[str, object], output_dir: str | Path) -> tuple[Path, Path]:
    workload = config["workload"]; seed = int(config["experiment"]["seed"]); count = int(workload["tx_count"])
    if workload.get("plugin") != "asset_hotspot" or count < 0: raise ValueError("expected non-negative asset_hotspot workload")
    rng = random.Random(seed); stats = {"types": Counter(), "hot": 0, "cross": 0, "reads": 0, "writes": 0}; out = Path(output_dir); trace = out / "trace.jsonl.gz"
    with TraceJSONLWriter(trace) as writer:
        for i in range(count): writer.write(_record(i, rng, workload, stats))
    denom = count or 1
    meta = {"tx_count": count, "actual_tx_mix": {k: v / denom for k, v in sorted(stats["types"].items())}, "actual_hot_key_ratio": stats["hot"] / denom, "actual_cross_shard_ratio": stats["cross"] / denom, "avg_read_set_size": stats["reads"] / denom, "avg_write_set_size": stats["writes"] / denom, "seed": seed, "trace_format": "jsonl", "compression": "gzip", "schema_version": "v0"}
    meta_path = out / "trace_meta.json"; write_trace_meta(meta, meta_path); return trace, meta_path
