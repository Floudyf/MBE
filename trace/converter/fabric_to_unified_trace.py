"""Convert streaming Fabric runner logs to the MBE unified trace format."""
from __future__ import annotations

import argparse
import json
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path

import yaml

from trace.writer import TraceJSONLWriter, write_trace_meta

RAW_REQUIRED = {"tx_id", "tx_type", "submit_time", "commit_time", "status", "contract", "function", "args", "block_number", "event"}


def _time_ms(value: object) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000
    raise ValueError(f"timestamp must be a number or ISO string, got {value!r}")


def _expand(template: str, args: Mapping[str, object], context: str) -> str:
    try:
        return template.format(**args)
    except KeyError as error:
        raise ValueError(f"{context}: missing argument {error.args[0]!r} referenced by access schema") from error


def _fnv_shard(key: str, shard_count: int) -> int:
    value = 2166136261
    for byte in key.encode("utf-8"):
        value = (value ^ byte) * 16777619 & 0xFFFFFFFF
    return value % shard_count


def _schema_entry(schema: Mapping[str, object], contract: str, function: str) -> Mapping[str, object]:
    contracts = schema.get("contracts")
    entry = contracts.get(contract, {}).get(function) if isinstance(contracts, Mapping) else None
    if not isinstance(entry, Mapping):
        raise ValueError(f"no access schema for contract/function {contract}/{function}")
    return entry


def _convert_record(raw: Mapping[str, object], schema: Mapping[str, object], chain_id: str, shard_count: int) -> dict[str, object]:
    missing = sorted(field for field in RAW_REQUIRED if field not in raw)
    if missing:
        raise ValueError(f"raw chain log {raw.get('tx_id', '<unknown>')}: missing required fields {', '.join(missing)}")
    args = raw["args"]
    if not isinstance(args, Mapping):
        raise ValueError(f"raw chain log {raw['tx_id']}: args must be an object")
    entry = _schema_entry(schema, str(raw["contract"]), str(raw["function"]))
    def expand_list(field: str) -> list[str]:
        values = entry.get(field, [])
        if not isinstance(values, list):
            raise ValueError(f"access schema {raw['contract']}/{raw['function']}: {field} must be a list")
        return [_expand(str(value), args, f"{raw['contract']}/{raw['function']}") for value in values]
    access_list, primary_key = expand_list("access_list"), _expand(str(entry["primary_key"]), args, f"{raw['contract']}/{raw['function']}")
    delta_value = None
    if "delta_arg" in entry:
        name = str(entry["delta_arg"])
        if name not in args:
            raise ValueError(f"{raw['contract']}/{raw['function']}: missing argument {name!r} referenced by access schema")
        delta_value = float(args[name])
        if entry.get("delta_sign") == "negative":
            delta_value = -delta_value
    latency = raw.get("chain_latency_ms")
    if latency is None:
        latency = _time_ms(raw["commit_time"]) - _time_ms(raw["submit_time"])
    return {"tx_id": raw["tx_id"], "tx_type": entry.get("tx_type", raw["tx_type"]), "timestamp": _time_ms(raw["submit_time"]), "chain_id": chain_id, "contract": raw["contract"], "function": raw["function"], "args": dict(args), "read_set": expand_list("read_set"), "write_set": expand_list("write_set"), "access_list": access_list, "commutative": bool(entry.get("commutative", False)), "update_type": str(entry.get("update_type", "assign")), "status": raw["status"], "chain_latency_ms": float(latency), "primary_key": primary_key, "access_size": len(access_list), "is_cross_shard": len({_fnv_shard(key, shard_count) for key in access_list}) > 1, "hot_key_tag": "reward_hot" if primary_key.startswith("reward_pool:") else "normal", "conflict_group": primary_key, "dependency_hint": entry.get("dependency_hint", ""), "delta_value": delta_value}


def convert_raw_fabric_log(raw_log_path: Path, access_schema_path: Path, output_dir: Path, chain_id: str = "fabric_single_chain", shard_count: int = 4) -> dict[str, object]:
    """Stream raw JSONL to gzip JSONL and calculate metadata from emitted records."""
    if shard_count <= 0:
        raise ValueError("shard_count must be positive")
    schema = yaml.safe_load(access_schema_path.read_text(encoding="utf-8"))
    if not isinstance(schema, Mapping):
        raise ValueError("access schema must be a mapping")
    stats: dict[str, object] = {"tx": 0, "success": 0, "failed": 0, "reads": 0, "writes": 0, "access": 0, "comm_delta": 0, "conflicts": 0, "hot": 0, "cross": 0, "hotspots": set(), "types": Counter()}
    output_dir.mkdir(parents=True, exist_ok=True)
    trace_path = output_dir / "trace.jsonl.gz"
    with raw_log_path.open(encoding="utf-8") as source, TraceJSONLWriter(trace_path) as writer:
        for line_number, line in enumerate(source, start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
            except json.JSONDecodeError as error:
                raise ValueError(f"raw chain log line {line_number}: invalid JSON") from error
            if not isinstance(raw, Mapping):
                raise ValueError(f"raw chain log line {line_number}: expected JSON object")
            record = _convert_record(raw, schema, chain_id, shard_count)
            writer.write(record)
            stats["tx"] += 1; stats["success"] += record["status"] == "success"; stats["failed"] += record["status"] == "failed"
            stats["reads"] += len(record["read_set"]); stats["writes"] += len(record["write_set"]); stats["access"] += len(record["access_list"])
            stats["comm_delta"] += record["commutative"] and record["update_type"] == "delta"; stats["conflicts"] += bool(record["conflict_group"])
            stats["hot"] += record["hot_key_tag"] != "normal"; stats["cross"] += record["is_cross_shard"]; stats["types"][record["tx_type"]] += 1
            if record["hot_key_tag"] != "normal": stats["hotspots"].add(record["hot_key_tag"])
    denom = stats["tx"] or 1
    metadata = {"source": "fabric_raw_log", "chain_id": chain_id, "tx_count": stats["tx"], "success_count": stats["success"], "failed_count": stats["failed"], "actual_tx_mix": {key: value / denom for key, value in sorted(stats["types"].items())}, "actual_hot_key_ratio": stats["hot"] / denom, "actual_cross_shard_ratio": stats["cross"] / denom, "avg_read_set_size": stats["reads"] / denom, "avg_write_set_size": stats["writes"] / denom, "avg_access_set_size": stats["access"] / denom, "actual_commutative_update_ratio": stats["comm_delta"] / denom, "actual_conflict_ratio": stats["conflicts"] / denom, "hot_tx_ratio": stats["hot"] / denom, "multi_hotspot_count": len(stats["hotspots"]), "seed": 0, "trace_format": "jsonl_gzip", "compression": "gzip", "schema_version": "v1.4-a"}
    meta_path = output_dir / "trace_meta.json"; write_trace_meta(metadata, meta_path)
    return {"trace_path": trace_path, "meta_path": meta_path, **metadata}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-log", type=Path, required=True); parser.add_argument("--access-schema", type=Path, required=True); parser.add_argument("--output", type=Path, required=True); parser.add_argument("--chain-id", default="fabric_single_chain"); parser.add_argument("--shard-count", type=int, default=4)
    args = parser.parse_args(); result = convert_raw_fabric_log(args.raw_log, args.access_schema, args.output, args.chain_id, args.shard_count)
    print(f"wrote {result['trace_path']}\nwrote {result['meta_path']}")


if __name__ == "__main__": main()
