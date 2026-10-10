"""Offline, non-authoritative TxAllo terminal-wait trace audit.

Never infer wall-clock critical paths by subtracting timestamps from different
machines. Missing stages remain unavailable; this module never gates finality.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any

SCHEMA = "mbe_txallo_terminal_breakdown_v14"
STAGES = ("submitted", "received", "admitted", "proposed", "quorum_committed", "durable_committed", "sourcefinalize")
PAIRS = (("received", "admitted"), ("admitted", "proposed"),
         ("proposed", "quorum_committed"), ("quorum_committed", "durable_committed"))


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def _json(path: Path) -> dict[str, Any]:
    with path.open(encoding="utf-8-sig") as stream:
        data = json.load(stream)
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object: {path.name}")
    return data


def _rows(path: Path):
    handle = gzip.open(path, "rt", encoding="utf-8-sig") if path.suffix == ".gz" else path.open(encoding="utf-8-sig")
    with handle as stream:
        for lineno, line in enumerate(stream, 1):
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path.name}:{lineno}: invalid JSON") from exc
            if not isinstance(row, dict):
                raise ValueError(f"{path.name}:{lineno}: expected object")
            yield row


def _int(raw: Any) -> int | None:
    try:
        if raw is None or isinstance(raw, bool) or str(raw).strip() == "":
            return None
        return int(raw)
    except (ValueError, TypeError):
        return None


def _percentile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    data = sorted(values)
    pos = (len(data) - 1) * q
    low = math.floor(pos)
    high = math.ceil(pos)
    return round(data[low] + (data[high] - data[low]) * (pos - low), 3)


def _stats(samples: list[float]) -> dict[str, Any]:
    return {"sample_count": len(samples), "p50_ms": _percentile(samples, .5),
            "p95_ms": _percentile(samples, .95), "p99_ms": _percentile(samples, .99),
            "max_ms": round(max(samples), 3) if samples else None}


def build(root: Path | str) -> dict[str, Any]:
    root = Path(root)
    locations = {
        "plan": root / "compiled_run_plan.json",
        "epoch_timing": root / "client/txallo_epoch_timing.jsonl",
        "sidecar": root / "workload/txallo_dynamic_blocks.jsonl.gz",
        "sidecar_summary": root / "workload/txallo_dynamic_blocks_summary.json",
        "placement": root / "client/txallo_transaction_placement.csv",
        "client_lifecycle": root / "client/transaction_lifecycle.jsonl",
    }
    missing = [name for name, path in locations.items() if not path.is_file()]
    out: dict[str, Any] = {
        "schema_version": SCHEMA, "status": "unavailable" if missing else "partial",
        "scope": "read_only_postprocessing_not_execution_or_finality_gate",
        "missing_inputs": missing, "limitations": [
            "wall-clock timestamps from different nodes are not subtracted without clock synchronization proof",
            "epoch commit_wait_ns is monotonic client elapsed time and is NOT the sum of per-tx stages",
            "PBFT and persistence stage pairs use same-node events only; no cross-node or end-to-end inference",
            "events with absent matches are unavailable, not zero; sourcefinalize is not a universal stateless requirement",
            "final partial source epoch is excluded from the committed-only mapping-update timing evidence",
        ],
        "source_sha256": {key: _sha(p) for key, p in locations.items() if p.is_file()},
    }
    if missing:
        return out
    try:
        plan = _json(locations["plan"])
        cfg = plan.get("node_configs") or []
        if not isinstance(cfg, list):
            raise ValueError("invalid node_configs")
        side_summary = _json(locations["sidecar_summary"])
        first_block = _int(side_summary.get("first_block_num"))
        if first_block is None:
            raise ValueError("sidecar first_block_num missing")
        sharding = plan.get("plugin_profile", {}).get("sharding", {})
        if not sharding:
            sharding = (cfg[0].get("plugin_profile", {}).get("sharding", {}) if cfg and isinstance(cfg[0], dict) else {})
        epoch_width = _int((sharding.get("config") or {}).get("a_epoch_blocks")) or 15
        if epoch_width <= 0:
            raise ValueError("bad epoch width")

        timings = {}
        for t in _rows(locations["epoch_timing"]):
            epoch = _int(t.get("source_epoch"))
            duration = _int(t.get("commit_wait_ns"))
            if epoch is None or epoch < 0 or epoch in timings or duration is None or duration < 0:
                raise ValueError("invalid/duplicate epoch timing")
            timings[epoch] = {"commit_wait_ms": round(duration / 1e6, 3),
                              "total_barrier_ms": (round((_int(t.get("total_barrier_ns")) or 0) / 1e6, 3)
                                                   if t.get("total_barrier_ns") is not None else None),
                              "status": t.get("status")}
        index_epoch = {}
        for row in _rows(locations["sidecar"]):
            index = _int(row.get("materialized_index"))
            block = _int(row.get("block_num"))
            if index is None or block is None or block < first_block or index in index_epoch:
                raise ValueError("invalid sidecar index/block")
            index_epoch[index] = (block - first_block) // epoch_width

        logical_to_epoch = {}
        epoch_counts = Counter()
        cross_counts = Counter()
        with locations["placement"].open(encoding="utf-8-sig", newline="") as stream:
            for row in csv.DictReader(stream):
                i = _int(row.get("tx_index"))
                logical = str(row.get("logical_id") or "").strip()
                if i not in index_epoch or not logical or logical in logical_to_epoch:
                    raise ValueError("placement/sidecar identity not one-to-one")
                epoch = index_epoch[i]
                logical_to_epoch[logical] = epoch
                epoch_counts[epoch] += 1
                if str(row.get("txallo_cross_shard") or "").lower() in {"true", "1"}:
                    cross_counts[epoch] += 1
        if len(logical_to_epoch) != len(index_epoch):
            raise ValueError("sidecar and placement row counts disagree")
        if set(timings) - set(epoch_counts):
            raise ValueError("timing references source epoch without placement rows")

        # For each (logical transaction, node, stage) keep the first physical occurrence;
        # cross-node timestamps never enter the same duration calculation.
        events = defaultdict(dict)
        client_count = 0
        node_events = 0
        for candidate in [locations["client_lifecycle"], *sorted((root / "nodes").glob("*/transaction_lifecycle.jsonl"))]:
            for ev in _rows(candidate):
                logical = str(ev.get("logical_tx_id") or "").strip()
                stage = str(ev.get("stage") or "").strip().lower()
                node = str(ev.get("node_id") or candidate.parent.name).strip()
                timestamp = _int(ev.get("timestamp_ms"))
                if logical not in logical_to_epoch or stage not in STAGES or not node or timestamp is None:
                    continue
                if ev.get("success") is False:
                    continue
                token = (logical, node)
                previous = events[token].get(stage)
                if previous is None or timestamp < previous:
                    events[token][stage] = timestamp
                if candidate == locations["client_lifecycle"]:
                    client_count += 1
                else:
                    node_events += 1

        stage_present = defaultdict(set)
        stage_pairs_by_epoch = defaultdict(lambda: defaultdict(list))
        negative_order_by_epoch = Counter()
        terminal_seen = defaultdict(set)
        nodes_seen = defaultdict(set)
        for (logical, node), stages in events.items():
            epoch = logical_to_epoch[logical]
            for stage in stages:
                stage_present[epoch].add((logical, stage))
                if stage in ("durable_committed", "sourcefinalize"):
                    terminal_seen[epoch].add((logical, stage))
            if node != "mbe-client":
                nodes_seen[epoch].add(node)
                for first, second in PAIRS:
                    if first in stages and second in stages:
                        ms = stages[second] - stages[first]
                        if ms < 0:
                            negative_order_by_epoch[epoch] += 1
                        else:
                            stage_pairs_by_epoch[epoch][f"{first}_to_{second}"].append(float(ms))

        results = []
        for epoch in sorted(epoch_counts):
            pairs = stage_pairs_by_epoch.get(epoch, {})
            count = epoch_counts[epoch]
            coverage = {stage: len({logical for logical, s in stage_present[epoch] if s == stage}) for stage in STAGES}
            record: dict[str, Any] = {
                "source_epoch": epoch, "logical_transaction_count": count,
                "cross_shard_transaction_count": cross_counts[epoch],
                "closed_epoch_timing": timings.get(epoch),
                "observed_unique_transaction_count_by_stage": coverage,
                "same_node_duration_distribution": {f"{a}_to_{b}": _stats(pairs.get(f"{a}_to_{b}", [])) for a, b in PAIRS},
                "node_count_with_observed_events": len(nodes_seen.get(epoch, [])),
                "timestamp_order_anomalies": negative_order_by_epoch[epoch],
                "explanation_of_client_commit_wait": "not_attributable_from_cross_node_absolute_timestamps",
            }
            results.append(record)
        out.update({
            "status": "partial" if not any(stage_pairs_by_epoch.values()) else "passed_with_scope_limitations",
            "source_epoch_count": len(results), "closed_epoch_count": len(timings),
            "total_logical_transaction_count": len(logical_to_epoch),
            "event_rows_used": {"client": client_count, "nodes": node_events},
            "epoch_width_source_blocks": epoch_width,
            "epochs": results,
            "clock_sync_verified": False,
            "cross_node_latency_ms": None,
            "global_critical_path_ms": None,
        })
    except (OSError, ValueError, TypeError, KeyError, UnicodeError, OverflowError) as exc:
        out["status"] = "failed"
        out["blockers"] = [f"terminal_trace_audit:{type(exc).__name__}:{exc}"]
    return out


def emit(root: Path | str) -> dict[str, Any]:
    root = Path(root)
    result = build(root)
    dest = root / "aggregate/txallo_terminal_breakdown.json"
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(f".{dest.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(tmp, dest)
    return result
