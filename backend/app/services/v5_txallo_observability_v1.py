"""TxAllo v1 evidence closure. Offline evidence only; never a protocol input.

Do not invent counters when files are absent. Physical blocks are counted once
per execution-shard leader and never as a sum over PBFT validator replicas.
"""
from __future__ import annotations

from collections import Counter, defaultdict
import csv
import hashlib
import json
from pathlib import Path
from typing import Any

SCHEMA = "mbe_txallo_observability_v1"
TIMING = "mbe_txallo_epoch_timing_v1"
STAGES = ("commit_wait_ns", "replica_wait_ns", "allocation_update_ns", "mapping_ack_wait_ns")


def _json(path: Path) -> dict[str, Any]:
    try:
        data = json.loads(path.read_text(encoding="utf-8-sig"))
        return data if isinstance(data, dict) else {}
    except (OSError, ValueError, UnicodeError):
        return {}


def _sha(path: Path) -> str | None:
    if not path.is_file():
        return None
    h = hashlib.sha256()
    try:
        with path.open("rb") as f:
            for buf in iter(lambda: f.read(4 * 1024 * 1024), b""):
                h.update(buf)
    except OSError:
        return None
    return h.hexdigest()


def _plan_shards(plan: dict[str, Any]) -> list[str] | None:
    configs = plan.get("node_configs")
    if not isinstance(configs, list) or not configs:
        return None
    shards = sorted({str(n.get("execution_shard_id") or n.get("shard_id") or "")
                     for n in configs if isinstance(n, dict)})
    return shards if shards and "" not in shards else None


def _timing(root: Path) -> dict[str, Any]:
    source = root / "client" / "txallo_epoch_timing.jsonl"
    if not source.is_file():
        return {"status": "unavailable", "reason": "epoch_timing_not_recorded", "source": "client/txallo_epoch_timing.jsonl"}
    stages = Counter()
    rows: list[dict[str, Any]] = []
    errors = []
    seen = set()
    try:
        with source.open(encoding="utf-8-sig") as f:
            for line_no, line in enumerate(f, start=1):
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                    epoch = row["source_epoch"]
                    if row.get("schema_version") != TIMING or not isinstance(epoch, int) or epoch < 0:
                        raise ValueError("bad_schema_or_source_epoch")
                    if epoch in seen:
                        raise ValueError("duplicate_source_epoch")
                    seen.add(epoch)
                    values = {}
                    for key in (*STAGES, "total_barrier_ns"):
                        v = row.get(key)
                        if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                            raise ValueError("bad_duration:" + key)
                        values[key] = v
                    # Durations are sibling phases; total also contains the small
                    # instrumentation/housekeeping overhead and must be >= sum.
                    if sum(values[k] for k in STAGES) > values["total_barrier_ns"] + 1_000_000:
                        raise ValueError("phase_sum_exceeds_barrier")
                    if row.get("status") not in ("passed", "failed"):
                        raise ValueError("missing_status")
                    if row["status"] == "failed" and not row.get("failure_phase"):
                        raise ValueError("missing_failure_phase")
                    for key in (*STAGES, "total_barrier_ns"):
                        stages[key] += values[key]
                    rows.append(row)
                except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
                    errors.append(f"row_{line_no}:{exc}")
    except (OSError, UnicodeError) as exc:
        errors.append(f"unreadable:{type(exc).__name__}")
    if errors:
        return {"status": "failed", "blockers": errors, "source": "client/txallo_epoch_timing.jsonl"}
    if not rows:
        return {"status": "unavailable", "reason": "empty_timing_file", "source": "client/txallo_epoch_timing.jsonl"}
    return {
        "status": "passed" if all(row["status"] == "passed" for row in rows) else "failed",
        "source": "client/txallo_epoch_timing.jsonl",
        "scope": "successfully_closed_source_epochs_only;not_final_partial_epoch",
        "duration_semantics": "client_local_monotonic_nanoseconds;sum_of_nonoverlapping_phase_durations",
        "closed_epoch_count": len(rows),
        "failed_epoch_count": sum(row["status"] == "failed" for row in rows),
        "total_ns_by_phase": dict(stages),
        "total_barrier_ms": stages["total_barrier_ns"] / 1_000_000,
        "phase_ms": {k: stages[k] / 1_000_000 for k in STAGES},
        "phase_percent_of_barrier": {k: (stages[k] / stages["total_barrier_ns"] * 100 if stages["total_barrier_ns"] else None) for k in STAGES},
        "records": rows,
    }


def _leader_paths(root: Path, plan: dict[str, Any]) -> tuple[list[Path], str]:
    cfg = plan.get("node_configs")
    if isinstance(cfg, list) and cfg:
        leaders = [n for n in cfg if isinstance(n, dict) and
                   (n.get("leader") is True or n.get("role") == "leader")]
        if leaders:
            expected = [root / "nodes" / str(n.get("node_id")) / "block_execution_summary.json" for n in leaders]
            if any(not f.is_file() for f in expected):
                return [], "leader_summary_missing"
            keys = [str(n.get("execution_shard_id") or n.get("shard_id") or "") for n in leaders]
            if "" in keys or len(set(keys)) != len(keys):
                return [], "ambiguous_execution_shard_leaders"
            return expected, "declared_leader_per_execution_shard"
    return [], "leader_identity_not_proven"


def _physical(root: Path, plan: dict[str, Any]) -> dict[str, Any]:
    paths, identity = _leader_paths(root, plan)
    if not paths:
        return {"status": "unavailable", "reason": identity}
    counts = Counter()
    costs = Counter()
    per_shard = {}
    seen = set()
    for path in paths:
        document = _json(path)
        if not isinstance(document.get("blocks"), list):
            return {"status": "failed", "blocker": "invalid_block_execution_summary", "source": str(path)}
        shard = str(document.get("shard_id") or "")
        if not shard or shard in per_shard:
            return {"status": "failed", "blocker": "ambiguous_shard_in_execution_summary"}
        shard_count = Counter()
        for row in document["blocks"]:
            if not isinstance(row, dict) or not str(row.get("block_hash") or ""):
                return {"status": "failed", "blocker": "invalid_physical_block_row", "source": str(path)}
            token = (shard, row["block_hash"])
            if token in seen:
                return {"status": "failed", "blocker": "duplicate_physical_block_hash", "source": str(path)}
            seen.add(token)
            n = row.get("executed_transaction_count")
            system = row.get("system_delta_drain_block_count")
            if not isinstance(n, int) or n < 0:
                return {"status": "failed", "blocker": "missing_executed_transaction_count"}
            counts["physical_blocks"] += 1
            shard_count["physical_blocks"] += 1
            counts["physical_transaction_instances"] += n
            if n:
                counts["blocks_with_transaction_instances"] += 1
                shard_count["blocks_with_transaction_instances"] += 1
            else:
                counts["blocks_without_transaction_instances"] += 1
            if system == 1:
                counts["system_delta_drain_blocks"] += 1
                shard_count["system_delta_drain_blocks"] += 1
            # Category counts may overlap; do not add them to block count.
            for field in ("block_execution_ms", "state_commitment_ms", "wal_append_ms", "wal_sync_ms", "state_db_apply_ms", "durable_commit_ms"):
                value = row.get(field)
                if isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0:
                    costs[field] += value
        per_shard[shard] = dict(shard_count)
    # Runtime counters are attempts and diagnostic actions; they are not
    # direct counts of state.NewCommitment() invocations.
    obs_keys = (
        "txallo_commitment_reuse_block_attempt_count",
        "txallo_commitment_full_build_block_attempt_count",
        "txallo_historical_projection_block_attempt_count",
        "txallo_replica_write_fanout_count",
        "txallo_replica_enqueue_ack_count",
        "txallo_replica_admission_deferred_tx_count",
        "txallo_replica_exact_local_projection_count",
    )
    runtime_counts = Counter()
    runtime_sources = []
    for path in paths:
        counter_path = path.parent / "runtime_metrics.json"
        obj = _json(counter_path)
        counters = obj.get("counts") if isinstance(obj.get("counts"), dict) else {}
        if counters:
            runtime_sources.append(str(counter_path.relative_to(root)).replace("\\", "/"))
        for key in obs_keys:
            value = counters.get(key)
            if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                runtime_counts[key] += value
    result = {"status": "passed", "scope": "one_declared_leader_per_execution_shard;resource_sum_not_wall_critical_path",
              "leader_selection": identity, "counts": dict(counts), "cost_sum_ms": dict(costs),
              "per_shard": per_shard, "sources": [str(p.relative_to(root)).replace('\\', '/') for p in paths],
              "runtime_attempt_counters": dict(runtime_counts) if runtime_sources else None,
              "runtime_counter_sources": runtime_sources,
              "classification_limitations": ["blocks_with_transaction_instances can include protocol relay instances",
                 "system_delta_drain_blocks identifies empty-TxList system-delta blocks only",
                 "no claim of total pure-replica block count without per-block delta origin"]}
    net = root / "network_message_summary.csv"
    if net.is_file():
        types = defaultdict(lambda: {"message_count": 0, "message_bytes": None})
        try:
            with net.open(encoding="utf-8-sig", newline="") as f:
                for row in csv.DictReader(f):
                    if str(row.get("scope") or "") != "message_type":
                        continue
                    typ = str(row.get("message_type") or "").strip()
                    if not typ:
                        continue
                    c = row.get("message_count")
                    if c not in (None, ""):
                        types[typ]["message_count"] += int(float(c))
                    for k in ("message_bytes", "total_bytes", "bytes"):
                        if row.get(k) not in (None, ""):
                            types[typ]["message_bytes"] = (types[typ]["message_bytes"] or 0) + int(float(row[k]))
                            break
            result["network_by_message_type"] = dict(types)
            result["network_source"] = "network_message_summary.csv"
        except (OSError, ValueError, UnicodeError, csv.Error):
            result["network_status"] = "failed_unreadable_or_invalid"
    else:
        result["network_status"] = "unavailable"
    return result


def build_summary(run_dir: Path | str, *, declared_shards: list[str] | None = None,
                  metric_context: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(run_dir)
    plan = _json(root / "compiled_run_plan.json")
    if not plan:
        return {"schema_version": SCHEMA, "status": "unavailable", "reason": "compiled_run_plan_missing"}
    from backend.app.services.v5_txallo_epoch_benefit_v1 import audit_run
    plan_shards = _plan_shards(plan)
    shards = declared_shards or plan_shards
    benefit = audit_run(root, shards) if shards else {"passed": False, "blockers": ["unproven_shard_inventory"]}
    inputs = ["compiled_run_plan.json", "workload/txallo_mapping_epochs.jsonl", "client/txallo_transaction_placement.csv",
              "client/txallo_allocation_summary.json", "client/txallo_client_binary_v1.json",
              "workload/txallo_history_summary.json", "client/txallo_epoch_timing.jsonl"]
    provenance = {name: {"status": "available", "sha256": digest} if (digest := _sha(root/name)) else
                  {"status": "unavailable"} for name in inputs}
    timing = _timing(root)
    physical = _physical(root, plan)
    metrics = metric_context or {}
    correctness = {key: metrics.get(key) for key in (
        "method_correctness_oracle_kind", "method_correctness_oracle_status", "method_correctness_oracle_valid",
        "serial_order_replay_equivalent", "txallo_stateful_replica_feed_complete")}
    status = "passed" if benefit.get("passed") and timing["status"] == "passed" and physical["status"] == "passed" else "partial"
    return {"schema_version": SCHEMA, "status": status,
            "method_id": metrics.get("method_id"), "shard_inventory": shards,
            "shard_inventory_source": "explicit" if declared_shards else "compiled_run_plan",
            "sources_sha256": provenance,
            "epoch_timing": timing, "physical_cost": physical,
            "mapping_benefit": benefit, "correctness_metrics_as_available": correctness,
            "limitations": ["no missing count or duration is treated as zero",
                            "client and node executable provenance are distinct: no equivalence inferred",
                            "older runs have no recoverable per-stage monotonic timings",
                            "the mapping benefit audit is offline, not an actual replay TPS improvement"]}


def emit_for_extractor(metrics: dict[str, Any], run_dir: Path) -> None:
    """Called before scheduler cold archive; never changes TxAllo execution."""
    try:
        summary = build_summary(run_dir, metric_context=metrics)
    except Exception as exc:
        # An optional evidence exporter must not change TxAllo's business
        # outcome. Surface an explicit unavailable/failed state instead.
        summary = {"schema_version": SCHEMA, "status": "failed",
                   "blockers": ["observability_export_exception:" + type(exc).__name__ + ":" + str(exc)]}
    target = Path(run_dir) / "aggregate" / "txallo_evidence_summary.json"
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    except OSError as exc:
        metrics["txallo_observability_status"] = "failed_write:" + type(exc).__name__
        return
    metrics["txallo_observability_status"] = summary["status"]
    for phase, ms in (summary.get("epoch_timing", {}).get("phase_ms") or {}).items():
        metrics["txallo_" + phase.removesuffix("_ns") + "_ms"] = ms
    if summary.get("epoch_timing", {}).get("total_barrier_ms") is not None:
        metrics["txallo_total_epoch_barrier_ms"] = summary["epoch_timing"]["total_barrier_ms"]
    metrics["txallo_dynamic_benefit_status"] = "passed" if summary.get("mapping_benefit", {}).get("passed") else "unavailable_or_failed"
    if summary.get("mapping_benefit", {}).get("passed"):
        metrics["txallo_dynamic_minus_frozen_cross_ratio"] = summary["mapping_benefit"]["delta_actual_minus_frozen_cross_shard_ratio"]
    metrics.setdefault("source_artifacts", []).append("aggregate/txallo_evidence_summary.json")
