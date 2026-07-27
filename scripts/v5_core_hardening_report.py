from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from statistics import mean


ROOT = Path(__file__).resolve().parents[1]
MISSING = "missing"
PLUGIN_CATEGORIES = [
    "workload",
    "transaction_admission",
    "txpool",
    "sharding",
    "routing",
    "block_producer",
    "consensus",
    "network",
    "execution",
    "scheduler",
    "block_executor",
    "state_access",
    "state_storage",
    "cross_shard",
    "commit",
    "fault_injection",
    "metrics",
    "observability",
]
METATRACK_REQUIRED_FIELDS = [
    "submitted_logical_tx_count",
    "fast_logical_tx_count",
    "conservative_logical_tx_count",
    "fast_validator_execution_count",
    "conservative_validator_execution_count",
    "scheduler_dispatch_event_count",
    "remote_read_rpc_count",
    "remote_write_rpc_count",
    "classification_reason_counts",
    "pre_aggregation_physical_op_count",
    "post_aggregation_physical_op_count",
    "aggregated_key_count",
    "aggregated_logical_delta_count",
]
METATRACK_BLOCK_EXECUTOR_REQUIRED_FIELDS = [
    "blocked_event_count",
    "wakeup_event_count",
    "completion_channel_event_count",
    "business_execute_invocation_count",
    "retry_execution_count",
    "reexecution_count",
    "validator_execution_completion_count",
    "unique_final_logical_completion_count",
    "duplicate_final_completion_count",
    "configured_worker_count",
    "max_ready_queue_depth",
    "max_fast_ready_queue_depth",
    "max_conservative_ready_queue_depth",
    "max_dependency_frontier_width",
    "max_inflight_business_executions",
]


def read_json(path: Path) -> dict:
    if not path.is_file():
        return {}
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, dict) else {}


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def replay_state_updates(snapshot: dict[str, str], updates: list[dict], namespace: str) -> dict[str, str]:
    state = {str(key): str(value) for key, value in snapshot.items()}
    for item in updates:
        key = str(item.get("key", ""))
        if "::" not in key:
            key = f"{namespace}::{key}"
        if item.get("update_semantics") == "commutative_delta":
            current_text = state.get(key, "")
            if current_text == "" and item.get("has_initial_value") is True:
                current = int(item.get("initial_value", 0))
            else:
                current = int(current_text or 0)
            state[key] = str(current + int(item.get("delta", 0)))
        else:
            state[key] = str(item.get("value", ""))
    return state


def replay_state_delta_wal_records(records: list[dict], namespace: str, snapshot: dict[str, str] | None = None) -> dict[str, str]:
    state = dict(snapshot or {})
    for record in records:
        updates = record.get("state_updates")
        if isinstance(updates, list):
            state = replay_state_updates(state, updates, namespace)
    return state


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def logical_path(path: Path) -> str:
    resolved = path.resolve()
    roots = [
        ("repo", ROOT.resolve()),
        ("formal_run_root", Path(os.environ["MBE_FORMAL_RUN_ROOT"]).resolve()) if os.environ.get("MBE_FORMAL_RUN_ROOT") else None,
        ("runtime_root", Path(os.environ["MBE_RUNTIME_ROOT"]).resolve()) if os.environ.get("MBE_RUNTIME_ROOT") else None,
        ("workload_cache_root", Path(os.environ["MBE_WORKLOAD_CACHE_ROOT"]).resolve()) if os.environ.get("MBE_WORKLOAD_CACHE_ROOT") else None,
    ]
    for item in roots:
        if item is None:
            continue
        label, root = item
        try:
            return f"{label}:{resolved.relative_to(root).as_posix()}"
        except ValueError:
            pass
    return resolved.name


def resolve_logical(value: str, formal_root: Path | None = None) -> Path:
    if ":" in value:
        label, rel = value.split(":", 1)
        env = {
            "repo": ROOT,
            "formal_run_root": Path(os.environ.get("MBE_FORMAL_RUN_ROOT", "")) if os.environ.get("MBE_FORMAL_RUN_ROOT") else formal_root,
            "runtime_root": Path(os.environ.get("MBE_RUNTIME_ROOT", "")),
            "workload_cache_root": Path(os.environ.get("MBE_WORKLOAD_CACHE_ROOT", "")),
        }.get(label)
        if env and str(env):
            candidate = (env / rel).resolve()
            if label == "formal_run_root" and not candidate.exists() and formal_root is not None:
                parent_candidate = (formal_root.parent / rel).resolve()
                if parent_candidate.exists():
                    return parent_candidate
            return candidate
    return (ROOT / value).resolve()


def pct(values: list[float], percentile: float) -> float | str:
    if not values:
        return MISSING
    values = sorted(values)
    index = min(int(round((len(values) - 1) * percentile)), len(values) - 1)
    return values[index]


def node_dirs(run_dir: Path) -> list[Path]:
    nodes = run_dir / "nodes"
    if not nodes.is_dir():
        return []
    return sorted([path for path in nodes.iterdir() if path.is_dir()], key=lambda path: path.name)


def require_field(blockers: list[str], method_id: str, source: str, data: dict, key: str) -> object:
    if key not in data:
        blockers.append(f"{method_id}: missing {source}.{key}")
        return MISSING
    return data[key]


def sum_node_json(run_dir: Path, name: str, key: str, blockers: list[str], method_id: str) -> float | str:
    total = 0.0
    found = False
    for node in node_dirs(run_dir):
        path = node / name
        if not path.is_file():
            blockers.append(f"{method_id}: missing {node.name}/{name}")
            continue
        data = read_json(path)
        if key not in data:
            blockers.append(f"{method_id}: missing {node.name}/{name}.{key}")
            continue
        value = data[key]
        if isinstance(value, (int, float)):
            total += float(value)
            found = True
        else:
            blockers.append(f"{method_id}: non-numeric {node.name}/{name}.{key}")
    return total if found else MISSING


def sum_block_persistence(run_dir: Path, key: str, blockers: list[str], method_id: str) -> float | str:
    total = 0.0
    found = False
    for node in node_dirs(run_dir):
        path = node / "block_execution_summary.json"
        if not path.is_file():
            blockers.append(f"{method_id}: missing {node.name}/block_execution_summary.json")
            continue
        data = read_json(path)
        blocks = data.get("blocks")
        if not isinstance(blocks, list):
            blockers.append(f"{method_id}: missing {node.name}/block_execution_summary.json.blocks")
            continue
        for index, block in enumerate(blocks):
            if not isinstance(block, dict) or key not in block:
                blockers.append(f"{method_id}: missing {node.name}/block_execution_summary.json.blocks[{index}].{key}")
                continue
            value = block[key]
            if not isinstance(value, (int, float)):
                blockers.append(f"{method_id}: non-numeric {node.name}/block_execution_summary.json.blocks[{index}].{key}")
                continue
            total += float(value)
            found = True
    return total if found else MISSING


def latest_receipt_root(run_dir: Path, blockers: list[str], method_id: str) -> str:
    latest_height = -1
    latest_root = MISSING
    found_chain = False
    for node in node_dirs(run_dir):
        rows = read_csv(node / "committed_chain.csv")
        if not rows:
            blockers.append(f"{method_id}: missing or empty {node.name}/committed_chain.csv")
            continue
        found_chain = True
        for row in rows:
            if "receipt_root" not in row:
                blockers.append(f"{method_id}: missing {node.name}/committed_chain.csv.receipt_root")
                continue
            try:
                height = int(row.get("height") or -1)
            except ValueError:
                blockers.append(f"{method_id}: non-numeric {node.name}/committed_chain.csv.height")
                continue
            if height >= latest_height:
                latest_height = height
                latest_root = row["receipt_root"] or MISSING
    if not found_chain:
        blockers.append(f"{method_id}: no committed chain evidence for receipt root")
    if latest_root == MISSING:
        blockers.append(f"{method_id}: missing latest receipt root from committed chain")
    return latest_root


def collect(acceptance: Path) -> tuple[dict, list[dict[str, object]], list[str]]:
    report = read_json(acceptance)
    formal_root = acceptance.parent
    blockers: list[str] = []
    rows: list[dict[str, object]] = []
    if report.get("acceptance_passed") is not True:
        blockers.append("input acceptance did not pass")
    for blocker in report.get("blockers") or []:
        blockers.append(f"input acceptance blocker: {blocker}")
    per_method = report.get("results") or report.get("per_method") or {}
    if not per_method:
        blockers.append("acceptance has no per-method results")
    for method_id, evidence in per_method.items():
        run_dir = resolve_logical(str(evidence.get("run_dir", "")), formal_root=formal_root)
        summary = read_json(run_dir / "real_cluster_summary.json")
        finality = read_json(run_dir / "finality_summary.json")
        drain = read_json(run_dir / "drain_status.json")
        shutdown = read_json(run_dir / "shutdown_status.json")
        replay = read_json(run_dir / "workload_replay_summary.json")
        plugin = read_json(run_dir / "nodes" / "n0" / "plugin_snapshot.json")
        identity = read_json(run_dir / "run_identity.json")
        node_summaries = [read_json(node / "node_summary.json") for node in node_dirs(run_dir) if (node / "node_summary.json").is_file()]
        block_exec = [read_json(node / "block_execution_summary.json") for node in node_dirs(run_dir) if (node / "block_execution_summary.json").is_file()]
        commit_rows = []
        for node in node_dirs(run_dir):
            commit_rows.extend(read_csv(node / "commit_log.csv"))
        latencies = []
        for row in read_csv(run_dir / "transaction_finality.csv"):
            try:
                latencies.append(float(row.get("finality_ms") or row.get("latency_ms") or 0))
            except ValueError:
                continue
        if require_field(blockers, method_id, "real_cluster_summary", summary, "receipt_root_consistent") is not True:
            blockers.append(f"{method_id}: receipt_root_consistent is not true")
        row = {
            "method_id": method_id,
            "run_dir": logical_path(run_dir),
            "git_head": require_field(blockers, method_id, "run_identity", identity, "git_head"),
            "working_tree_diff_sha256": require_field(blockers, method_id, "run_identity", identity, "working_tree_diff_sha256"),
            "executable_sha256": require_field(blockers, method_id, "run_identity", identity, "executable_sha256"),
            "compiled_plan_digest": require_field(blockers, method_id, "run_identity", identity, "compiled_plan_digest"),
            "plugin_snapshot_digest": require_field(blockers, method_id, "run_identity", identity, "plugin_snapshot_digest"),
            "submitted": require_field(blockers, method_id, "finality_summary", finality, "submitted_unique_tx_count"),
            "terminal": require_field(blockers, method_id, "finality_summary", finality, "terminal_unique_tx_count"),
            "incomplete": require_field(blockers, method_id, "finality_summary", finality, "incomplete_unique_tx_count"),
            "throughput_tps": require_field(blockers, method_id, "finality_summary", finality, "throughput_tps"),
            "p50_latency_ms": finality.get("p50_finality_ms") if "p50_finality_ms" in finality else pct(latencies, 0.50),
            "p95_latency_ms": finality.get("p95_finality_ms") if "p95_finality_ms" in finality else pct(latencies, 0.95),
            "p99_latency_ms": finality.get("p99_finality_ms") if "p99_finality_ms" in finality else pct(latencies, 0.99),
            "state_root_consistent": require_field(blockers, method_id, "real_cluster_summary", summary, "state_root_consistent"),
            "receipt_root_consistent": require_field(blockers, method_id, "real_cluster_summary", summary, "receipt_root_consistent"),
            "plan_digest_consistent": require_field(blockers, method_id, "real_cluster_summary", summary, "plan_digest_consistent"),
            "no_fallback": require_field(blockers, method_id, "real_cluster_summary", summary, "no_fallback"),
            "orphan_process_count": require_field(blockers, method_id, "real_cluster_summary", summary, "orphan_process_count"),
            "drain_reason": require_field(blockers, method_id, "drain_status", drain, "completion_reason"),
            "shutdown_success": require_field(blockers, method_id, "shutdown_status", shutdown, "success"),
            "workload_materialized_sha256": replay.get("materialized_sha256") or replay.get("materialized_workload_sha256") or MISSING,
            "block_execution_summary_count": len(block_exec) if block_exec else MISSING,
            "node_summary_count": len(node_summaries) if node_summaries else MISSING,
            "commit_log_row_count": len(commit_rows) if commit_rows else MISSING,
            "latest_receipt_root_from_committed_chain": latest_receipt_root(run_dir, blockers, method_id),
            "wal_append_ms": sum_block_persistence(run_dir, "wal_append_ms", blockers, method_id),
            "wal_sync_ms": sum_block_persistence(run_dir, "wal_sync_ms", blockers, method_id),
            "snapshot_write_ms": sum_block_persistence(run_dir, "snapshot_write_ms", blockers, method_id),
            "receipt_batch_write_ms": sum_block_persistence(run_dir, "receipt_batch_write_ms", blockers, method_id),
            "tx_index_batch_write_ms": sum_block_persistence(run_dir, "tx_index_batch_write_ms", blockers, method_id),
            "durable_commit_ms": sum_block_persistence(run_dir, "durable_commit_ms", blockers, method_id),
            "persistence_written_bytes": sum_block_persistence(run_dir, "persistence_written_bytes", blockers, method_id),
            "wal_record_count": sum_block_persistence(run_dir, "wal_record_count", blockers, method_id),
            "snapshot_count": sum_block_persistence(run_dir, "snapshot_count", blockers, method_id),
            "scheduler_stolen_work_count": require_field(blockers, method_id, "real_cluster_summary", summary, "scheduler_stolen_work_count"),
            "remote_state_access_count": require_field(blockers, method_id, "real_cluster_summary", summary, "remote_state_access_count"),
            "scheduler_blocked_count": require_field(blockers, method_id, "real_cluster_summary", summary, "scheduler_blocked_count"),
            "scheduler_wakeup_count": require_field(blockers, method_id, "real_cluster_summary", summary, "scheduler_wakeup_count"),
            "logical_update_count": require_field(blockers, method_id, "real_cluster_summary", summary, "logical_update_count"),
            "physical_update_count": require_field(blockers, method_id, "real_cluster_summary", summary, "physical_update_count"),
            "pre_aggregation_physical_op_count": sum_commit_csv(run_dir, "pre_aggregation_physical_op_count", fallback="logical_update_count"),
            "post_aggregation_physical_op_count": sum_commit_csv(run_dir, "post_aggregation_physical_op_count", fallback="physical_update_count"),
            "aggregated_key_count": sum_mapping_csv(run_dir, "aggregated_key_count"),
            "aggregated_logical_delta_count": sum_mapping_csv(run_dir, "aggregated_logical_delta_count"),
            "physical_write_reduction_ratio": MISSING,
            "block_stm_business_execution_invocation_count": block_stm_total(run_dir, "business_execution_invocation_count"),
            "block_stm_serial_oracle_ms": block_stm_total(run_dir, "serial_oracle_ms"),
            "block_stm_serial_fallback_count": block_stm_total(run_dir, "serial_fallback_count"),
            "configured_worker_count": max_block_optional(run_dir, "configured_worker_count"),
            "max_ready_queue_depth": max_block_optional(run_dir, "max_ready_queue_depth"),
            "max_fast_ready_queue_depth": max_block_optional(run_dir, "max_fast_ready_queue_depth"),
            "max_conservative_ready_queue_depth": max_block_optional(run_dir, "max_conservative_ready_queue_depth"),
            "max_dependency_frontier_width": max_block_optional(run_dir, "max_dependency_frontier_width"),
            "max_inflight_business_executions": max_block_optional(run_dir, "max_inflight_business_executions"),
            "worker_execution_count": json.dumps(collect_worker_execution_counts(run_dir)),
            "submitted_logical_tx_count": MISSING,
            "blocked_event_count": sum_block_optional(run_dir, "blocked_event_count"),
            "wakeup_event_count": sum_block_optional(run_dir, "wakeup_event_count"),
            "completion_channel_event_count": sum_block_optional(run_dir, "completion_channel_event_count"),
            "business_execute_invocation_count": sum_block_optional(run_dir, "business_execute_invocation_count"),
            "retry_execution_count": sum_block_optional(run_dir, "retry_execution_count"),
            "reexecution_count": sum_block_optional(run_dir, "reexecution_count"),
            "validator_execution_completion_count": sum_block_optional(run_dir, "validator_execution_completion_count"),
            "unique_final_logical_completion_count": MISSING,
            "duplicate_final_completion_count": sum_block_optional(run_dir, "duplicate_final_completion_count"),
            "metric_scope": MISSING,
            "metric_unit": MISSING,
        }
        for category in PLUGIN_CATEGORIES:
            row[category] = (plugin.get(category) or {}).get("plugin_id") or MISSING
            if row[category] == MISSING:
                blockers.append(f"{method_id}: missing plugin category {category}")
        row["physical_write_reduction_ratio"] = write_reduction(row)
        if row["workload_materialized_sha256"] == MISSING:
            blockers.append(f"{method_id}: missing workload materialized hash")
        for key in ("wal_record_count", "persistence_written_bytes"):
            value = row[key]
            if not isinstance(value, (int, float)) or value <= 0:
                blockers.append(f"{method_id}: {key} must be positive for WAL-mode run")
        if str(method_id).startswith("metatrack"):
            metatrack_summary = collect_metatrack_summary(run_dir, summary)
            row.update(metatrack_summary)
            for key in METATRACK_REQUIRED_FIELDS:
                if row.get(key, MISSING) == MISSING:
                    blockers.append(f"{method_id}: missing MetaTrack report field {key}")
            if row.get("block_executor") == "metatrack_block_executor":
                for key in METATRACK_BLOCK_EXECUTOR_REQUIRED_FIELDS:
                    if row.get(key, MISSING) == MISSING:
                        blockers.append(f"{method_id}: missing MetaTrack actual execution field {key}")
                if isinstance(row.get("max_inflight_business_executions"), (int, float)) and isinstance(row.get("configured_worker_count"), (int, float)):
                    if row["max_inflight_business_executions"] > row["configured_worker_count"]:
                        blockers.append(f"{method_id}: max_inflight_business_executions exceeds configured_worker_count")
                if row.get("duplicate_final_completion_count") != 0:
                    blockers.append(f"{method_id}: duplicate_final_completion_count must be 0")
            if isinstance(row.get("fast_logical_tx_count"), int) and isinstance(row.get("conservative_logical_tx_count"), int) and isinstance(row.get("submitted"), int):
                if row["fast_logical_tx_count"] + row["conservative_logical_tx_count"] != row["submitted"]:
                    blockers.append(f"{method_id}: fast_logical + conservative_logical must equal submitted logical tx")
            if isinstance(row.get("unique_final_logical_completion_count"), int) and isinstance(row.get("submitted"), int):
                if row["unique_final_logical_completion_count"] != row["submitted"]:
                    blockers.append(f"{method_id}: unique_final_logical_completion_count must equal submitted logical tx")
        rows.append(row)
    return report, rows, blockers


def write_reduction(summary: dict) -> float:
    pre = summary.get("pre_aggregation_physical_op_count", summary.get("logical_update_count"))
    post = summary.get("post_aggregation_physical_op_count", summary.get("physical_update_count"))
    if not isinstance(pre, (int, float)) or pre <= 0:
        return MISSING
    if not isinstance(post, (int, float)):
        return MISSING
    return (float(pre) - float(post)) / float(pre)


def sum_commit_csv(run_dir: Path, key: str, fallback: str | None = None) -> int | str:
    total = 0
    found = False
    for node in node_dirs(run_dir):
        for row in read_csv(node / "commit_log.csv"):
            value = row.get(key)
            if value in (None, "") and fallback:
                value = row.get(fallback)
            if value in (None, ""):
                continue
            try:
                total += int(float(value))
                found = True
            except ValueError:
                continue
    return total if found else MISSING


def sum_mapping_csv(run_dir: Path, key: str) -> int | str:
    if key == "aggregated_key_count":
        count = 0
        found = False
        for node in node_dirs(run_dir):
            for row in read_csv(node / "logical_physical_update_mapping.csv"):
                found = True
                if (row.get("aggregation_applied") or "").lower() == "true":
                    count += 1
        return count if found else MISSING
    if key == "aggregated_logical_delta_count":
        total = 0
        found = False
        for node in node_dirs(run_dir):
            for row in read_csv(node / "logical_physical_update_mapping.csv"):
                found = True
                if (row.get("aggregation_applied") or "").lower() != "true":
                    continue
                try:
                    total += int(float(row.get("logical_update_count") or 0))
                except ValueError:
                    continue
        return total if found else MISSING
    return MISSING


def block_stm_total(run_dir: Path, key: str) -> float:
    total = 0.0
    for node in node_dirs(run_dir):
        metrics = read_json(node / "block_stm_summary.json").get("block_stm_metrics") or {}
        value = metrics.get(key, 0)
        if isinstance(value, (int, float)):
            total += float(value)
    return total


def sum_block_optional(run_dir: Path, key: str) -> float | str:
    total = 0.0
    found = False
    for node in node_dirs(run_dir):
        for block in read_json(node / "block_execution_summary.json").get("blocks") or []:
            if not isinstance(block, dict):
                continue
            value = block.get(key)
            if isinstance(value, (int, float)):
                total += float(value)
                found = True
    return total if found else MISSING


def max_block_optional(run_dir: Path, key: str) -> float | str:
    best: float | None = None
    for node in node_dirs(run_dir):
        for block in read_json(node / "block_execution_summary.json").get("blocks") or []:
            if not isinstance(block, dict):
                continue
            value = block.get(key)
            if isinstance(value, (int, float)):
                best = float(value) if best is None else max(best, float(value))
    return best if best is not None else MISSING


def collect_worker_execution_counts(run_dir: Path) -> list[int]:
    totals: list[int] = []
    for node in node_dirs(run_dir):
        for block in read_json(node / "block_execution_summary.json").get("blocks") or []:
            counts = block.get("worker_execution_count") if isinstance(block, dict) else None
            if not isinstance(counts, list):
                continue
            while len(totals) < len(counts):
                totals.append(0)
            for index, value in enumerate(counts):
                if isinstance(value, (int, float)):
                    totals[index] += int(value)
    return totals


def collect_metatrack_summary(run_dir: Path, summary: dict) -> dict[str, object]:
    execution_rows = []
    scheduler_rows = []
    remote_rows = []
    business_rows = []
    reason_counts: dict[str, int] = {}
    fast_logical: set[str] = set()
    conservative_logical: set[str] = set()
    final_completion_keys: set[tuple[str, str, str, str]] = set()
    final_logical_tx: set[str] = set()
    duplicate_final = 0
    fast_instances = 0
    conservative_instances = 0
    for node in node_dirs(run_dir):
        execution_rows.extend(read_csv(node / "execution_log.csv"))
        scheduler_rows.extend(read_csv(node / "metatrack_scheduler_trace.csv"))
        remote_rows.extend(read_csv(node / "remote_state_access.csv"))
        business_rows.extend(read_csv(node / "business_execute_invocation_count_by_node.csv"))
    for row in execution_rows:
        tx_id = row.get("tx_id") or row.get("logical_tx_id") or ""
        track = row.get("track") or ""
        reason = row.get("reason") or row.get("decision_reason") or MISSING
        for code in [item for item in str(reason).replace(";", "|").split("|") if item]:
            reason_counts[code] = reason_counts.get(code, 0) + 1
        if track == "fast":
            fast_instances += 1
            if tx_id:
                fast_logical.add(tx_id)
        elif track == "conservative":
            conservative_instances += 1
            if tx_id:
                conservative_logical.add(tx_id)
    dispatch_events = sum((row.get("decision_reason") or "").startswith("actual_dispatch") for row in scheduler_rows)
    remote_reads = sum((row.get("access_kind") or "") == "read" for row in remote_rows)
    remote_writes = sum((row.get("access_kind") or "") == "write_apply" for row in remote_rows)
    for row in business_rows:
        if (row.get("final_completion") or "").lower() != "true":
            continue
        key = (row.get("node_id") or "", row.get("block_height") or "", row.get("block_hash") or "", row.get("tx_id") or "")
        if key in final_completion_keys:
            duplicate_final += 1
        else:
            final_completion_keys.add(key)
        if row.get("tx_id"):
            final_logical_tx.add(row["tx_id"])
    submitted_logical = len(fast_logical | conservative_logical) if execution_rows else MISSING
    return {
        "submitted_logical_tx_count": submitted_logical,
        "fast_logical_tx_count": len(fast_logical) if execution_rows else MISSING,
        "conservative_logical_tx_count": len(conservative_logical) if execution_rows else MISSING,
        "fast_validator_execution_count": fast_instances if execution_rows else MISSING,
        "conservative_validator_execution_count": conservative_instances if execution_rows else MISSING,
        "scheduler_dispatch_event_count": dispatch_events if scheduler_rows else MISSING,
        "remote_read_rpc_count": remote_reads if remote_rows else MISSING,
        "remote_write_rpc_count": remote_writes if remote_rows else MISSING,
        "classification_reason_counts": json.dumps(reason_counts, sort_keys=True) if execution_rows else MISSING,
        "unique_final_logical_completion_count": len(final_logical_tx) if business_rows else MISSING,
        "duplicate_final_completion_count": duplicate_final if business_rows else MISSING,
        "metric_scope": "run",
        "metric_unit": "logical_tx|validator_execution_instance|scheduler_event|worker_attempt|final_completion",
        "logical_update_count": summary.get("logical_update_count", MISSING),
        "physical_update_count": summary.get("physical_update_count", MISSING),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--acceptance", required=True)
    parser.add_argument("--output-root", required=True)
    args = parser.parse_args()
    output = Path(args.output_root).resolve()
    output.mkdir(parents=True, exist_ok=True)
    acceptance, rows, blockers = collect(Path(args.acceptance).resolve())
    write_csv(output / "method_identity_summary.csv", rows)
    write_csv(output / "performance_summary.csv", rows)
    write_csv(output / "latency_percentiles.csv", rows)
    write_csv(output / "persistence_io_summary.csv", rows)
    write_csv(output / "fairness_check.csv", [{
        "method_id": row["method_id"],
        "workload_materialized_sha256": row["workload_materialized_sha256"],
        "submitted": row["submitted"],
        "terminal": row["terminal"],
        "plan_digest_consistent": row["plan_digest_consistent"],
    } for row in rows])
    write_csv(output / "throughput_windows.csv", [{"method_id": row["method_id"], "throughput_tps": row["throughput_tps"]} for row in rows])
    write_csv(output / "metatrack_mechanism_summary.csv", [row for row in rows if str(row["method_id"]).startswith("metatrack")])
    write_csv(output / "block_stm_mechanism_summary.csv", [row for row in rows if str(row["method_id"]).endswith("block_stm")])
    write_csv(output / "plugin_runtime_wiring_audit.csv", plugin_audit_rows(rows))
    correctness = {"acceptance_passed": acceptance.get("acceptance_passed") is True and not blockers, "blockers": blockers, "input_blockers": acceptance.get("blockers"), "methods": rows}
    (output / "correctness_summary.json").write_text(json.dumps(correctness, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "v5_core_hardening_report.md").write_text(markdown_report(rows, blockers), encoding="utf-8")
    print(json.dumps({"report_dir": logical_path(output), "method_count": len(rows), "blockers": blockers}, sort_keys=True))
    return 0 if not blockers else 1


def plugin_audit_rows(rows: list[dict[str, object]]) -> list[dict[str, object]]:
    call_sites = {
        "workload": "executor/v5/workload_iterator.go: WorkloadPlugin.NewIterator; executor/v5/client.go: WorkloadRecord stream",
        "transaction_admission": "executor/v5/runtime.go: NodeRuntime.handleSubmit -> Admission.Admit",
        "txpool": "executor/v5/runtime.go: NodeRuntime.handleSubmit/scheduleBlock -> TxPoolPlugin.CreatePool mempool methods",
        "sharding": "executor/v5/registry.go: ShardingPlugin.ShardFor via routing/client/runtime ownership",
        "routing": "executor/v5/client.go: route workload records; executor/v5/registry.go: RoutingPlugin.Route/PlanBatch",
        "block_producer": "executor/v5/runtime.go: runProducerLoop -> BlockProducerPlugin.ShouldProduce/BuildCandidate",
        "consensus": "executor/v5/runtime.go: PBFT-style preprepare/prepare/commit handlers use ConsensusPlugin policy",
        "network": "executor/v5/runtime.go: NetworkPlugin.CreateTransport/Start/Stop/Send/Broadcast",
        "execution": "executor/v5/registry.go: ExecutionPlugin.Classify/ClassifyBatch",
        "scheduler": "executor/v5/registry.go: SchedulerPlugin.Schedule; executor/v5/runtime.go: planned schedule artifact",
        "block_executor": "executor/v5/runtime.go: Commit path -> BlockExecutor.ExecuteBlock",
        "state_access": "executor/v5/runtime.go: prepareMetaTrackStateSnapshot/applyMetaTrackRemoteDeltas",
        "state_storage": "executor/v5/runtime.go: StateStorage.Open/Snapshot/ApplyBatch/PersistDelta/SnapshotIfDue",
        "cross_shard": "executor/v5/runtime.go: relay SourceLock/BuildRelay/TargetCommit/Finalize",
        "commit": "executor/v5/runtime.go: CommitPlugin.DecideCommit before StateStorage.ApplyBatch",
        "fault_injection": "executor/v5/runtime.go: NetworkPlugin transport creation with FaultPolicy",
        "metrics": "executor/v5/runtime.go: RuntimeEvent emission and metric artifact flush",
        "observability": "executor/v5/runtime.go: ObservabilityPlugin.Observe via RuntimeEvent artifact path",
    }
    result = []
    for row in rows:
        for category in PLUGIN_CATEGORIES:
            result.append({
                "method_id": row["method_id"],
                "category": category,
                "current_plugin": row.get(category),
                "real_runtime_call_site": call_sites.get(category, MISSING),
                "current_gap": "bounded_by_current_v5_truth_label",
                "required_change": "wired_for_current_v5_core_hardening_scope",
            })
    return result


def markdown_report(rows: list[dict[str, object]], blockers: list[str]) -> str:
    lines = [
        "# V5 Core Hardening Report",
        "",
        f"- acceptance_passed: `{not blockers}`",
        f"- blockers: `{blockers}`",
        "- performance claims: none; rows report observed local run evidence only.",
        "",
        "## Methods",
    ]
    for row in rows:
        lines.append(
            f"- `{row['method_id']}`: terminal={row['terminal']}/{row['submitted']}, throughput_tps={row['throughput_tps']}, "
            f"p95_ms={row['p95_latency_ms']}, no_fallback={row['no_fallback']}, run_dir={row['run_dir']}"
        )
    lines.extend(["", "## Mechanism Notes"])
    for row in rows:
        if str(row["method_id"]).startswith("metatrack"):
            lines.append(
                f"- `{row['method_id']}`: stolen_work={row['scheduler_stolen_work_count']}, remote_access={row['remote_state_access_count']}, "
                f"fast_logical={row.get('fast_logical_tx_count')}, conservative_logical={row.get('conservative_logical_tx_count')}, "
                f"validator_instances={row.get('validator_execution_completion_count')}, completion_channel_events={row.get('completion_channel_event_count')}, "
                f"max_inflight={row.get('max_inflight_business_executions')}/{row.get('configured_worker_count')}, duplicate_final={row.get('duplicate_final_completion_count')}, "
                f"write_reduction={row['physical_write_reduction_ratio']}"
            )
        if str(row["method_id"]).endswith("block_stm"):
            lines.append(
                f"- `{row['method_id']}`: serial_oracle_ms={row['block_stm_serial_oracle_ms']}, "
                f"business_execution_invocations={row['block_stm_business_execution_invocation_count']}, "
                f"serial_fallback_count={row['block_stm_serial_fallback_count']}"
            )
    lines.extend(["", "## Persistence", "- WAL/snapshot and batch receipt/tx-index metrics are in `persistence_io_summary.csv`."])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    raise SystemExit(main())
