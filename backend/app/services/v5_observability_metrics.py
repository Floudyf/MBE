from __future__ import annotations

import csv
import json
import math
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable


RESOURCE_TIMESERIES = "resource_usage_timeseries.csv"
RESOURCE_RAW_SUMMARY = "resource_sampler_summary.json"
RESOURCE_SUMMARY = "resource_usage_summary.json"
NETWORK_SUMMARY = "network_metrics_summary.json"
NETWORK_MESSAGE_SUMMARY = "network_message_summary.csv"

METATRACK_WINDOW_PLAN = "metatrack_consensus_window_plan.jsonl"
METATRACK_WINDOW_SUMMARY = "metatrack_consensus_window_summary.json"
METATRACK_WINDOW_EXECUTION = "metatrack_consensus_window_execution.csv"


def write_observability_summaries(run_dir: Path) -> dict[str, dict]:
    """Build optional post-run observability artifacts without changing run validity.

    The function is intentionally side-effect free with respect to protocol/runtime
    state. It reads persisted finality/resource/network evidence after the real
    supervisor has exited and writes research-only summaries. Callers must treat
    failures as non-fatal to the formal execution result.
    """
    run_dir = Path(run_dir)
    finality = _read_json(run_dir / "finality_summary.json")
    cluster = _read_json(run_dir / "real_cluster_summary.json")
    resource = summarize_resource_usage(run_dir, finality)
    network = summarize_network_usage(run_dir, finality, cluster)
    metatrack_window = summarize_metatrack_consensus_windows(run_dir)
    _write_json(run_dir / RESOURCE_SUMMARY, resource)
    _write_json(run_dir / NETWORK_SUMMARY, network)
    _write_network_csv(run_dir / NETWORK_MESSAGE_SUMMARY, network)
    if metatrack_window.get("available"):
        _write_json(run_dir / METATRACK_WINDOW_SUMMARY, metatrack_window)
    return {"resource": resource, "network": network, "metatrack_consensus_window": metatrack_window}


def summarize_resource_usage(run_dir: Path, finality: dict[str, Any] | None = None) -> dict[str, Any]:
    finality = finality or _read_json(Path(run_dir) / "finality_summary.json")
    raw_summary = _read_json(Path(run_dir) / RESOURCE_RAW_SUMMARY)
    path = Path(run_dir) / RESOURCE_TIMESERIES
    rows = _read_resource_rows(path)
    start_ms = _number(finality.get("completion_window_start_ms"))
    end_ms = _number(finality.get("completion_window_end_ms"))

    base = {
        "schema_version": "mbe_v5_resource_usage_summary_v1",
        "scope": "validator_node_processes_only",
        "measurement_boundary": "completion_window",
        "source_artifacts": [name for name in (RESOURCE_TIMESERIES, RESOURCE_RAW_SUMMARY, "finality_summary.json") if (Path(run_dir) / name).is_file()],
        "available": False,
        "sampling_available": bool(raw_summary.get("sampling_available", bool(rows))),
        "sampling_error": raw_summary.get("sampling_error") or "",
        "sample_interval_ms": raw_summary.get("sample_interval_ms"),
        "expected_process_count": raw_summary.get("expected_process_count"),
        "metrics": {},
    }
    if not rows or start_ms is None or end_ms is None or end_ms <= start_ms:
        base["unavailable_reason"] = "resource_samples_or_completion_window_missing"
        return base

    start_row, end_row = _bracket_rows(rows, int(start_ms), int(end_ms))
    window_rows = [row for row in rows if int(start_ms) <= row["timestamp_ms"] <= int(end_ms)]
    rss_rows = _unique_rows_by_timestamp([start_row, *window_rows, end_row])
    if not rss_rows:
        base["unavailable_reason"] = "no_resource_samples_cover_completion_window"
        return base

    rss_values = [row["cluster_rss_bytes"] for row in rss_rows if row["sampled_process_count"] > 0]
    start_cpu_ms = _interpolated_cpu_time(rows, int(start_ms))
    end_cpu_ms = _interpolated_cpu_time(rows, int(end_ms))
    cpu_delta_ms = max(0.0, end_cpu_ms - start_cpu_ms)
    wall_ms = float(end_ms - start_ms)
    sampled_counts = [row["sampled_process_count"] for row in rss_rows]
    per_validator_rss = [row["cluster_rss_bytes"] / row["sampled_process_count"] for row in rss_rows if row["sampled_process_count"] > 0]
    expected = int(raw_summary.get("expected_process_count") or max((row["expected_process_count"] for row in rows), default=0))
    expected_samples = max(1, math.floor(wall_ms / max(float(raw_summary.get("sample_interval_ms") or 500), 1.0)) + 1)
    coverage = min(1.0, len(window_rows) / expected_samples) if expected_samples else None

    metrics: dict[str, Any] = {
        "cluster_node_cpu_time_ms": cpu_delta_ms,
        "resource_window_ms": wall_ms,
        "average_cluster_cpu_cores": cpu_delta_ms / wall_ms if wall_ms > 0 else None,
        "cluster_rss_peak_bytes": max(rss_values) if rss_values else None,
        "cluster_rss_mean_bytes": (sum(rss_values) / len(rss_values)) if rss_values else None,
        "cluster_rss_p95_bytes": _percentile(rss_values, 0.95) if rss_values else None,
        "cluster_rss_mean_per_sampled_validator_bytes": (sum(per_validator_rss) / len(per_validator_rss)) if per_validator_rss else None,
        "cluster_rss_peak_per_sampled_validator_bytes": max(per_validator_rss) if per_validator_rss else None,
        "resource_sample_count": len(window_rows),
        "resource_sample_interval_ms": raw_summary.get("sample_interval_ms"),
        "resource_sampling_coverage": coverage,
        "resource_sampled_process_count_min": min(sampled_counts) if sampled_counts else 0,
        "resource_sampled_process_count_max": max(sampled_counts) if sampled_counts else 0,
        "resource_expected_process_count": expected,
        "resource_window_start_ms": int(start_ms),
        "resource_window_end_ms": int(end_ms),
        "resource_window_alignment": "cpu_linear_interpolation_rss_bracketing_samples",
        "resource_start_sample_timestamp_ms": start_row["timestamp_ms"],
        "resource_end_sample_timestamp_ms": end_row["timestamp_ms"],
    }
    terminal = _positive_number(finality.get("terminal_unique_tx_count"))
    finalized = _positive_number(finality.get("finalized_unique_logical_tx_count"))
    if terminal:
        metrics["cpu_ms_per_terminal_tx"] = cpu_delta_ms / terminal
    if finalized:
        metrics["cpu_ms_per_finalized_tx"] = cpu_delta_ms / finalized

    base["available"] = bool(rss_values)
    base["metrics"] = metrics
    return base


def summarize_network_usage(
    run_dir: Path,
    finality: dict[str, Any] | None = None,
    cluster: dict[str, Any] | None = None,
) -> dict[str, Any]:
    run_dir = Path(run_dir)
    finality = finality or _read_json(run_dir / "finality_summary.json")
    cluster = cluster or _read_json(run_dir / "real_cluster_summary.json")
    start_ms = _number(finality.get("completion_window_start_ms"))
    end_ms = _number(finality.get("completion_window_end_ms"))
    base = {
        "schema_version": "mbe_v5_network_metrics_summary_v1",
        "scope": "successful_node_receive_application_envelopes",
        "byte_semantics": "application_layer_tcp_json_envelope_bytes_from_receive_decode",
        "measurement_boundary": "completion_window",
        "source_artifacts": ["nodes/*/network_log.csv", "finality_summary.json", "real_cluster_summary.json"],
        "available": False,
        "metrics": {},
        "categories": {},
        "scope_categories": {},
        "message_types": {},
    }
    if start_ms is None or end_ms is None or end_ms < start_ms:
        base["unavailable_reason"] = "completion_window_missing"
        return base

    node_execution_shards = _node_execution_shards(run_dir)
    delivered: list[dict[str, Any]] = []
    send_failures = 0
    receive_failures = 0
    fault_drops = 0
    read_paths = 0
    for path in sorted((run_dir / "nodes").glob("*/network_log.csv")):
        read_paths += 1
        try:
            with path.open("r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                for row in reader:
                    ts = _int_or_none(row.get("timestamp"))
                    direction = str(row.get("direction") or "")
                    success = _truthy(row.get("success"))
                    if ts is None or ts < int(start_ms) or ts > int(end_ms):
                        continue
                    # Diagnostic failures use the same formal completion window
                    # as delivered traffic, avoiding startup/shutdown pollution.
                    if direction in {"send", "state_access_send"} and not success:
                        send_failures += 1
                    if direction == "receive" and not success:
                        receive_failures += 1
                    if direction.startswith("fault_drop_"):
                        fault_drops += 1
                    if direction != "receive" or not success:
                        continue
                    delivered.append({
                        "timestamp_ms": ts,
                        "node_id": str(row.get("node_id") or ""),
                        "peer_id": str(row.get("peer_id") or ""),
                        "message_type": str(row.get("message_type") or ""),
                        "message_id": str(row.get("message_id") or ""),
                        "bytes": max(0, _int_or_none(row.get("bytes")) or 0),
                    })
        except OSError:
            continue

    if read_paths == 0:
        base["unavailable_reason"] = "network_logs_missing"
        return base

    categories: dict[str, dict[str, int]] = defaultdict(lambda: {"message_count": 0, "bytes": 0})
    scope_categories: dict[str, dict[str, int]] = defaultdict(lambda: {"message_count": 0, "bytes": 0})
    message_types: dict[str, dict[str, int]] = defaultdict(lambda: {"message_count": 0, "bytes": 0})
    for row in delivered:
        category = classify_network_message(row["message_type"], row["peer_id"])
        scope = classify_network_scope(row["node_id"], row["peer_id"], node_execution_shards)
        categories[category]["message_count"] += 1
        categories[category]["bytes"] += row["bytes"]
        scope_categories[scope]["message_count"] += 1
        scope_categories[scope]["bytes"] += row["bytes"]
        message_types[row["message_type"]]["message_count"] += 1
        message_types[row["message_type"]]["bytes"] += row["bytes"]

    total_messages = len(delivered)
    total_bytes = sum(row["bytes"] for row in delivered)
    for name in NETWORK_CATEGORIES:
        categories.setdefault(name, {"message_count": 0, "bytes": 0})
    category_payload: dict[str, dict[str, float | int]] = {}
    for name in NETWORK_CATEGORIES:
        item = categories[name]
        category_payload[name] = {
            **item,
            "message_share_percent": (100.0 * item["message_count"] / total_messages) if total_messages else 0.0,
            "byte_share_percent": (100.0 * item["bytes"] / total_bytes) if total_bytes else 0.0,
        }
    scope_payload: dict[str, dict[str, float | int]] = {}
    for name in NETWORK_SCOPE_CATEGORIES:
        item = scope_categories[name]
        scope_payload[name] = {
            **item,
            "message_share_percent": (100.0 * item["message_count"] / total_messages) if total_messages else 0.0,
            "byte_share_percent": (100.0 * item["bytes"] / total_bytes) if total_bytes else 0.0,
        }

    metrics: dict[str, Any] = {
        "delivered_network_message_count": total_messages,
        "delivered_network_bytes": total_bytes,
        "network_send_failure_count": send_failures,
        "network_receive_failure_count": receive_failures,
        "fault_drop_message_count": fault_drops,
        "network_measurement_window_start_ms": int(start_ms),
        "network_measurement_window_end_ms": int(end_ms),
        "network_log_file_count": read_paths,
    }
    terminal = _positive_number(finality.get("terminal_unique_tx_count"))
    finalized = _positive_number(finality.get("finalized_unique_logical_tx_count"))
    committed_blocks = _positive_number(
        cluster.get("actual_committed_block_count")
        if isinstance(cluster, dict)
        else None
    ) or _positive_number(finality.get("committed_block_count"))
    if terminal:
        metrics["network_messages_per_terminal_tx"] = total_messages / terminal
        metrics["network_bytes_per_terminal_tx"] = total_bytes / terminal
    if finalized:
        metrics["network_messages_per_finalized_tx"] = total_messages / finalized
        metrics["network_bytes_per_finalized_tx"] = total_bytes / finalized

    consensus = category_payload["consensus"]
    metrics["pbft_message_count"] = consensus["message_count"]
    metrics["pbft_network_bytes"] = consensus["bytes"]
    if committed_blocks:
        metrics["pbft_messages_per_committed_block"] = consensus["message_count"] / committed_blocks
        metrics["pbft_bytes_per_committed_block"] = consensus["bytes"] / committed_blocks
    for metric_key, message_type in {
        "pbft_preprepare_count": "PBFT_PRE_PREPARE",
        "pbft_prepare_count": "PBFT_PREPARE",
        "pbft_commit_count": "PBFT_COMMIT",
        "pbft_view_change_count": "PBFT_VIEW_CHANGE",
        "pbft_checkpoint_count": "PBFT_CHECKPOINT",
    }.items():
        metrics[metric_key] = message_types.get(message_type, {}).get("message_count", 0)

    base["available"] = True
    base["metrics"] = metrics
    base["categories"] = category_payload
    base["scope_categories"] = scope_payload
    base["message_types"] = dict(sorted(message_types.items()))
    base["category_message_count_invariant"] = sum(item["message_count"] for item in category_payload.values()) == total_messages
    base["category_byte_count_invariant"] = sum(item["bytes"] for item in category_payload.values()) == total_bytes
    base["scope_message_count_invariant"] = sum(item["message_count"] for item in scope_payload.values()) == total_messages
    base["scope_byte_count_invariant"] = sum(item["bytes"] for item in scope_payload.values()) == total_bytes
    return base



def summarize_metatrack_consensus_windows(run_dir: Path) -> dict[str, Any]:
    """Reconstruct v6.5.6.8 windows only from durable committed signed metadata."""
    run_dir = Path(run_dir)
    cluster = _read_json(run_dir / "real_cluster_summary.json")
    raw_block_limit = cluster.get("configured_block_size")
    try:
        block_limit = int(raw_block_limit) if raw_block_limit not in (None, "") and not isinstance(raw_block_limit, bool) else None
    except (TypeError, ValueError):
        block_limit = None
    representative_blocks: dict[tuple[str, int, str], dict[str, Any]] = {}
    source_artifacts: list[str] = []
    for node_dir in sorted((run_dir / "nodes").glob("*")):
        if not node_dir.is_dir():
            continue
        rows, blocks, sources = _metatrack_durable_window_rows(node_dir)
        if rows:
            _write_metatrack_window_execution_csv(node_dir / METATRACK_WINDOW_EXECUTION, rows)
        for key, block in blocks.items():
            representative_blocks.setdefault(key, block)
        for source in sources:
            try:
                rel = source.relative_to(run_dir).as_posix()
            except ValueError:
                rel = str(source)
            if rel not in source_artifacts:
                source_artifacts.append(rel)

    base: dict[str, Any] = {
        "schema_version": "mbe_metatrack_consensus_window_observability_v656814",
        "available": False,
        "truth_scope": "durable_signed_consensus_window_metadata_post_run_reconstruction_v2",
        "source_artifacts": sorted(source_artifacts),
        "metrics": {},
        "windows": [],
    }
    if not representative_blocks:
        base["unavailable_reason"] = "durable_metatrack_consensus_window_blocks_missing"
        return base

    transactions: dict[int, dict[str, Any]] = {}
    block_rows: list[dict[str, Any]] = []
    replica_consistent = True
    for key, block in sorted(representative_blocks.items(), key=lambda item: (item[0][0], item[0][1], item[0][2])):
        tx_list = block.get("tx_list") if isinstance(block.get("tx_list"), list) else []
        if not tx_list:
            continue
        routings = [
            tx.get("execution_routing") for tx in tx_list
            if isinstance(tx, dict) and isinstance(tx.get("execution_routing"), dict)
        ]
        if not routings or any(int(r.get("consensus_window_sequence") or 0) <= 0 for r in routings):
            continue
        first = routings[0]
        block_rows.append({
            "shard_id": str(block.get("shard_id") or first.get("execution_shard") or ""),
            "height": int(block.get("height") or 0),
            "block_hash": str(block.get("block_hash") or ""),
            "tx_count": len(tx_list),
            "window_sequence": int(first.get("consensus_window_sequence") or 0),
            "start_route_batch_sequence": int(first.get("consensus_window_start_batch_sequence") or 0),
            "end_route_batch_sequence": int(first.get("consensus_window_end_batch_sequence") or 0),
            "route_batch_count": int(first.get("consensus_window_route_batch_count") or 0),
            "global_transaction_count": int(first.get("consensus_window_transaction_count") or 0),
            "signed_shard_transaction_count": int(first.get("consensus_window_shard_transaction_count") or 0),
            "signed_critical_path": int(first.get("consensus_window_critical_path") or 0),
        })
        for item in tx_list:
            if not isinstance(item, dict):
                replica_consistent = False
                continue
            routing = item.get("execution_routing") if isinstance(item.get("execution_routing"), dict) else {}
            ordinal = int(routing.get("routing_ordinal") or 0)
            if ordinal <= 0:
                replica_consistent = False
                continue
            canonical = _metatrack_window_tx_projection(item)
            existing = transactions.get(ordinal)
            if existing is not None and existing != canonical:
                replica_consistent = False
            else:
                transactions[ordinal] = canonical

    if not transactions:
        base["unavailable_reason"] = "signed_window_transactions_missing"
        return base

    txs = [transactions[key] for key in sorted(transactions)]
    shard_by_ordinal = {int(tx["routing_ordinal"]): str(tx["execution_shard"]) for tx in txs}
    by_window: dict[int, list[dict[str, Any]]] = defaultdict(list)
    by_batch: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for tx in txs:
        by_window[int(tx["window_sequence"])].append(tx)
        by_batch[int(tx["route_batch_sequence"])].append(tx)

    window_sequences = sorted(key for key in by_window if key > 0)
    windows: list[dict[str, Any]] = []
    global_consistent = replica_consistent and window_sequences == list(range(1, len(window_sequences) + 1))
    for sequence in window_sequences:
        group = sorted(by_window[sequence], key=lambda tx: int(tx["routing_ordinal"]))
        signed = _metatrack_signed_window_identity(group[0])
        metadata_match = all(_metatrack_signed_window_identity(tx) == signed for tx in group)
        start_batch = int(signed[1])
        end_batch = int(signed[2])
        expected_batches = list(range(start_batch, end_batch + 1)) if start_batch > 0 and end_batch >= start_batch else []
        observed_batches = sorted({int(tx["route_batch_sequence"]) for tx in group})
        route_batches_match = observed_batches == expected_batches and int(signed[3]) == len(expected_batches)
        n = len(group)
        signed_n = int(signed[4])
        recomputed_l = _metatrack_recompute_critical_path(group)
        signed_l = int(signed[5])
        signed_critical_match = recomputed_l == signed_l
        shard_counts: dict[str, int] = defaultdict(int)
        signed_shard_counts: dict[str, set[int]] = defaultdict(set)
        for tx in group:
            shard = str(tx["execution_shard"])
            shard_counts[shard] += 1
            signed_shard_counts[shard].add(int(tx["window_shard_transaction_count"]))
        shard_count_match = all(
            len(signed_shard_counts[shard]) == 1
            and next(iter(signed_shard_counts[shard])) == count
            for shard, count in shard_counts.items()
        )
        tx_count_match = signed_n == n
        exact_edges = set()
        cross_edges = set()
        for tx in group:
            ordinal = int(tx["routing_ordinal"])
            for pred in _metatrack_execution_predecessors(tx):
                if pred in transactions:
                    exact_edges.add((ordinal, pred))
                    if shard_by_ordinal.get(pred) != str(tx["execution_shard"]):
                        cross_edges.add((ordinal, pred))
        rounds = [int(tx["consensus_execution_round"]) for tx in group if int(tx["consensus_execution_round"]) > 0]
        width = (n / recomputed_l) if recomputed_l > 0 else None
        row: dict[str, Any] = {
            "window_sequence": sequence,
            "start_route_batch_sequence": start_batch,
            "end_route_batch_sequence": end_batch,
            "route_batch_count": int(signed[3]),
            "transaction_count": n,
            "signed_transaction_count": signed_n,
            "recomputed_critical_path": recomputed_l,
            "signed_critical_path": signed_l,
            "signed_critical_path_match": signed_critical_match,
            "structural_width_numerator": n,
            "structural_width_denominator": recomputed_l,
            "structural_width": width,
            "shard_transaction_counts": dict(sorted(shard_counts.items())),
            "metadata_match": metadata_match,
            "route_batches_match": route_batches_match,
            "transaction_count_match": tx_count_match,
            "shard_transaction_count_match": shard_count_match,
            "global_round_min": min(rounds) if rounds else 0,
            "global_round_max": max(rounds) if rounds else 0,
            "global_round_span": (max(rounds) - min(rounds)) if rounds else 0,
            "exact_version_predecessor_edge_count": len(exact_edges),
            "cross_shard_exact_predecessor_edge_count": len(cross_edges),
        }
        next_batch_sequence = end_batch + 1
        next_batch = sorted(by_batch.get(next_batch_sequence, []), key=lambda tx: int(tx["routing_ordinal"]))
        if next_batch:
            candidate = sorted(group + next_batch, key=lambda tx: int(tx["routing_ordinal"]))
            candidate_l = _metatrack_recompute_critical_path(candidate)
            candidate_n = len(candidate)
            candidate_counts: dict[str, int] = defaultdict(int)
            for tx in candidate:
                candidate_counts[str(tx["execution_shard"])] += 1
            fits = block_limit is None or all(count <= block_limit for count in candidate_counts.values())
            improves = candidate_n * recomputed_l > n * candidate_l if recomputed_l > 0 and candidate_l > 0 else False
            if not fits:
                stop_reason = "block_size_limit"
                decision_match = True
            elif not improves:
                stop_reason = "critical_width_not_improved"
                decision_match = True
            else:
                stop_reason = "signed_window_boundary_rule_mismatch"
                decision_match = False
            row.update({
                "candidate_next_route_batch_sequence": next_batch_sequence,
                "candidate_transaction_count": candidate_n,
                "candidate_critical_path": candidate_l,
                "candidate_structural_width": (candidate_n / candidate_l) if candidate_l > 0 else None,
                "candidate_shard_transaction_counts": dict(sorted(candidate_counts.items())),
                "stop_reason": stop_reason,
                "decision_rule_match": decision_match,
            })
        else:
            row.update({
                "candidate_next_route_batch_sequence": None,
                "candidate_transaction_count": None,
                "candidate_critical_path": None,
                "candidate_structural_width": None,
                "candidate_shard_transaction_counts": {},
                "stop_reason": "input_end",
                "decision_rule_match": True,
            })
        row_consistent = all([
            metadata_match,
            route_batches_match,
            tx_count_match,
            shard_count_match,
            signed_critical_match,
            bool(row["decision_rule_match"]),
        ])
        row["signed_reconstruction_match"] = row_consistent
        global_consistent = global_consistent and row_consistent
        windows.append(row)

    for left, right in zip(windows, windows[1:]):
        if int(right["start_route_batch_sequence"]) != int(left["end_route_batch_sequence"]) + 1:
            global_consistent = False

    actual_by_window_shard = {
        (int(row["window_sequence"]), str(row["shard_id"])): int(row["tx_count"])
        for row in block_rows
    }
    expected_blocks = sum(len(row["shard_transaction_counts"]) for row in windows)
    actual_projection_match = True
    for row in windows:
        seq = int(row["window_sequence"])
        expected_counts = row["shard_transaction_counts"]
        for shard, count in expected_counts.items():
            if actual_by_window_shard.get((seq, shard)) != int(count):
                actual_projection_match = False
        if any(key[0] == seq and key[1] not in expected_counts for key in actual_by_window_shard):
            actual_projection_match = False
    if len(actual_by_window_shard) != expected_blocks:
        actual_projection_match = False
    global_consistent = global_consistent and actual_projection_match

    baseline_blocks = sum(
        len({str(tx["execution_shard"]) for tx in batch_txs})
        for batch_txs in by_batch.values()
    )
    imbalances = []
    for row in windows:
        counts = list(row["shard_transaction_counts"].values())
        if len(counts) > 1 and sum(counts) > 0:
            mean = sum(counts) / len(counts)
            imbalances.append((max(counts) - min(counts)) / mean if mean else 0.0)
    widths = [
        float(row["structural_width"])
        for row in windows
        if isinstance(row.get("structural_width"), (int, float)) and row["structural_width"] > 0
    ]
    stop_counts: dict[str, int] = defaultdict(int)
    for row in windows:
        stop_counts[str(row["stop_reason"])] += 1

    def avg(values: list[float]) -> float | None:
        return sum(values) / len(values) if values else None

    metrics: dict[str, Any] = {
        "metatrack_consensus_window_observability_available": True,
        "metatrack_consensus_window_count": len(windows),
        "metatrack_consensus_window_average_tx_count": avg([float(row["transaction_count"]) for row in windows]),
        "metatrack_consensus_window_average_route_batch_count": avg([float(row["route_batch_count"]) for row in windows]),
        "metatrack_consensus_window_max_route_batch_count": max((int(row["route_batch_count"]) for row in windows), default=0),
        "metatrack_consensus_window_average_critical_path": avg([float(row["recomputed_critical_path"]) for row in windows]),
        "metatrack_consensus_window_max_critical_path": max((int(row["recomputed_critical_path"]) for row in windows), default=0),
        "metatrack_consensus_window_average_structural_width": avg(widths),
        "metatrack_consensus_window_min_structural_width": min(widths) if widths else None,
        "metatrack_consensus_window_max_structural_width": max(widths) if widths else None,
        "metatrack_consensus_window_critical_width_stop_count": stop_counts["critical_width_not_improved"],
        "metatrack_consensus_window_block_size_stop_count": stop_counts["block_size_limit"],
        "metatrack_consensus_window_input_end_stop_count": stop_counts["input_end"],
        "metatrack_consensus_window_boundary_mismatch_count": stop_counts["signed_window_boundary_rule_mismatch"],
        "metatrack_consensus_window_average_shard_tx_imbalance": avg(imbalances),
        "metatrack_consensus_window_exact_predecessor_edge_count": sum(int(row["exact_version_predecessor_edge_count"]) for row in windows),
        "metatrack_consensus_window_cross_shard_exact_predecessor_edge_count": sum(int(row["cross_shard_exact_predecessor_edge_count"]) for row in windows),
        "metatrack_consensus_window_observed_pbft_block_count": len(actual_by_window_shard),
        "metatrack_consensus_window_expected_pbft_block_count": expected_blocks,
        "metatrack_consensus_window_baseline_route_batch_pbft_block_count": baseline_blocks,
        "metatrack_consensus_window_pbft_blocks_saved": max(0, baseline_blocks - expected_blocks),
        "metatrack_consensus_window_signed_reconstruction_match": global_consistent,
        "metatrack_consensus_window_actual_projection_match": actual_projection_match,
        "metatrack_consensus_window_truth_scope": "durable_signed_consensus_window_metadata_post_run_reconstruction_v2",
    }
    base["available"] = True
    base["metrics"] = metrics
    base["windows"] = windows
    base["observed_blocks"] = sorted(block_rows, key=lambda row: (int(row["window_sequence"]), str(row["shard_id"])))
    base["signed_reconstruction_match"] = global_consistent
    _write_metatrack_window_plan_jsonl(run_dir / METATRACK_WINDOW_PLAN, windows)
    aggregate_dir = run_dir / "aggregate"
    aggregate_dir.mkdir(parents=True, exist_ok=True)
    _write_json(aggregate_dir / METATRACK_WINDOW_SUMMARY, base)
    return base


def _metatrack_window_tx_projection(tx: dict[str, Any]) -> dict[str, Any]:
    routing = tx.get("execution_routing") if isinstance(tx.get("execution_routing"), dict) else {}
    return {
        "tx_id": str(tx.get("tx_id") or tx.get("logical_tx_id") or ""),
        "routing_ordinal": int(routing.get("routing_ordinal") or 0),
        "execution_shard": str(routing.get("execution_shard") or ""),
        "route_batch_sequence": int(routing.get("route_batch_sequence") or 0),
        "consensus_execution_predecessor_ordinals": [
            int(value) for value in (routing.get("consensus_execution_predecessor_ordinals") or []) if int(value) > 0
        ],
        "consensus_execution_round": int(routing.get("consensus_execution_round") or 0),
        "state_versions": [dict(value) for value in (routing.get("state_versions") or []) if isinstance(value, dict)],
        "window_sequence": int(routing.get("consensus_window_sequence") or 0),
        "window_start_batch": int(routing.get("consensus_window_start_batch_sequence") or 0),
        "window_end_batch": int(routing.get("consensus_window_end_batch_sequence") or 0),
        "window_route_batch_count": int(routing.get("consensus_window_route_batch_count") or 0),
        "window_transaction_count": int(routing.get("consensus_window_transaction_count") or 0),
        "window_shard_transaction_count": int(routing.get("consensus_window_shard_transaction_count") or 0),
        "window_critical_path": int(routing.get("consensus_window_critical_path") or 0),
    }


def _metatrack_signed_window_identity(tx: dict[str, Any]) -> tuple[int, int, int, int, int, int]:
    # Window identity contains only metadata that is globally identical across all
    # shard projections. consensus_window_shard_transaction_count is explicitly
    # shard-local and is validated separately by shard_count_match below.
    return (
        int(tx["window_sequence"]),
        int(tx["window_start_batch"]),
        int(tx["window_end_batch"]),
        int(tx["window_route_batch_count"]),
        int(tx["window_transaction_count"]),
        int(tx["window_critical_path"]),
    )


def _metatrack_execution_predecessors(tx: dict[str, Any]) -> list[int]:
    ordinal = int(tx.get("routing_ordinal") or 0)
    seen = {
        int(pred)
        for pred in (tx.get("consensus_execution_predecessor_ordinals") or [])
        if 0 < int(pred) < ordinal
    }
    for dep in tx.get("state_versions") or []:
        if not isinstance(dep, dict):
            continue
        pred = int(dep.get("required_version") or 0)
        required_round = int(dep.get("required_execution_round") or 0)
        if pred > 0 and required_round > 0 and pred < ordinal:
            seen.add(pred)
    return sorted(seen)


def _metatrack_recompute_critical_path(txs: list[dict[str, Any]]) -> int:
    depths: dict[int, int] = {}
    maximum = 0
    for tx in sorted(txs, key=lambda item: int(item.get("routing_ordinal") or 0)):
        ordinal = int(tx.get("routing_ordinal") or 0)
        depth = 1
        for pred in _metatrack_execution_predecessors(tx):
            depth = max(depth, depths.get(pred, 0) + 1)
        depths[ordinal] = depth
        maximum = max(maximum, depth)
    return maximum


def _metatrack_durable_window_rows(node_dir: Path) -> tuple[list[dict[str, Any]], dict[tuple[str, int, str], dict[str, Any]], list[Path]]:
    marker_path = node_dir / "commit_markers.jsonl"
    committed_hashes = {
        str(row.get("block_hash") or "")
        for row in _read_jsonl_dicts(marker_path)
        if row.get("committed") is True
    }
    source = node_dir / "committed_blocks.jsonl"
    wrapped_source = source.is_file()
    if not wrapped_source:
        source = node_dir / "blocks.jsonl"
    sources = [path for path in (source, marker_path) if path.is_file()]
    rows: list[dict[str, Any]] = []
    blocks: dict[tuple[str, int, str], dict[str, Any]] = {}
    for raw in _read_jsonl_dicts(source):
        block = raw.get("block") if isinstance(raw.get("block"), dict) else raw
        block_hash = str(block.get("block_hash") or "")
        if not wrapped_source and block_hash not in committed_hashes:
            continue
        tx_list = block.get("tx_list") if isinstance(block.get("tx_list"), list) else []
        routings = [
            tx.get("execution_routing")
            for tx in tx_list
            if isinstance(tx, dict) and isinstance(tx.get("execution_routing"), dict)
        ]
        if not routings:
            continue
        first = routings[0]
        sequence = int(first.get("consensus_window_sequence") or 0)
        if sequence <= 0:
            continue
        shard = str(block.get("shard_id") or first.get("execution_shard") or "")
        height = int(block.get("height") or 0)
        key = (shard, height, block_hash)
        blocks[key] = block
        rounds = [
            int(routing.get("consensus_execution_round") or 0)
            for routing in routings
            if int(routing.get("consensus_execution_round") or 0) > 0
        ]
        rows.append({
            "node_id": node_dir.name,
            "shard_id": shard,
            "height": height,
            "block_hash": block_hash,
            "tx_count": len(tx_list),
            "window_sequence": sequence,
            "start_route_batch_sequence": int(first.get("consensus_window_start_batch_sequence") or 0),
            "end_route_batch_sequence": int(first.get("consensus_window_end_batch_sequence") or 0),
            "route_batch_count": int(first.get("consensus_window_route_batch_count") or 0),
            "global_transaction_count": int(first.get("consensus_window_transaction_count") or 0),
            "signed_shard_transaction_count": int(first.get("consensus_window_shard_transaction_count") or 0),
            "signed_critical_path": int(first.get("consensus_window_critical_path") or 0),
            "global_round_min": min(rounds) if rounds else 0,
            "global_round_max": max(rounds) if rounds else 0,
            "global_round_span": (max(rounds) - min(rounds)) if rounds else 0,
        })
    return rows, blocks, sources


def _read_jsonl_dicts(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    out: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                value = json.loads(line)
                if isinstance(value, dict):
                    out.append(value)
    except (OSError, json.JSONDecodeError):
        return []
    return out


def _write_metatrack_window_plan_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _write_metatrack_window_execution_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    fields = [
        "node_id",
        "shard_id",
        "height",
        "block_hash",
        "tx_count",
        "window_sequence",
        "start_route_batch_sequence",
        "end_route_batch_sequence",
        "route_batch_count",
        "global_transaction_count",
        "signed_shard_transaction_count",
        "signed_critical_path",
        "global_round_min",
        "global_round_max",
        "global_round_span",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)

NETWORK_CATEGORIES = (
    "client_ingress",
    "transaction_gossip",
    "consensus",
    "porygon_esc",
    "cross_shard",
    "remote_state",
    "recovery_control",
    "other",
)

NETWORK_SCOPE_CATEGORIES = (
    "client",
    "intra_execution_shard",
    "inter_execution_shard",
    "unknown",
)


def classify_network_message(message_type: str, peer_id: str = "") -> str:
    kind = str(message_type or "").upper()
    peer = str(peer_id or "").lower()
    if kind == "TX_GOSSIP" and peer == "mbe-client":
        return "client_ingress"
    if kind == "TX_GOSSIP":
        return "transaction_gossip"
    if kind.startswith("PBFT_") or kind == "BLOCK_PROPOSAL":
        return "consensus"
    if kind.startswith("PORYGON_ESC_"):
        return "porygon_esc"
    if "XSHARD" in kind or "CROSS_SHARD" in kind or kind in {"V5_XSHARD_FINALIZE", "V5_XSHARD_FINALIZE_ACK"}:
        return "cross_shard"
    if "STATE_FETCH" in kind or "STATE_DELTA" in kind or "STATE_ACCESS" in kind or "REMOTE_STATE" in kind:
        return "remote_state"
    if "CATCHUP" in kind or kind in {"NODE_HELLO", "NODE_SHUTDOWN"}:
        return "recovery_control"
    return "other"


def _node_execution_shards(run_dir: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted((Path(run_dir) / "nodes").glob("*/node_runtime_status.json")):
        status = _read_json(path)
        node_id = str(status.get("node_id") or "").strip()
        shard_id = str(status.get("execution_shard_id") or status.get("shard_id") or "").strip()
        if node_id and shard_id:
            out[node_id] = shard_id
    return out


def classify_network_scope(node_id: str, peer_id: str, node_execution_shards: dict[str, str]) -> str:
    peer = str(peer_id or "").strip()
    if peer.lower() == "mbe-client":
        return "client"
    receiver_shard = node_execution_shards.get(str(node_id or "").strip())
    sender_shard = node_execution_shards.get(peer)
    if not receiver_shard or not sender_shard:
        return "unknown"
    if receiver_shard == sender_shard:
        return "intra_execution_shard"
    return "inter_execution_shard"


def _read_resource_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                timestamp = _int_or_none(row.get("timestamp_ms"))
                cpu = _float_or_none(row.get("cluster_cpu_time_ms"))
                rss = _int_or_none(row.get("cluster_rss_bytes"))
                sampled = _int_or_none(row.get("sampled_process_count"))
                expected = _int_or_none(row.get("expected_process_count"))
                if timestamp is None or cpu is None or rss is None:
                    continue
                rows.append({
                    "timestamp_ms": timestamp,
                    "cluster_cpu_time_ms": cpu,
                    "cluster_rss_bytes": max(0, rss),
                    "sampled_process_count": max(0, sampled or 0),
                    "expected_process_count": max(0, expected or 0),
                })
    except OSError:
        return []
    return sorted(rows, key=lambda item: item["timestamp_ms"])


def _bracket_rows(rows: list[dict[str, Any]], start_ms: int, end_ms: int) -> tuple[dict[str, Any], dict[str, Any]]:
    before = [row for row in rows if row["timestamp_ms"] <= start_ms]
    after_start = [row for row in rows if row["timestamp_ms"] >= start_ms]
    after = [row for row in rows if row["timestamp_ms"] >= end_ms]
    before_end = [row for row in rows if row["timestamp_ms"] <= end_ms]
    start_row = before[-1] if before else (after_start[0] if after_start else rows[0])
    end_row = after[0] if after else (before_end[-1] if before_end else rows[-1])
    if end_row["timestamp_ms"] < start_row["timestamp_ms"]:
        end_row = start_row
    return start_row, end_row


def _interpolated_cpu_time(rows: list[dict[str, Any]], timestamp_ms: int) -> float:
    if not rows:
        return 0.0
    before = [row for row in rows if row["timestamp_ms"] <= timestamp_ms]
    after = [row for row in rows if row["timestamp_ms"] >= timestamp_ms]
    left = before[-1] if before else rows[0]
    right = after[0] if after else rows[-1]
    if right["timestamp_ms"] <= left["timestamp_ms"]:
        return float(left["cluster_cpu_time_ms"])
    ratio = (timestamp_ms - left["timestamp_ms"]) / (right["timestamp_ms"] - left["timestamp_ms"])
    ratio = max(0.0, min(1.0, float(ratio)))
    return float(left["cluster_cpu_time_ms"]) + ratio * (float(right["cluster_cpu_time_ms"]) - float(left["cluster_cpu_time_ms"]))


def _unique_rows_by_timestamp(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    by_timestamp = {int(row["timestamp_ms"]): row for row in rows if row}
    return [by_timestamp[key] for key in sorted(by_timestamp)]


def _percentile(values: list[int], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return ordered[0]
    index = (len(ordered) - 1) * p
    lower = math.floor(index)
    upper = math.ceil(index)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _write_network_csv(path: Path, network: dict[str, Any]) -> None:
    categories = network.get("categories") if isinstance(network.get("categories"), dict) else {}
    message_types = network.get("message_types") if isinstance(network.get("message_types"), dict) else {}
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["scope", "category", "message_type", "message_count", "bytes", "message_share_percent", "byte_share_percent"])
        writer.writeheader()
        for name in NETWORK_CATEGORIES:
            item = categories.get(name) if isinstance(categories.get(name), dict) else {}
            writer.writerow({"scope": "category", "category": name, "message_type": "", **item})
        scopes = network.get("scope_categories") if isinstance(network.get("scope_categories"), dict) else {}
        for name in NETWORK_SCOPE_CATEGORIES:
            item = scopes.get(name) if isinstance(scopes.get(name), dict) else {}
            writer.writerow({"scope": "communication_scope", "category": name, "message_type": "", **item})
        for name, item in sorted(message_types.items()):
            if not isinstance(item, dict):
                continue
            writer.writerow({"scope": "message_type", "category": classify_network_message(name), "message_type": name, "message_count": item.get("message_count", 0), "bytes": item.get("bytes", 0), "message_share_percent": "", "byte_share_percent": ""})


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def _truthy(value: object) -> bool:
    return str(value or "").strip().lower() in {"true", "1", "yes"}


def _int_or_none(value: object) -> int | None:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _float_or_none(value: object) -> float | None:
    try:
        if value is None or value == "" or isinstance(value, bool):
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _number(value: object) -> float | None:
    return _float_or_none(value)


def _positive_number(value: object) -> float | None:
    number = _float_or_none(value)
    return number if number is not None and number > 0 else None
