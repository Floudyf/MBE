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
    _write_json(run_dir / RESOURCE_SUMMARY, resource)
    _write_json(run_dir / NETWORK_SUMMARY, network)
    _write_network_csv(run_dir / NETWORK_MESSAGE_SUMMARY, network)
    return {"resource": resource, "network": network}


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
        "message_types": {},
    }
    if start_ms is None or end_ms is None or end_ms < start_ms:
        base["unavailable_reason"] = "completion_window_missing"
        return base

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
    message_types: dict[str, dict[str, int]] = defaultdict(lambda: {"message_count": 0, "bytes": 0})
    for row in delivered:
        category = classify_network_message(row["message_type"], row["peer_id"])
        categories[category]["message_count"] += 1
        categories[category]["bytes"] += row["bytes"]
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
    base["message_types"] = dict(sorted(message_types.items()))
    base["category_message_count_invariant"] = sum(item["message_count"] for item in category_payload.values()) == total_messages
    base["category_byte_count_invariant"] = sum(item["bytes"] for item in category_payload.values()) == total_bytes
    return base


NETWORK_CATEGORIES = (
    "client_ingress",
    "transaction_gossip",
    "consensus",
    "cross_shard",
    "remote_state",
    "recovery_control",
    "other",
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
    if "XSHARD" in kind or "CROSS_SHARD" in kind or kind in {"V5_XSHARD_FINALIZE", "V5_XSHARD_FINALIZE_ACK"}:
        return "cross_shard"
    if "STATE_FETCH" in kind or "STATE_DELTA" in kind or "STATE_ACCESS" in kind or "REMOTE_STATE" in kind:
        return "remote_state"
    if "CATCHUP" in kind or kind in {"NODE_HELLO", "NODE_SHUTDOWN"}:
        return "recovery_control"
    return "other"


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
