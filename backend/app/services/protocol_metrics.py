from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any

from backend.app.services.chain_backend import ChainProfile
from backend.app.services.dual_chain_metrics import percentile
from backend.app.services.dual_chain_profiles import source_target_profiles


def pending_stats(results: list[dict[str, Any]]) -> tuple[int, float]:
    points: list[tuple[int, int]] = []
    for result in results:
        start = int(result["metadata"].get("started_at_ms", 0))
        end = start + int(result["e2e_latency_ms"])
        points.append((start, 1))
        points.append((end, -1))
    pending = 0
    samples = []
    for _, delta in sorted(points, key=lambda item: (item[0], item[1])):
        pending += delta
        samples.append(max(0, pending))
    return (max(samples) if samples else 0, round(mean(samples), 3) if samples else 0.0)


def summarize_protocol_results(
    result_rows: list[dict[str, Any]],
    event_rows: list[dict[str, Any]],
    profiles: dict[str, ChainProfile],
    data_truth_label: str,
) -> list[dict[str, Any]]:
    source, target = source_target_profiles(profiles)
    by_protocol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    events_by_protocol: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in result_rows:
        by_protocol[str(row["protocol_name"])].append(row)
    for row in event_rows:
        events_by_protocol[str(row["protocol_name"])].append(row)

    summaries = []
    for protocol_name, rows in sorted(by_protocol.items()):
        latencies = [float(row["e2e_latency_ms"]) for row in rows]
        max_pending, avg_pending = pending_stats(rows)
        action_count = sum(int(row["action_count"]) for row in rows)
        event_count = len(events_by_protocol.get(protocol_name, []))
        summaries.append({
            "protocol_name": protocol_name,
            "cross_tx_count": len(rows),
            "success_count": sum(1 for row in rows if row["success"]),
            "timeout_count": sum(1 for row in rows if row["timeout"]),
            "refund_count": sum(1 for row in rows if row["refunded"]),
            "failed_count": sum(1 for row in rows if row["failed"]),
            "avg_e2e_latency_ms": round(mean(latencies), 3) if latencies else 0.0,
            "p99_e2e_latency_ms": round(percentile(latencies, 99), 3),
            "avg_source_wait_time_ms": round(mean(float(row["source_wait_time_ms"]) for row in rows), 3) if rows else 0.0,
            "avg_target_wait_time_ms": round(mean(float(row["target_wait_time_ms"]) for row in rows), 3) if rows else 0.0,
            "avg_finality_wait_time_ms": round(mean(float(row["finality_wait_time_ms"]) for row in rows), 3) if rows else 0.0,
            "max_pending_count": max_pending,
            "avg_pending_count": avg_pending,
            "action_count": action_count,
            "event_count": event_count,
            "chain_speed_imbalance": round(target.finality_budget_ms / max(source.finality_budget_ms, 1), 6),
            "source_chain_id": source.chain_id,
            "target_chain_id": target.chain_id,
            "source_backend_type": source.backend_type,
            "target_backend_type": target.backend_type,
            "data_truth_label": data_truth_label,
        })
    return summaries
