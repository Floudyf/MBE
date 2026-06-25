from __future__ import annotations

from collections import defaultdict
from statistics import mean
from typing import Any

from backend.app.services.chain_backend import ChainProfile
from backend.app.services.dual_chain_profiles import source_target_profiles


FINAL_STATUSES = {"completed", "timeout", "refunded", "failed"}


def percentile(values: list[float], percentile_value: float) -> float:
    if not values:
        return 0.0
    sorted_values = sorted(values)
    index = min(len(sorted_values) - 1, int(round((percentile_value / 100) * (len(sorted_values) - 1))))
    return float(sorted_values[index])


def summarize_dual_chain_metrics(
    stage_metrics: list[dict[str, Any]],
    profiles: dict[str, ChainProfile],
    data_truth_label: str,
) -> dict[str, Any]:
    source, target = source_target_profiles(profiles)
    by_cross_tx: dict[str, list[dict[str, Any]]] = defaultdict(list)
    status_by_cross_tx: dict[str, str] = {}
    for row in stage_metrics:
        by_cross_tx[str(row["cross_tx_id"])].append(row)
        if row["status"] in FINAL_STATUSES:
            status_by_cross_tx[str(row["cross_tx_id"])] = str(row["status"])

    e2e_latencies = []
    for rows in by_cross_tx.values():
        start = min(int(row["submit_time_ms"]) for row in rows)
        end = max(int(row["expected_finality_time_ms"]) for row in rows)
        e2e_latencies.append(float(end - start))

    source_wait = sum(float(row["finality_wait_time_ms"]) for row in stage_metrics if row["chain_id"] == source.chain_id)
    target_wait = sum(float(row["finality_wait_time_ms"]) for row in stage_metrics if row["chain_id"] == target.chain_id)
    finality_wait = source_wait + target_wait
    source_budget = max(source.finality_budget_ms, 1)
    target_budget = target.finality_budget_ms

    return {
        "cross_tx_count": len(by_cross_tx),
        "stage_record_count": len(stage_metrics),
        "completed_cross_tx_count": sum(1 for status in status_by_cross_tx.values() if status == "completed"),
        "timeout_cross_tx_count": sum(1 for status in status_by_cross_tx.values() if status == "timeout"),
        "refunded_cross_tx_count": sum(1 for status in status_by_cross_tx.values() if status == "refunded"),
        "failed_cross_tx_count": sum(1 for status in status_by_cross_tx.values() if status == "failed"),
        "avg_e2e_latency_ms": round(mean(e2e_latencies), 3) if e2e_latencies else 0.0,
        "p99_e2e_latency_ms": round(percentile(e2e_latencies, 99), 3),
        "avg_stage_latency_ms": round(mean(float(row["stage_latency_ms"]) for row in stage_metrics), 3) if stage_metrics else 0.0,
        "finality_wait_time_ms": int(finality_wait),
        "source_wait_time_ms": int(source_wait),
        "target_wait_time_ms": int(target_wait),
        "chain_speed_imbalance": round(target_budget / source_budget, 6),
        "source_chain_id": source.chain_id,
        "target_chain_id": target.chain_id,
        "source_backend_type": source.backend_type,
        "target_backend_type": target.backend_type,
        "source_block_interval_ms": source.block_interval_ms,
        "target_block_interval_ms": target.block_interval_ms,
        "source_finality_depth": source.finality_depth,
        "target_finality_depth": target.finality_depth,
        "data_truth_label": data_truth_label,
    }
