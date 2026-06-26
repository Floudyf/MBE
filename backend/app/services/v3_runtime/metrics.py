from __future__ import annotations

from backend.app.services.v3_runtime.models import RuntimeSummary, TxResult


def percentile(values: list[int], pct: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round((pct / 100) * (len(ordered) - 1)))))
    return float(ordered[index])


def build_summary(
    *,
    run_id: str,
    stage: str,
    backend_type: str,
    truth_label: str,
    chain_profile_id: str,
    plugin_profile_id: str,
    experiment_profile_id: str,
    tx_results: list[TxResult],
    block_count: int,
) -> RuntimeSummary:
    success_count = sum(1 for tx in tx_results if tx.status == "success")
    failure_count = len(tx_results) - success_count
    latencies = [tx.latency_ms for tx in tx_results]
    first_submit = min((tx.submit_time_ms for tx in tx_results), default=0)
    last_commit = max((tx.commit_time_ms for tx in tx_results), default=0)
    duration_s = max(0.001, (last_commit - first_submit) / 1000)
    return RuntimeSummary(
        run_id=run_id,
        stage=stage,
        backend_type=backend_type,
        truth_label=truth_label,
        chain_profile_id=chain_profile_id,
        plugin_profile_id=plugin_profile_id,
        experiment_profile_id=experiment_profile_id,
        tx_count=len(tx_results),
        success_count=success_count,
        failure_count=failure_count,
        block_count=block_count,
        throughput_tps=round(len(tx_results) / duration_s, 6),
        avg_latency_ms=round(sum(latencies) / len(latencies), 6) if latencies else 0.0,
        p95_latency_ms=percentile(latencies, 95),
        p99_latency_ms=percentile(latencies, 99),
        runtime_mode="python_reference_single_process_logical_nodes",
    )
