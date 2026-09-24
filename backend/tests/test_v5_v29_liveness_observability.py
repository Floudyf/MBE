import csv
import json
from pathlib import Path

from backend.app.services.v5_observability_metrics import summarize_network_usage, summarize_resource_usage
from backend.app.services.v5_real_cluster_runner import _completion_gate


def _write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_v29_network_protocol_and_execution_shard_scope_are_separate(tmp_path: Path) -> None:
    _write_json(tmp_path / "nodes" / "n0" / "node_runtime_status.json", {"node_id": "n0", "shard_id": "porygon-global", "execution_shard_id": "s0"})
    _write_json(tmp_path / "nodes" / "n1" / "node_runtime_status.json", {"node_id": "n1", "shard_id": "porygon-global", "execution_shard_id": "s1"})
    log = tmp_path / "nodes" / "n0" / "network_log.csv"
    with log.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "node_id", "peer_id", "direction", "message_type", "message_id", "height", "view", "sequence", "bytes", "success", "error", "latency_ms"])
        writer.writerow([1500, "n0", "n1", "receive", "PORYGON_ESC_WAVE_RESULT_V1", "m1", 1, 0, 0, 512, "true", "", 1])
    finality = {"completion_window_start_ms": 1000, "completion_window_end_ms": 2000, "terminal_unique_tx_count": 1}
    result = summarize_network_usage(tmp_path, finality, {"actual_committed_block_count": 1})
    assert result["categories"]["porygon_esc"]["message_count"] == 1
    assert result["categories"]["cross_shard"]["message_count"] == 0
    assert result["scope_categories"]["inter_execution_shard"]["message_count"] == 1


def test_v29_resource_summary_exposes_per_validator_rss(tmp_path: Path) -> None:
    (tmp_path / "resource_sampler_summary.json").write_text(json.dumps({"sampling_available": True, "sample_interval_ms": 500, "expected_process_count": 8}), encoding="utf-8")
    with (tmp_path / "resource_usage_timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp_ms", "cluster_cpu_time_ms", "cluster_rss_bytes", "sampled_process_count", "expected_process_count"])
        writer.writerow([1000, 0, 800, 8, 8])
        writer.writerow([1500, 500, 1600, 8, 8])
        writer.writerow([2000, 1000, 800, 8, 8])
    result = summarize_resource_usage(tmp_path, {"completion_window_start_ms": 1000, "completion_window_end_ms": 2000})
    metrics = result["metrics"]
    assert metrics["cluster_rss_mean_per_sampled_validator_bytes"] == 400 / 3
    assert metrics["cluster_rss_peak_per_sampled_validator_bytes"] == 200


def test_v29_completion_gate_rejects_pending_exact_version_work(tmp_path: Path) -> None:
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 2000})
    _write_json(tmp_path / "finality_summary.json", {
        "logical_window_start_ms": 1000, "logical_window_end_ms": 1500, "logical_finality_duration_ms": 500,
        "logical_finality_tps": 2, "drain_finished_at_ms": 2000, "drain_duration_ms": 500,
        "completion_window_start_ms": 1000, "completion_window_end_ms": 2000, "completion_duration_ms": 1000,
        "end_to_end_tps": 1, "throughput_tps": 1,
    })
    _write_json(tmp_path / "nodes" / "n0" / "node_runtime_status.json", {
        "node_id": "n0", "pending_state_delta_count": 0, "pending_state_delta_key_count": 0,
        "ready_state_delta_count": 0, "pending_commit_count": 0, "proposal_in_flight": False,
        "pending_state_fetch_count": 1, "pending_state_version_subscription_count": 2,
        "pending_version_admission_watch_count": 3,
    })
    summary = {"finality_evidence": {"incomplete_unique_tx_count": 0}, "initial_state_digest": "a", "global_final_state_digest": "b"}
    gate = _completion_gate(tmp_path, summary)
    blockers = set(gate["blockers"])
    assert "node_runtime_status:n0:pending_state_fetch_count_not_zero" in blockers
    assert "node_runtime_status:n0:pending_state_version_subscription_count_not_zero" in blockers
    assert "node_runtime_status:n0:pending_version_admission_watch_count_not_zero" in blockers
