import csv
import json
import math
from pathlib import Path

from backend.app.services.v5_observability_metrics import (
    classify_network_message,
    summarize_network_usage,
    summarize_resource_usage,
)


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_resource_summary_uses_cpu_baseline_delta_and_concurrent_cluster_rss(tmp_path: Path):
    _write_json(tmp_path / "resource_sampler_summary.json", {
        "sampling_available": True,
        "sample_interval_ms": 500,
        "expected_process_count": 2,
    })
    with (tmp_path / "resource_usage_timeseries.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp_ms", "cluster_cpu_time_ms", "cluster_rss_bytes", "sampled_process_count", "expected_process_count", "failed_process_count"])
        writer.writerow([900, 500, 1000, 2, 2, 0])
        writer.writerow([1000, 600, 1200, 2, 2, 0])
        writer.writerow([1500, 900, 2000, 2, 2, 0])
        writer.writerow([2000, 1200, 1600, 2, 2, 0])
        writer.writerow([2100, 1300, 1400, 2, 2, 0])
    finality = {
        "completion_window_start_ms": 1000,
        "completion_window_end_ms": 2000,
        "terminal_unique_tx_count": 10,
        "finalized_unique_logical_tx_count": 8,
    }
    summary = summarize_resource_usage(tmp_path, finality)
    assert summary["available"] is True
    metrics = summary["metrics"]
    assert math.isclose(metrics["cluster_node_cpu_time_ms"], 600.0)
    assert math.isclose(metrics["average_cluster_cpu_cores"], 0.6)
    assert metrics["cluster_rss_peak_bytes"] == 2000
    assert metrics["cluster_rss_mean_bytes"] == 1600
    assert math.isclose(metrics["cpu_ms_per_terminal_tx"], 60.0)
    assert math.isclose(metrics["cpu_ms_per_finalized_tx"], 75.0)


def test_network_summary_counts_successful_receive_once_inside_completion_window(tmp_path: Path):
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    with (node_dir / "network_log.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "node_id", "peer_id", "direction", "message_type", "message_id", "height", "view", "sequence", "bytes", "success", "error", "latency_ms"])
        writer.writerow([900, "n0", "mbe-client", "receive", "TX_GOSSIP", "a", 0, 0, 0, 100, "true", "", 0])
        writer.writerow([1100, "n0", "mbe-client", "receive", "TX_GOSSIP", "b", 0, 0, 0, 101, "true", "", 0])
        writer.writerow([1200, "n0", "n1", "receive", "PBFT_PREPARE", "c", 1, 0, 1, 201, "true", "", 0])
        writer.writerow([1200, "n0", "n1", "send", "PBFT_PREPARE", "c", 1, 0, 1, 20, "true", "", 0])
        writer.writerow([1300, "n0", "n1", "receive", "V5_STATE_FETCH_RESPONSE", "d", 1, 0, 1, 301, "true", "", 0])
        writer.writerow([1400, "n0", "n1", "receive", "UNKNOWN_NEW_TYPE", "e", 1, 0, 1, 401, "true", "", 0])
        writer.writerow([1500, "n0", "n1", "receive", "PBFT_COMMIT", "f", 1, 0, 1, 501, "false", "bad", 0])
        writer.writerow([1600, "n0", "n1", "fault_drop_send", "PBFT_COMMIT", "g", 1, 0, 1, 0, "false", "fault", 0])
        writer.writerow([2100, "n0", "n1", "receive", "PBFT_COMMIT", "h", 1, 0, 1, 600, "true", "", 0])
    finality = {
        "completion_window_start_ms": 1000,
        "completion_window_end_ms": 2000,
        "terminal_unique_tx_count": 4,
        "finalized_unique_logical_tx_count": 4,
    }
    summary = summarize_network_usage(tmp_path, finality)
    assert summary["available"] is True
    metrics = summary["metrics"]
    assert metrics["delivered_network_message_count"] == 4
    assert metrics["delivered_network_bytes"] == 101 + 201 + 301 + 401
    assert metrics["network_messages_per_terminal_tx"] == 1
    assert metrics["network_receive_failure_count"] == 1
    assert metrics["fault_drop_message_count"] == 1
    assert summary["categories"]["client_ingress"]["message_count"] == 1
    assert summary["categories"]["consensus"]["message_count"] == 1
    assert summary["categories"]["remote_state"]["message_count"] == 1
    assert summary["categories"]["other"]["message_count"] == 1
    assert summary["category_message_count_invariant"] is True
    assert summary["category_byte_count_invariant"] is True


def test_network_classifier_has_total_other_fallback():
    assert classify_network_message("TX_GOSSIP", "mbe-client") == "client_ingress"
    assert classify_network_message("TX_GOSSIP", "n1") == "transaction_gossip"
    assert classify_network_message("PBFT_COMMIT", "n1") == "consensus"
    assert classify_network_message("V5_XSHARD_FINALIZE_ACK", "n1") == "cross_shard"
    assert classify_network_message("V5_STATE_DELTA_APPLY", "n1") == "remote_state"
    assert classify_network_message("V5_CATCHUP_REQUEST", "n1") == "recovery_control"
    assert classify_network_message("FUTURE_MESSAGE", "n1") == "other"
