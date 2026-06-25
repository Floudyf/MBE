from pathlib import Path

import csv

from backend.app.services.protocol_replay import run_protocol_replay

ROOT = Path(__file__).resolve().parents[2]


def summaries_by_protocol(tmp_path: Path) -> dict[str, dict]:
    result = run_protocol_replay(ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml", tmp_path)
    return {item["protocol_name"]: item for item in result["summary"]["items"]}


def test_protocol_summary_contains_required_metrics(tmp_path: Path) -> None:
    summaries = summaries_by_protocol(tmp_path)
    serial = summaries["lock_mint_serial"]

    for key in (
        "success_count",
        "timeout_count",
        "refund_count",
        "avg_e2e_latency_ms",
        "p99_e2e_latency_ms",
        "max_pending_count",
        "source_backend_type",
        "target_backend_type",
        "data_truth_label",
    ):
        assert key in serial
    assert serial["data_truth_label"] == "synthetic_replay"
    assert serial["source_backend_type"] == "local_virtual"


def test_pipeline_and_serial_have_observable_pending_difference(tmp_path: Path) -> None:
    summaries = summaries_by_protocol(tmp_path)

    assert summaries["lock_mint_serial"]["max_pending_count"] == 1
    assert summaries["lock_mint_pipeline"]["max_pending_count"] >= 2


def test_committee_delay_changes_latency(tmp_path: Path) -> None:
    summaries = summaries_by_protocol(tmp_path)

    assert summaries["committee_bridge_basic"]["avg_e2e_latency_ms"] > summaries["lock_mint_pipeline"]["avg_e2e_latency_ms"]


def test_protocol_results_and_events_are_written(tmp_path: Path) -> None:
    run_protocol_replay(ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml", tmp_path)
    with (tmp_path / "protocol_results.csv").open(encoding="utf-8", newline="") as stream:
        results = list(csv.DictReader(stream))
    with (tmp_path / "protocol_events.csv").open(encoding="utf-8", newline="") as stream:
        events = list(csv.DictReader(stream))

    assert results
    assert events
    assert {"protocol_name", "cross_tx_id", "backend_type", "data_truth_label"}.issubset(results[0])
    assert {"event_id", "event_type", "source", "status"}.issubset(events[0])
