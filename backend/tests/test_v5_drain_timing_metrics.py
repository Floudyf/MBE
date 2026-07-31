import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import extract
from backend.app.services.v5_formal_scheduler import _is_paper_candidate_result
from backend.app.services.v5_real_cluster_runner import _completion_gate, _run_status_from_completion


def _write_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")


def _write_required_files(run_dir: Path) -> None:
    for name in ["transaction_lifecycle.jsonl", "transaction_finality.csv", "client_receipt_log.csv", "throughput_windows.csv"]:
        (run_dir / name).write_text("", encoding="utf-8")


def _finality(**overrides: object) -> dict:
    data = {
        "logical_transaction_count": 60,
        "finalized_unique_logical_tx_count": 60,
        "throughput_tps": 10.0,
        "logical_window_start_ms": 1000,
        "logical_window_end_ms": 5000,
        "logical_finality_duration_ms": 4000,
        "logical_finality_tps": 15.0,
        "drain_started_at_ms": 5000,
        "drain_finished_at_ms": 7000,
        "drain_duration_ms": 2000,
        "system_delta_drain_block_count": 2,
        "completion_window_start_ms": 1000,
        "completion_window_end_ms": 7000,
        "completion_duration_ms": 6000,
        "end_to_end_tps": 10.0,
        "tail_completion_overhead_ms": 2000,
        "p50_finality_ms": 100,
        "p95_finality_ms": 200,
        "p99_finality_ms": 300,
    }
    data.update(overrides)
    return data


def test_metric_extractor_preserves_logical_tps_and_uses_end_to_end_throughput(tmp_path: Path) -> None:
    _write_json(tmp_path / "real_cluster_summary.json", {"ready_to_commit": True, "no_fallback": True, "state_root_consistent": True, "receipt_root_consistent": True, "plan_digest_consistent": True})
    _write_json(tmp_path / "finality_summary.json", _finality())
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 7000})
    _write_required_files(tmp_path)

    metrics = extract(tmp_path)

    assert metrics["throughput_tps"] == 10.0
    assert metrics["end_to_end_tps"] == 10.0
    assert metrics["logical_finality_tps"] == 15.0
    assert metrics["completion_duration_ms"] == 6000
    assert metrics["drain_duration_ms"] == 2000
    assert metrics["missing"] == []


def test_metric_extractor_blocks_missing_drain_completion_fields(tmp_path: Path) -> None:
    _write_json(tmp_path / "real_cluster_summary.json", {"ready_to_commit": True, "no_fallback": True, "state_root_consistent": True, "receipt_root_consistent": True, "plan_digest_consistent": True})
    finality = _finality()
    finality.pop("completion_duration_ms")
    _write_json(tmp_path / "finality_summary.json", finality)
    _write_required_files(tmp_path)

    metrics = extract(tmp_path)

    assert "drain_status.json" in metrics["missing"]
    assert "finality_summary.json:completion_duration_ms" in metrics["missing"]


def test_completion_gate_rejects_pending_system_delta_after_logical_finality(tmp_path: Path) -> None:
    _write_json(tmp_path / "finality_summary.json", _finality())
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 7000})
    node_dir = tmp_path / "node_0"
    node_dir.mkdir()
    _write_json(node_dir / "node_runtime_status.json", {"node_id": "node_0", "pending_state_delta_count": 1, "pending_state_delta_key_count": 0, "ready_state_delta_count": 0, "pending_commit_count": 0, "proposal_in_flight": False})

    gate = _completion_gate(tmp_path, {"finality_evidence": {"incomplete_unique_tx_count": 0}})

    assert gate["passed"] is False
    assert "node_runtime_status:node_0:pending_state_delta_count_not_zero" in gate["blockers"]


def test_completion_gate_reads_formal_nodes_directory_status(tmp_path: Path) -> None:
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 7000})
    node_dir = tmp_path / "nodes" / "n0"
    node_dir.mkdir(parents=True)
    _write_json(
        node_dir / "node_runtime_status.json",
        {
            "node_id": "n0",
            "pending_state_delta_count": 0,
            "pending_state_delta_key_count": 0,
            "ready_state_delta_count": 1,
            "pending_commit_count": 0,
            "proposal_in_flight": False,
        },
    )
    _write_json(tmp_path / "finality_summary.json", _finality())

    gate = _completion_gate(tmp_path, {"finality_evidence": {"incomplete_unique_tx_count": 0}})

    assert gate["passed"] is False
    assert "node_runtime_status:n0:ready_state_delta_count_not_zero" in gate["blockers"]


def test_completion_gate_passes_after_drain_quiescence_and_zero_pending(tmp_path: Path) -> None:
    _write_json(tmp_path / "finality_summary.json", _finality())
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 7000})
    node_dir = tmp_path / "node_0"
    node_dir.mkdir()
    _write_json(node_dir / "node_runtime_status.json", {"node_id": "node_0", "pending_state_delta_count": 0, "pending_state_delta_key_count": 0, "ready_state_delta_count": 0, "pending_commit_count": 0, "proposal_in_flight": False})

    gate = _completion_gate(tmp_path, {"finality_evidence": {"incomplete_unique_tx_count": 0}})

    assert gate == {"passed": True, "blockers": []}


def test_run_status_depends_on_process_and_completion_gate_not_paper_candidate_readiness() -> None:
    assert _run_status_from_completion(0, {"passed": True, "blockers": []}) == "completed"
    assert _run_status_from_completion(1, {"passed": True, "blockers": []}) == "failed"
    assert _run_status_from_completion(0, {"passed": False, "blockers": ["pending"]}) == "failed"


def test_paper_candidate_inputs_block_old_throughput_window(tmp_path: Path) -> None:
    _write_json(tmp_path / "real_cluster_summary.json", {"ready_to_commit": True, "no_fallback": True, "state_root_consistent": True, "receipt_root_consistent": True, "plan_digest_consistent": True})
    _write_json(tmp_path / "finality_summary.json", _finality(throughput_tps=15.0))
    _write_json(tmp_path / "drain_status.json", {"completion_reason": "drain_quiescent", "drain_finished_at": 7000})
    _write_required_files(tmp_path)

    metrics = extract(tmp_path)

    assert "finality_summary.json:throughput_tps_must_equal_end_to_end_tps" in metrics["missing"]


def test_paper_candidate_requires_complete_metric_completeness() -> None:
    result = {"status": "completed", "summary": {"ready_to_commit": True, "no_fallback": True}}

    assert _is_paper_candidate_result(result, {"metric_completeness": "incomplete", "missing": []}) is False
    assert _is_paper_candidate_result(result, {"metric_completeness": "complete", "missing": ["metric:worker_count"]}) is False
    assert _is_paper_candidate_result(result, {"metric_completeness": "complete", "missing": []}) is True
