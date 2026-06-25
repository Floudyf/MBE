from pathlib import Path

from backend.app.services.chain_backed_trace_adapter import adapt_trace_for_calibration, detect_fabric_smoke_trace, extract_observed_records


def test_fabric_smoke_status_missing_only_checks_files(tmp_path: Path) -> None:
    status = detect_fabric_smoke_trace(tmp_path)

    assert status["status"] == "missing"
    assert status["web_starts_fabric"] is False
    assert "scripts/v1_fabric_smoke.py" in status["cli_command"]
    assert "network.sh" in status["warnings"][0]


def test_chain_backed_adapter_extracts_existing_trace_without_starting_chain(tmp_path: Path) -> None:
    trace = tmp_path / "trace.jsonl"
    meta = tmp_path / "trace_meta.json"
    trace.write_text('{"tx_id":"tx1","stage":"invoke","chain_id":"fabric","submit_time_ms":0,"commit_time_ms":10,"finality_time_ms":10,"stage_latency_ms":10}\n', encoding="utf-8")
    meta.write_text('{"source":"fabric_smoke"}\n', encoding="utf-8")

    observed = extract_observed_records(trace)
    adapted = adapt_trace_for_calibration(trace, meta)

    assert observed["records"][0]["tx_id"] == "tx1"
    assert adapted["scope"] == "single_chain_fabric_smoke"
    assert any("not full cross-chain trace" in warning for warning in adapted["warnings"])
