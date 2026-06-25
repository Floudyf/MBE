import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.app.services.protocol_replay import ProtocolReplayError, run_protocol_replay

ROOT = Path(__file__).resolve().parents[2]
SAMPLE_CONFIG = ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml"


def test_protocol_replay_runner_writes_outputs(tmp_path: Path) -> None:
    result = run_protocol_replay(SAMPLE_CONFIG, tmp_path)

    assert result["status"] == "completed"
    assert result["stage"] == "V2.6"
    assert result["data_truth_label"] == "synthetic_replay"
    assert {item["protocol_name"] for item in result["summary"]["items"]} == {
        "committee_bridge_basic",
        "fixed_window_baseline",
        "lock_mint_pipeline",
        "lock_mint_serial",
    }
    for name in ("used_config.yaml", "used_config.json", "protocol_summary.csv", "protocol_summary.json", "protocol_results.csv", "protocol_events.csv", "runtime.log", "report.md"):
        assert (tmp_path / name).is_file()

    log_text = (tmp_path / "runtime.log").read_text(encoding="utf-8")
    assert "stage: V2.6" in log_text
    assert "no Docker/Fabric/network.sh started" in log_text
    assert "no time.Sleep used" in log_text
    assert "not MetaFlow" in log_text


def test_protocol_replay_rejects_invalid_schema(tmp_path: Path) -> None:
    trace = tmp_path / "bad_trace.jsonl"
    meta = tmp_path / "bad_meta.json"
    config = tmp_path / "config.yaml"
    trace.write_text(json.dumps({"schema_version": "v2.cross_chain_trace.v1"}) + "\n", encoding="utf-8")
    meta.write_text(json.dumps({
        "schema_version": "v2.multi_chain_trace_meta.v1",
        "trace_format": "jsonl",
        "data_truth_label": "synthetic_replay",
        "chain_count": 2,
        "chains": [
            {"chain_id": "chain_a", "role": "source", "backend": "mock_chain", "block_interval_ms": 100, "finality_depth": 3},
            {"chain_id": "chain_b", "role": "target", "backend": "mock_chain", "block_interval_ms": 300, "finality_depth": 5},
        ],
        "cross_tx_count": 1,
        "stage_record_count": 1,
        "limitations": ["test"],
    }), encoding="utf-8")
    config.write_text(
        "\n".join([
            "version: v2",
            "stage: v2.6",
            "experiment: {name: bad, runnable: true}",
            "trace:",
            "  trace_file: bad_trace.jsonl",
            "  meta_file: bad_meta.json",
            "  data_truth_label: synthetic_replay",
            "chains:",
            "  chain_a: {chain_id: chain_a, role: source, backend: mock_chain, backend_type: local_virtual, block_interval_ms: 100, finality_depth: 3}",
            "  chain_b: {chain_id: chain_b, role: target, backend: mock_chain, backend_type: local_virtual, block_interval_ms: 300, finality_depth: 5}",
            "protocols:",
            "  - {name: lock_mint_serial, enabled: true}",
        ])
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(ProtocolReplayError, match="schema validation failed"):
        run_protocol_replay(config, tmp_path / "out", root=tmp_path)


def test_protocol_replay_rejects_planned_live_backend(tmp_path: Path) -> None:
    config = tmp_path / "live_backend.yaml"
    (tmp_path / "trace.jsonl").write_text((ROOT / "trace/samples/v2_cross_trace_small.jsonl").read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "meta.json").write_text((ROOT / "trace/samples/v2_multi_chain_trace_meta.json").read_text(encoding="utf-8").replace("v2_cross_trace_small.jsonl", "trace.jsonl"), encoding="utf-8")
    text = SAMPLE_CONFIG.read_text(encoding="utf-8")
    text = text.replace("trace_file: trace/samples/v2_cross_trace_small.jsonl", "trace_file: trace.jsonl")
    text = text.replace("meta_file: trace/samples/v2_multi_chain_trace_meta.json", "meta_file: meta.json")
    text = text.replace("backend_type: local_virtual", "backend_type: fabric_live", 1)
    config.write_text(text, encoding="utf-8")

    with pytest.raises(Exception, match="not runnable"):
        run_protocol_replay(config, tmp_path / "out", root=tmp_path)


def test_cli_runs_protocol_replay_to_requested_output(tmp_path: Path) -> None:
    output = tmp_path / "cli_out"
    result = subprocess.run(
        [sys.executable, "scripts/v2_6_cross_chain_protocol_replay.py", "--config", "configs/experiments/v2_cross_chain_protocol_sample.yaml", "--out", str(output)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert '"status": "completed"' in result.stdout
    assert (output / "protocol_summary.json").is_file()
