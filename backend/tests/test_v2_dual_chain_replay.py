import json
import subprocess
import sys
from pathlib import Path

import pytest

from backend.app.services.dual_chain_profiles import DualChainConfigError
from backend.app.services.dual_chain_replay import run_dual_chain_replay

ROOT = Path(__file__).resolve().parents[2]
SAMPLE_CONFIG = ROOT / "configs/experiments/v2_dual_chain_sample.yaml"


def test_dual_chain_replay_writes_expected_artifacts(tmp_path: Path) -> None:
    result = run_dual_chain_replay(SAMPLE_CONFIG, tmp_path)

    assert result["status"] == "completed"
    assert result["summary"]["cross_tx_count"] == 2
    assert result["summary"]["stage_record_count"] == 9
    assert result["summary"]["source_backend_type"] == "local_virtual"
    assert result["summary"]["target_backend_type"] == "local_virtual"
    assert result["summary"]["data_truth_label"] == "synthetic_replay"
    for name in ("used_config.yaml", "used_config.json", "dual_chain_summary.csv", "dual_chain_summary.json", "stage_metrics.csv", "runtime.log", "report.md"):
        assert (tmp_path / name).is_file()

    log_text = (tmp_path / "runtime.log").read_text(encoding="utf-8")
    assert "docker_fabric_network_sh_started=false" in log_text
    assert "cross_chain_protocol_execution=false" in log_text


def test_dual_chain_replay_rejects_planned_topology() -> None:
    with pytest.raises(DualChainConfigError, match="planned"):
        run_dual_chain_replay(ROOT / "configs/topologies/v2_dual_chain_planned.yaml", ROOT / ".cache/should_not_run")


def test_dual_chain_replay_rejects_invalid_schema(tmp_path: Path) -> None:
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
            "stage: V2.5",
            "topology: dual_chain",
            "status: runnable",
            "runnable: true",
            "data_truth_label: synthetic_replay",
            "trace:",
            f"  path: {trace.name}",
            f"  meta_path: {meta.name}",
            "chains:",
            "  chain_a: {chain_id: chain_a, role: source, backend: mock_chain, backend_type: local_virtual, block_interval_ms: 100, finality_depth: 3}",
            "  chain_b: {chain_id: chain_b, role: target, backend: mock_chain, backend_type: local_virtual, block_interval_ms: 300, finality_depth: 5}",
        ])
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(DualChainConfigError, match="schema validation failed"):
        run_dual_chain_replay(config, tmp_path / "out", root=tmp_path)


def test_cli_runs_sample_to_requested_output(tmp_path: Path) -> None:
    output = tmp_path / "cli_out"
    result = subprocess.run(
        [sys.executable, "scripts/v2_5_dual_chain_replay.py", "--config", "configs/experiments/v2_dual_chain_sample.yaml", "--out", str(output)],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=True,
    )

    assert '"status": "completed"' in result.stdout
    assert (output / "dual_chain_summary.json").is_file()
