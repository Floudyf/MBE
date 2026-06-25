from pathlib import Path

from backend.app.services.sweep_report_v2 import generate_markdown_report
from backend.app.services.sweep_runner_v2 import SWEEP_CONFIGS, run_sweep


def test_v2_sweep_writes_summary_json_report_and_runtime_log(tmp_path: Path) -> None:
    result = run_sweep(SWEEP_CONFIGS["v2_baseline_sweep"], tmp_path)

    assert result["stage"] == "V2.8"
    assert result["status"] == "completed"
    assert (tmp_path / "sweep_summary.csv").is_file()
    assert (tmp_path / "sweep_summary.json").is_file()
    assert (tmp_path / "sweep_report.md").is_file()
    assert (tmp_path / "runtime.log").is_file()
    assert (tmp_path / "case_artifacts_index.json").is_file()

    report = (tmp_path / "sweep_report.md").read_text(encoding="utf-8")
    assert "## Data Truth" in report
    assert "## Non-goals" in report
    assert "not real chain execution" in report
    assert "not a production cross-chain bridge" in report
    assert "MetaFlow" in report


def test_v2_sweep_report_generator_is_conservative() -> None:
    markdown = generate_markdown_report({
        "sweep_id": "v2_committee_delay_sweep",
        "rows": [{"case_id": "case_000001", "case_type": "protocol_baseline", "protocol_name": "committee_bridge_basic", "status": "completed"}],
        "config": {"sweep": {"data_truth_label": "synthetic_replay", "backend_type": "local_virtual", "protocol_truth": "local_baseline_model"}, "parameters": {}, "protocols": ["committee_bridge_basic"]},
        "artifacts": [],
    })

    assert "local virtual-time replay only" in markdown
    assert "not real chain execution" in markdown
    assert "not real committee signature latency" in markdown
