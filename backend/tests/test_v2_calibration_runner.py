from pathlib import Path

import pytest
import yaml

from backend.app.services.calibration_runner_v2 import CALIBRATION_CONFIGS, CalibrationBlocked, compare_observed_vs_replay, load_calibration_config, run_calibration


def test_calibration_configs_exist_and_load() -> None:
    assert set(CALIBRATION_CONFIGS) == {"v2_synthetic_calibration_sample", "v2_fabric_smoke_calibration"}
    for path in CALIBRATION_CONFIGS.values():
        assert path.is_file()
        config = load_calibration_config(path)
        assert config["stage"] == "v2.9"
        assert config["calibration"]["runnable"] is True
        assert config["replay"]["sleep_enabled"] is False


def test_synthetic_calibration_sample_runs_and_writes_artifacts(tmp_path: Path) -> None:
    result = run_calibration(CALIBRATION_CONFIGS["v2_synthetic_calibration_sample"], tmp_path)

    assert result["stage"] == "V2.9"
    assert result["status"] == "completed"
    assert result["summary"]["data_truth_label"] == "synthetic_replay"
    assert result["summary"]["backend_type"] == "local_virtual"
    assert result["summary"]["calibration_truth"] == "synthetic_observation_sample"
    assert result["summary"]["observed_record_count"] > 0
    assert result["summary"]["replay_record_count"] > 0
    assert (tmp_path / "calibration_summary.csv").is_file()
    assert (tmp_path / "calibration_summary.json").is_file()
    assert (tmp_path / "replay_vs_observed.csv").is_file()
    assert (tmp_path / "calibration_report.md").is_file()
    assert (tmp_path / "runtime.log").is_file()


def test_compare_observed_vs_replay_outputs_error_fields() -> None:
    comparison = compare_observed_vs_replay(
        [{"record_id": "s1", "stage_id": "s1", "observed_commit_time_ms": 10, "observed_finality_time_ms": 20, "observed_latency_ms": 20}],
        [{"stage_id": "s1", "expected_commit_time_ms": 15, "expected_finality_time_ms": 25, "stage_latency_ms": 25}],
    )

    row = comparison["rows"][0]
    assert row["commit_error_ms"] == 5
    assert row["finality_error_ms"] == 5
    assert row["latency_error_ms"] == 5
    assert row["matched"] is True


def test_fabric_calibration_blocks_when_trace_missing() -> None:
    config = yaml.safe_load(CALIBRATION_CONFIGS["v2_fabric_smoke_calibration"].read_text(encoding="utf-8"))
    config["input"]["trace_file"] = ".cache/test_v2_missing_fabric/trace.jsonl.gz"
    config["input"]["meta_file"] = ".cache/test_v2_missing_fabric/trace_meta.json"
    path = Path(".cache/test_v2_missing_fabric_config.yaml")
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")

    with pytest.raises(CalibrationBlocked) as exc_info:
        run_calibration(path, Path(".cache/test_v2_missing_fabric_calibration"))

    assert exc_info.value.payload["status"] == "blocked"
    assert "scripts/v1_fabric_smoke.py" in exc_info.value.payload["cli_command"]
