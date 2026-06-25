from pathlib import Path

import pytest
import yaml

from backend.app.services.calibration_runner_v2 import CALIBRATION_CONFIGS, CalibrationError, load_calibration_config
from backend.app.services.cross_chain_protocols import list_cross_chain_protocols


def _workspace_tmp_config(name: str, config: dict) -> Path:
    root = Path(".cache/test_v2_calibration_boundaries")
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_v2_calibration_keeps_planned_topology_guard() -> None:
    planned = yaml.safe_load(Path("configs/topologies/v2_dual_chain_planned.yaml").read_text(encoding="utf-8"))

    assert planned["status"] == "planned"
    assert planned["runnable"] is False


def test_v2_calibration_rejects_live_backend_and_sleep() -> None:
    config = yaml.safe_load(CALIBRATION_CONFIGS["v2_synthetic_calibration_sample"].read_text(encoding="utf-8"))
    config["calibration"]["backend_type"] = "fabric_live"
    path = _workspace_tmp_config("bad_live_calibration.yaml", config)
    with pytest.raises(CalibrationError, match="local_virtual or trace_replay"):
        load_calibration_config(path)

    config = yaml.safe_load(CALIBRATION_CONFIGS["v2_synthetic_calibration_sample"].read_text(encoding="utf-8"))
    config["replay"]["sleep_enabled"] = True
    path = _workspace_tmp_config("bad_sleep_calibration.yaml", config)
    with pytest.raises(CalibrationError, match="sleep_enabled"):
        load_calibration_config(path)


def test_metaflow_remains_planned_not_runnable() -> None:
    protocols = {item["name"]: item for item in list_cross_chain_protocols()}

    assert protocols["metaflow"]["status"] == "planned"
