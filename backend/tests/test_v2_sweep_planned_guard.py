from pathlib import Path

import pytest
import yaml

from backend.app.services.sweep_runner_v2 import SWEEP_CONFIGS, SweepError, load_sweep_config


def test_v2_dual_chain_planned_topology_remains_non_runnable() -> None:
    planned = yaml.safe_load(Path("configs/topologies/v2_dual_chain_planned.yaml").read_text(encoding="utf-8"))

    assert planned["status"] == "planned"
    assert planned["runnable"] is False


def _workspace_tmp_config(name: str, config: dict) -> Path:
    root = Path(".cache/test_v2_sweep_planned_guard")
    root.mkdir(parents=True, exist_ok=True)
    path = root / name
    path.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    return path


def test_v2_sweep_rejects_live_backend() -> None:
    config = yaml.safe_load(SWEEP_CONFIGS["v2_baseline_sweep"].read_text(encoding="utf-8"))
    config["sweep"]["backend_type"] = "fabric_live"
    path = _workspace_tmp_config("bad_live_sweep.yaml", config)

    with pytest.raises(SweepError, match="local_virtual or trace_replay"):
        load_sweep_config(path)


def test_v2_sweep_rejects_metaflow_and_sleep() -> None:
    config = yaml.safe_load(SWEEP_CONFIGS["v2_baseline_sweep"].read_text(encoding="utf-8"))
    config["protocols"].append("metaflow")
    path = _workspace_tmp_config("bad_metaflow_sweep.yaml", config)

    with pytest.raises(SweepError, match="MetaFlow"):
        load_sweep_config(path)

    config = yaml.safe_load(SWEEP_CONFIGS["v2_baseline_sweep"].read_text(encoding="utf-8"))
    config["runner"]["sleep_enabled"] = True
    path = _workspace_tmp_config("bad_sleep_sweep.yaml", config)

    with pytest.raises(SweepError, match="sleep_enabled"):
        load_sweep_config(path)
