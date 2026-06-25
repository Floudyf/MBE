from pathlib import Path

import yaml
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.dual_chain_profiles import DualChainConfigError, load_dual_chain_config

ROOT = Path(__file__).resolve().parents[2]
client = TestClient(app)


def test_v2_planned_dual_chain_yaml_stays_non_runnable() -> None:
    document = yaml.safe_load((ROOT / "configs/topologies/v2_dual_chain_planned.yaml").read_text(encoding="utf-8"))

    assert document["status"] == "planned"
    assert document["runnable"] is False


def test_v2_5_sample_is_explicitly_runnable_but_does_not_change_composer_guard() -> None:
    sample = load_dual_chain_config(ROOT / "configs/experiments/v2_dual_chain_sample.yaml")
    preview = client.post("/api/v2/composer/preview", json={"topology": "dual_chain", "trace_source": "synthetic", "cross_chain_protocol": "disabled"})

    assert sample["stage"] == "V2.5"
    assert sample["runnable"] is True
    assert preview.status_code == 200
    assert preview.json()["status"] == "planned"
    assert preview.json()["runnable"] is False


def test_loader_refuses_planned_dual_chain_topology() -> None:
    try:
        load_dual_chain_config(ROOT / "configs/topologies/v2_dual_chain_planned.yaml")
    except DualChainConfigError as exc:
        assert "planned" in str(exc)
    else:
        raise AssertionError("planned topology must not be runnable")
