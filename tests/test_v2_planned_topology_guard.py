from pathlib import Path

import yaml

from backend.app.services.config_validator_v2 import validate_planned_topology_file, validate_selection
from backend.app.services.plugin_registry import load_registry


ROOT = Path(__file__).resolve().parents[1]
PLANNED_TOPOLOGY = ROOT / "configs/topologies/v2_dual_chain_planned.yaml"


def test_v2_dual_chain_planned_yaml_stays_planned_and_not_runnable() -> None:
    document = yaml.safe_load(PLANNED_TOPOLOGY.read_text(encoding="utf-8"))

    assert document["version"] == "v2"
    assert document["topology"] == "dual_chain"
    assert document["status"] == "planned"
    assert document["runnable"] is False
    assert "V2.5" in document["reason"]


def test_v2_planned_topology_validator_never_marks_yaml_runnable() -> None:
    result = validate_planned_topology_file()

    assert result["status"] == "planned"
    assert result["runnable"] is False
    assert "v2_dual_chain_planned" in result["blocked_by"]


def test_v2_force_run_for_planned_dual_chain_is_invalid() -> None:
    result = validate_selection({"topology": "dual_chain", "trace_source": "synthetic", "cross_chain_protocol": "disabled", "force_run": True}, load_registry())

    assert result["status"] == "invalid"
    assert result["runnable"] is False
    assert "planned_config_force_run" in result["blocked_by"]
