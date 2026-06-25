from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from backend.app.main import app
from backend.app.services.protocol_replay import ProtocolReplayError, run_protocol_replay

ROOT = Path(__file__).resolve().parents[2]
client = TestClient(app)


def test_planned_dual_chain_yaml_remains_non_runnable() -> None:
    document = yaml.safe_load((ROOT / "configs/topologies/v2_dual_chain_planned.yaml").read_text(encoding="utf-8"))

    assert document["status"] == "planned"
    assert document["runnable"] is False


def test_v2_6_sample_is_local_baseline_not_production_bridge() -> None:
    document = yaml.safe_load((ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml").read_text(encoding="utf-8"))

    assert document["stage"] == "v2.6"
    assert document["experiment"]["runnable"] is True
    assert document["replay"]["backend_interface"] == "ChainBackend"
    assert document["replay"]["protocol_interface"] == "CrossChainProtocol"
    assert any("not a production cross-chain bridge" in note for note in document["notes"])


def test_metaflow_config_is_rejected(tmp_path: Path) -> None:
    config = tmp_path / "metaflow.yaml"
    (tmp_path / "trace.jsonl").write_text((ROOT / "trace/samples/v2_cross_trace_small.jsonl").read_text(encoding="utf-8"), encoding="utf-8")
    (tmp_path / "meta.json").write_text((ROOT / "trace/samples/v2_multi_chain_trace_meta.json").read_text(encoding="utf-8").replace("v2_cross_trace_small.jsonl", "trace.jsonl"), encoding="utf-8")
    text = (ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml").read_text(encoding="utf-8")
    text = text.replace("trace_file: trace/samples/v2_cross_trace_small.jsonl", "trace_file: trace.jsonl")
    text = text.replace("meta_file: trace/samples/v2_multi_chain_trace_meta.json", "meta_file: meta.json")
    text = text.replace("name: lock_mint_serial", "name: metaflow", 1)
    config.write_text(text, encoding="utf-8")

    with pytest.raises(ProtocolReplayError, match="metaflow"):
        run_protocol_replay(config, tmp_path / "out", root=tmp_path)


def test_generic_cross_chain_composer_preview_is_not_production_runnable() -> None:
    response = client.post("/api/v2/composer/preview", json={"topology": "cross_chain_replay", "trace_source": "synthetic", "cross_chain_protocol": "lock_mint_serial"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "planned"
    assert payload["runnable"] is False
    assert payload["data_truth_label"] == "planned_cross_chain_replay"
