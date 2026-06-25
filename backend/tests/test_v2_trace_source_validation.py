from pathlib import Path

from fastapi.testclient import TestClient

from backend.app import main
from backend.app.services.trace_source_validator import validate_trace_source


client = TestClient(main.app)


def test_synthetic_validation_is_ready_but_not_real_chain_execution() -> None:
    result = validate_trace_source({"source_id": "synthetic", "workload": "asset_hotspot_v1", "tx_count": 100})

    assert result["status"] == "ready"
    assert result["runnable"] is True
    assert result["data_truth_label"] == "synthetic_replay"
    assert any("not real chain execution" in warning for warning in result["warnings"])


def test_existing_trace_validation_accepts_workspace_file_without_loading_it() -> None:
    result = validate_trace_source({"source_id": "existing_trace", "trace_path": "tests/golden/trace_small.jsonl.gz"})

    assert result["status"] == "ready"
    assert result["runnable"] is True
    assert result["data_truth_label"] == "existing_trace_replay"
    assert result["meta_detected"] is True
    assert result["size_bytes"] > 0


def test_existing_trace_validation_rejects_escape_and_missing_file(tmp_path: Path) -> None:
    escaped = validate_trace_source({"source_id": "existing_trace", "trace_path": "../secret.json"})
    missing = validate_trace_source({"source_id": "existing_trace", "trace_path": "tests/golden/missing.jsonl.gz"})

    assert escaped["status"] == "invalid"
    assert "trace_path_outside_workspace" in escaped["blocked_by"]
    assert missing["status"] == "missing_file"
    assert "trace_path_missing" in missing["blocked_by"]


def test_existing_trace_api_validation_works_for_golden_trace() -> None:
    response = client.post("/api/v2/trace-sources/validate", json={"source_id": "existing_trace", "trace_path": "tests/golden/trace_small.jsonl.gz"})

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "ready"
    assert payload["data_truth_label"] == "existing_trace_replay"
    assert payload["meta_detected"] is True


def test_fabric_chain_backed_validation_only_checks_files(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(main, "V1_FABRIC_SMOKE_OUT", tmp_path)
    missing = client.post("/api/v2/trace-sources/validate", json={"source_id": "fabric_chain_backed_trace"})
    (tmp_path / "trace.jsonl.gz").write_bytes(b"fake")
    (tmp_path / "trace_meta.json").write_text("{}", encoding="utf-8")
    ready = client.post("/api/v2/trace-sources/validate", json={"source_id": "fabric_chain_backed_trace"})

    assert missing.status_code == 200
    assert missing.json()["status"] == "missing"
    assert "v1_fabric_smoke.py" in missing.json()["cli_command"]
    assert any("never starts Docker" in warning for warning in missing.json()["warnings"])
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"
    assert ready.json()["runnable"] is True
