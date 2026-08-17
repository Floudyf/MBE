from __future__ import annotations
import importlib.util
import sys
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "v5_2_run_group_acceptance.py"


def _load():
    spec = importlib.util.spec_from_file_location("v5_2_acceptance_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_required(directory: Path, valid_zip: bool) -> None:
    for name in ("raw_summary.csv", "aggregate_summary.csv", "confidence_interval.csv", "formal_matrix.csv", "fairness_matrix.csv"):
        (directory / name).write_text("h\\nv\\n", encoding="utf-8")
    bundle = directory / "artifacts.zip"
    if valid_zip:
        with zipfile.ZipFile(bundle, "w", zipfile.ZIP_DEFLATED) as archive:
            archive.writestr("reproducibility_manifest.json", "{}\\n")
            archive.writestr("artifact_manifest.json", "{}\\n")
    else:
        bundle.write_bytes(b"not-a-zip")


def test_artifact_problems_accept_valid_bundle(tmp_path):
    module = _load()
    _write_required(tmp_path, True)
    assert module._artifact_problems(tmp_path) == []


def test_artifact_problems_reject_corrupt_bundle(tmp_path):
    module = _load()
    _write_required(tmp_path, False)
    problems = module._artifact_problems(tmp_path)
    assert any(item.startswith("artifacts.zip:invalid:BadZipFile:") for item in problems)


def test_terminal_wait_survives_completed_before_bundle(monkeypatch, tmp_path):
    module = _load()
    current = {"run_group_id": "g", "status": "completed", "bundle_status": "ready"}
    monkeypatch.setattr(module, "read_group", lambda _gid: dict(current))
    calls = {"n": 0}
    def problems(_directory):
        calls["n"] += 1
        return ["artifacts.zip:missing"] if calls["n"] < 3 else []
    monkeypatch.setattr(module, "_artifact_problems", problems)
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    observed = module._wait_for_terminal_artifacts("g", tmp_path, timeout_seconds=1.0, poll_seconds=0.01)
    assert observed["status"] == "completed"
    assert calls["n"] == 3


def test_terminal_wait_stops_on_bundle_failure(monkeypatch, tmp_path):
    module = _load()
    current = {"run_group_id": "g", "status": "completed", "bundle_status": "failed", "bundle_error": "synthetic"}
    monkeypatch.setattr(module, "read_group", lambda _gid: dict(current))
    monkeypatch.setattr(module, "_artifact_problems", lambda _directory: ["artifacts.zip:missing"])
    observed = module._wait_for_terminal_artifacts("g", tmp_path, timeout_seconds=90.0)
    assert observed["bundle_status"] == "failed"
