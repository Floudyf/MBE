from __future__ import annotations

import importlib.util
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "v5_execution_methods_closure_acceptance.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("v5_execution_methods_closure_acceptance", SCRIPT)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_effective_supervisor_timeout_honors_formal_duration_and_environment(monkeypatch) -> None:
    module = _load_module()
    monkeypatch.setenv("MBE_V5_REAL_CLUSTER_TIMEOUT_SECONDS", "7200")

    assert module.effective_supervisor_timeout_seconds(240, 3_600_000) == 7200
    monkeypatch.delenv("MBE_V5_REAL_CLUSTER_TIMEOUT_SECONDS")
    assert module.effective_supervisor_timeout_seconds(240, 3_600_000) == 3690


def test_run_supervisor_timeout_requests_normal_shutdown_before_tree_termination(monkeypatch, tmp_path: Path) -> None:
    module = _load_module()

    class FakeProcess:
        pid = 4242

        def __init__(self, *_args, **_kwargs) -> None:
            self.calls = 0
            self.returncode = 1

        def communicate(self, *, timeout=None):
            self.calls += 1
            if self.calls == 1:
                raise subprocess.TimeoutExpired(["supervisor"], timeout)
            return "shutdown output", "shutdown error"

    fake = FakeProcess()
    monkeypatch.setattr(module.subprocess, "Popen", lambda *_args, **_kwargs: fake)
    terminated = []
    monkeypatch.setattr(module, "_terminate_supervisor_process_tree", lambda process: terminated.append(process.pid))

    try:
        module.run_supervisor(["supervisor"], cwd=tmp_path, output=tmp_path, timeout_seconds=7)
    except subprocess.TimeoutExpired as exc:
        assert exc.timeout == 7
    else:
        raise AssertionError("expected timeout")

    assert (tmp_path / "stop.request").is_file()
    assert terminated == []
    assert "timed out after 7 seconds" in (tmp_path / "supervisor_stderr.log").read_text(encoding="utf-8")
