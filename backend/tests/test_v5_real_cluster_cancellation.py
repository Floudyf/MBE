from __future__ import annotations

import sys
from pathlib import Path

from backend.app.services.v5_real_cluster_runner import _run_supervisor_process


def test_supervisor_process_cancel_reaps_process_and_preserves_log_tail(tmp_path: Path) -> None:
    polls = 0

    def cancel_check() -> bool:
        nonlocal polls
        polls += 1
        return polls >= 4

    returncode, stdout, stderr, cancelled, timed_out = _run_supervisor_process(
        [
            sys.executable,
            "-c",
            "import time; print('cancel-test-started', flush=True); time.sleep(30)",
        ],
        tmp_path,
        10,
        cancel_check,
    )

    assert cancelled is True
    assert timed_out is False
    assert isinstance(returncode, int)
    assert "cancel-test-started" in stdout
    assert "cancellation requested" in stderr
