from pathlib import Path

import pytest

from backend.app.services.artifact_manager import ArtifactForbidden, get_artifact_path, list_artifacts


def test_v2_calibration_artifacts_are_allowlisted(tmp_path: Path) -> None:
    for filename in ["calibration_summary.csv", "calibration_summary.json", "replay_vs_observed.csv", "calibration_report.md"]:
        (tmp_path / filename).write_text("ok\n", encoding="utf-8")

    artifacts = {artifact["name"] for artifact in list_artifacts(tmp_path, "v2run_test")}

    assert {"calibration_summary.csv", "calibration_summary.json", "replay_vs_observed.csv", "calibration_report.md"} <= artifacts
    assert get_artifact_path(tmp_path, "replay_vs_observed.csv").name == "replay_vs_observed.csv"


def test_v2_calibration_artifact_allowlist_is_not_open(tmp_path: Path) -> None:
    (tmp_path / "fabric_private.pem").write_text("no\n", encoding="utf-8")

    with pytest.raises(ArtifactForbidden):
        get_artifact_path(tmp_path, "fabric_private.pem")
