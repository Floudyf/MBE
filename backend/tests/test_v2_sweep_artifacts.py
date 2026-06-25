from pathlib import Path

import pytest

from backend.app.services.artifact_manager import ArtifactForbidden, get_artifact_path, list_artifacts


def test_v2_sweep_artifacts_are_allowlisted(tmp_path: Path) -> None:
    for filename in ["sweep_summary.csv", "sweep_summary.json", "sweep_report.md", "case_artifacts_index.json"]:
        (tmp_path / filename).write_text("ok\n", encoding="utf-8")

    artifacts = {artifact["name"] for artifact in list_artifacts(tmp_path, "v2run_test")}

    assert {"sweep_summary.csv", "sweep_summary.json", "sweep_report.md", "case_artifacts_index.json"} <= artifacts
    assert get_artifact_path(tmp_path, "sweep_report.md").name == "sweep_report.md"


def test_v2_sweep_artifact_allowlist_is_not_open(tmp_path: Path) -> None:
    (tmp_path / "secret.txt").write_text("no\n", encoding="utf-8")

    with pytest.raises(ArtifactForbidden):
        get_artifact_path(tmp_path, "secret.txt")
