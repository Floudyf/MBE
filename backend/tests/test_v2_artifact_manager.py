from pathlib import Path

import pytest

from backend.app.services.artifact_manager import ArtifactError, ArtifactForbidden, ArtifactMissing, get_artifact_path, list_artifacts, mirror_run_to_latest


def test_artifact_manager_lists_only_allowlisted_files(tmp_path: Path) -> None:
    (tmp_path / "summary.csv").write_text("ok\n", encoding="utf-8")
    (tmp_path / "runtime.log").write_text("ok\n", encoding="utf-8")
    (tmp_path / "secret.txt").write_text("no\n", encoding="utf-8")

    artifacts = list_artifacts(tmp_path, "run1")

    assert {item["name"] for item in artifacts} == {"runtime.log", "summary.csv"}
    assert all(item["download_url"].startswith("/api/v2/runs/run1/artifacts/") for item in artifacts)


def test_artifact_path_rejects_escape_and_non_allowlisted_files(tmp_path: Path) -> None:
    with pytest.raises(ArtifactError):
        get_artifact_path(tmp_path, "../summary.csv")
    with pytest.raises(ArtifactForbidden):
        get_artifact_path(tmp_path, "secret.txt")


def test_missing_allowlisted_artifact_is_clear(tmp_path: Path) -> None:
    with pytest.raises(ArtifactMissing):
        get_artifact_path(tmp_path, "summary.csv")


def test_mirror_run_to_latest_replaces_latest_dir(tmp_path: Path) -> None:
    run_dir = tmp_path / "run"
    latest_dir = tmp_path / "latest"
    run_dir.mkdir()
    latest_dir.mkdir()
    (latest_dir / "old.txt").write_text("old\n", encoding="utf-8")
    (run_dir / "summary.csv").write_text("new\n", encoding="utf-8")

    mirror_run_to_latest(run_dir, latest_dir)

    assert (latest_dir / "summary.csv").read_text(encoding="utf-8") == "new\n"
    assert not (latest_dir / "old.txt").exists()
