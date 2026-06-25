from pathlib import Path

import pytest

from backend.app.services.artifact_manager import ArtifactForbidden, get_artifact_path, list_artifacts
from backend.app.services.dual_chain_replay import run_dual_chain_replay

ROOT = Path(__file__).resolve().parents[2]


def test_dual_chain_artifacts_are_allowlisted(tmp_path: Path) -> None:
    run_dual_chain_replay(ROOT / "configs/experiments/v2_dual_chain_sample.yaml", tmp_path)

    names = {item["name"] for item in list_artifacts(tmp_path, "run1")}

    assert "dual_chain_summary.csv" in names
    assert "dual_chain_summary.json" in names
    assert "stage_metrics.csv" in names
    assert "runtime.log" in names
    assert "report.md" in names


def test_dual_chain_artifacts_do_not_expand_to_arbitrary_files(tmp_path: Path) -> None:
    (tmp_path / "secret.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(ArtifactForbidden):
        get_artifact_path(tmp_path, "secret.json")
