from pathlib import Path

import pytest

from backend.app.services.artifact_manager import ArtifactForbidden, get_artifact_path, list_artifacts
from backend.app.services.protocol_replay import run_protocol_replay

ROOT = Path(__file__).resolve().parents[2]


def test_protocol_artifacts_are_allowlisted(tmp_path: Path) -> None:
    run_protocol_replay(ROOT / "configs/experiments/v2_cross_chain_protocol_sample.yaml", tmp_path)

    names = {item["name"] for item in list_artifacts(tmp_path, "run1")}

    assert "protocol_summary.csv" in names
    assert "protocol_summary.json" in names
    assert "protocol_results.csv" in names
    assert "protocol_events.csv" in names


def test_protocol_artifacts_do_not_open_arbitrary_files(tmp_path: Path) -> None:
    (tmp_path / "protocol_secret.json").write_text("{}\n", encoding="utf-8")

    with pytest.raises(ArtifactForbidden):
        get_artifact_path(tmp_path, "protocol_secret.json")
