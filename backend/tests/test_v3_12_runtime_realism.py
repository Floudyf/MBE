from __future__ import annotations

import pytest

from backend.app.models.v3_composer_draft import V3RuntimeTopology
from backend.app.services.artifact_manager import ARTIFACT_ALLOWLIST
from backend.app.services.v3_runtime_topology import normalize_topology, stage_metadata


def test_local_multi_process_topology_validation_passes() -> None:
    data, errors = normalize_topology(
        V3RuntimeTopology(
            node_runtime_mode="local_multi_process",
            process_runtime_mode="dry_run",
            max_local_processes=4,
            enable_committee_epoch=True,
            epoch_count=2,
        )
    )

    assert errors == []
    assert data["node_runtime_mode"] == "local_multi_process"
    assert data["process_runtime_mode"] == "dry_run"
    assert data["max_local_processes"] == 4
    assert data["epoch_count"] == 2


@pytest.mark.parametrize("mode", ["dry_run", "smoke"])
def test_process_runtime_modes_validate(mode: str) -> None:
    _, errors = normalize_topology(V3RuntimeTopology(node_runtime_mode="local_multi_process", process_runtime_mode=mode))

    assert errors == []


@pytest.mark.parametrize("value", [0, 33])
def test_max_local_processes_range_validation(value: int) -> None:
    _, errors = normalize_topology(V3RuntimeTopology(max_local_processes=value))

    assert any("max_local_processes" in error for error in errors)


@pytest.mark.parametrize("value", [0, 6])
def test_epoch_count_range_validation(value: int) -> None:
    _, errors = normalize_topology(V3RuntimeTopology(epoch_count=value))

    assert any("epoch_count" in error for error in errors)


def test_v3_12_artifacts_are_downloadable() -> None:
    expected = {
        "address_table.json",
        "multi_process_manifest.json",
        "node_process_log.csv",
        "node_lifecycle_log.csv",
        "network_message_log.csv",
        "node_process_status.json",
        "local_multi_process_summary.json",
        "shard_assignment_log.csv",
        "committee_assignment_log.csv",
        "committee_summary.json",
        "epoch_log.csv",
        "reconfiguration_plan.json",
        "reshard_plan_log.csv",
        "reconfiguration_summary.json",
    }

    assert expected.issubset(ARTIFACT_ALLOWLIST)


def test_stage_metadata_reflects_v3_12() -> None:
    metadata = stage_metadata()

    assert metadata["current_stage"] == "V3.12 Runtime Realism Closure"
    assert metadata["runtime_truth"] == "local_multi_process_runtime_mvp_not_production_cluster"
    assert metadata["next_stage"] == "V3.13 Metaverse Experiment Suite Closure"
