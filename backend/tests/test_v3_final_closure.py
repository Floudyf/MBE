from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from backend.app.models.v3_composer_draft import V3RuntimeTopology
from backend.app.services.artifact_manager import ARTIFACT_ALLOWLIST
from backend.app.services.v3_final_closure import write_v3_final_closure_artifacts
from backend.app.services.v3_realism_readiness import build_realism_readiness
from backend.app.services.v3_runtime_topology import normalize_topology, stage_metadata


FINAL_ARTIFACTS = {
    "fault_injection_config.json",
    "fault_injection_log.csv",
    "node_failure_log.csv",
    "node_recovery_log.csv",
    "network_fault_log.csv",
    "target_congestion_log.csv",
    "relay_fault_observation_log.csv",
    "fault_injection_summary.json",
    "observability_summary.json",
    "observability_timeline.csv",
    "component_health_summary.csv",
    "runtime_component_status.json",
    "final_artifact_catalog.json",
    "final_artifact_catalog.md",
    "v3_final_reproducibility_manifest.json",
    "v3_reproducibility_guide.md",
    "v3_experiment_manual.md",
    "v3_paper_experiment_mapping.md",
    "v3_final_summary.json",
}


def test_v3_final_artifacts_are_downloadable() -> None:
    assert FINAL_ARTIFACTS.issubset(ARTIFACT_ALLOWLIST)


def test_v3_final_topology_validation_accepts_fault_observability_repro_fields() -> None:
    topology, errors = normalize_topology(
        V3RuntimeTopology(
            fault_injection_enabled=True,
            fault_profile="mixed_fault",
            fault_seed=7,
            fault_start_round=2,
            fault_duration_rounds=3,
            failed_node_count=2,
            message_delay_ms=25,
            message_drop_ratio=0.5,
            target_congestion_ratio=0.25,
            relay_fault_mode="timeout",
            observability_enabled=True,
            observability_level="detailed",
            reproducibility_bundle_enabled=True,
            paper_mapping_enabled=True,
            final_artifact_catalog_enabled=True,
        )
    )

    assert errors == []
    assert topology["fault_profile"] == "mixed_fault"
    assert topology["relay_fault_mode"] == "timeout"
    assert topology["observability_level"] == "detailed"


@pytest.mark.parametrize(
    "patch",
    [
        {"fault_profile": "byzantine"},
        {"relay_fault_mode": "double_spend"},
        {"observability_level": "production"},
        {"message_drop_ratio": 1.1},
        {"target_congestion_ratio": -0.1},
        {"fault_seed": -1},
    ],
)
def test_v3_final_topology_validation_rejects_invalid_fields(patch: dict[str, object]) -> None:
    _, errors = normalize_topology(V3RuntimeTopology(**patch))
    assert errors


def test_v3_final_generator_writes_deterministic_fault_observability_and_repro_artifacts(tmp_path: Path) -> None:
    topology, errors = normalize_topology(
        V3RuntimeTopology(
            fault_injection_enabled=True,
            fault_profile="mixed_fault",
            fault_seed=314,
            failed_node_count=1,
            message_delay_ms=25,
            message_drop_ratio=0.25,
            target_congestion_ratio=0.25,
            relay_fault_mode="timeout",
            observability_level="detailed",
        )
    )
    assert errors == []

    metrics = write_v3_final_closure_artifacts(tmp_path, topology, {"network_message_count": 4, "relay_mvp_tx_count": 2, "metaverse_tx_count": 8})

    assert metrics["v3_final_enabled"] is True
    assert metrics["fault_profile"] == "mixed_fault"
    assert metrics["fault_event_count"] > 0
    assert metrics["node_failure_count"] == 1
    assert metrics["relay_fault_event_count"] == 1
    assert metrics["component_health_count"] >= 18
    assert metrics["reproducibility_manifest_available"] is True
    assert FINAL_ARTIFACTS.issubset({path.name for path in tmp_path.iterdir()})

    fault_summary = json.loads((tmp_path / "fault_injection_summary.json").read_text(encoding="utf-8"))
    observability = json.loads((tmp_path / "observability_summary.json").read_text(encoding="utf-8"))
    final_summary = json.loads((tmp_path / "v3_final_summary.json").read_text(encoding="utf-8"))
    with (tmp_path / "relay_fault_observation_log.csv").open(encoding="utf-8", newline="") as stream:
        relay_rows = list(csv.DictReader(stream))

    assert fault_summary["fault_injection_truth"] == "deterministic_fault_injection_mvp_not_byzantine_adversary"
    assert observability["observability_truth"] == "local_observability_summary_not_production_monitoring"
    assert final_summary["summary_metrics"]["v3_final_truth"] == "v3_final_emulator_closure_not_production_system"
    assert relay_rows[0]["relay_fault_mode"] == "timeout"
    assert "paper-grade final results" in (tmp_path / "v3_paper_experiment_mapping.md").read_text(encoding="utf-8")


def test_v3_final_none_fault_profile_writes_noop_summary(tmp_path: Path) -> None:
    topology, errors = normalize_topology(V3RuntimeTopology())
    assert errors == []

    metrics = write_v3_final_closure_artifacts(tmp_path, topology, {})

    assert metrics["fault_injection_enabled"] is False
    assert metrics["fault_profile"] == "none"
    assert metrics["fault_event_count"] == 0
    with (tmp_path / "fault_injection_log.csv").open(encoding="utf-8", newline="") as stream:
        assert len(list(csv.DictReader(stream))) == 0


def test_v3_final_stage_metadata_and_readiness_boundary() -> None:
    metadata = stage_metadata()
    readiness = build_realism_readiness()

    assert metadata["current_stage"] == "V3-final Fault, Observability, and Reproducibility Closure"
    assert metadata["runtime_truth"] == "v3_final_emulator_closure_not_production_system"
    assert metadata["next_stage"] == "V3 maintenance only; do not start V4 unless explicitly requested"
    assert readiness["closure_stage"] == "V3-final"
    assert "not production monitoring" in readiness["not_real_chain_claims"]
    assert "not production Byzantine adversary model" in readiness["not_real_chain_claims"]
