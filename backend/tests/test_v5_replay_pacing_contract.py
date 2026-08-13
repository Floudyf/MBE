from __future__ import annotations

import pytest
from pydantic import ValidationError

from backend.app.models.v5_experiment_spec import V5WorkloadSourceSpec
from backend.app.services.v5_workload_data_plane import WorkloadPreviewRequest


def test_max_throughput_is_default_without_target_rate() -> None:
    source = V5WorkloadSourceSpec(requested_tx_count=10_000, seed=11)
    assert source.replay_mode == "max_throughput"
    assert source.target_submission_tps is None


def test_fixed_rate_requires_absolute_target_tps() -> None:
    with pytest.raises(ValidationError):
        V5WorkloadSourceSpec(
            requested_tx_count=10_000,
            seed=11,
            replay_mode="fixed_rate",
        )


def test_max_throughput_rejects_fixed_rate_target() -> None:
    with pytest.raises(ValidationError):
        V5WorkloadSourceSpec(
            requested_tx_count=10_000,
            seed=11,
            replay_mode="max_throughput",
            target_submission_tps=500,
        )


def test_fixed_rate_accepts_absolute_target_tps() -> None:
    source = V5WorkloadSourceSpec(
        requested_tx_count=10_000,
        seed=11,
        replay_mode="fixed_rate",
        target_submission_tps=500,
    )
    assert source.replay_mode == "fixed_rate"
    assert source.target_submission_tps == 500


def test_workload_preview_replay_pacing_contract() -> None:
    with pytest.raises(ValidationError):
        WorkloadPreviewRequest(source_type="synthetic", plugin_id="deterministic_signed_synthetic", requested_tx_count=1000, seed=11, replay_mode="fixed_rate")
    with pytest.raises(ValidationError):
        WorkloadPreviewRequest(source_type="synthetic", plugin_id="deterministic_signed_synthetic", requested_tx_count=1000, seed=11, replay_mode="max_throughput", target_submission_tps=500)
    request = WorkloadPreviewRequest(source_type="synthetic", plugin_id="deterministic_signed_synthetic", requested_tx_count=1000, seed=11, replay_mode="fixed_rate", target_submission_tps=500)
    assert request.target_submission_tps == 500
