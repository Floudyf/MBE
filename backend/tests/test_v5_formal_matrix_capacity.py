from pathlib import Path

import pytest

from backend.app.services.v5_formal_plan_validator import (
    MAX_CHILD_RUNS,
    FormalPlanValidationError,
    validate_child_run_capacity,
)


def test_formal_matrix_capacity_accepts_paper_sweeps_and_boundary() -> None:
    assert MAX_CHILD_RUNS == 1024
    for child_count in (312, 520, 1024):
        validate_child_run_capacity(child_count)


def test_formal_matrix_capacity_rejects_above_boundary_and_empty() -> None:
    with pytest.raises(FormalPlanValidationError, match="1024 Child Run limit"):
        validate_child_run_capacity(1025)
    with pytest.raises(FormalPlanValidationError, match="at least one Child Run"):
        validate_child_run_capacity(0)


def test_frontend_uses_same_capacity_contract() -> None:
    repo = Path(__file__).resolve().parents[2]
    source = (repo / "frontend/src/pages/V5FormalRunPage.tsx").read_text(encoding="utf-8")
    assert "const MAX_FORMAL_CHILD_RUNS = 1024;" in source
    assert "input.estimatedChildren > MAX_FORMAL_CHILD_RUNS" in source
    assert "预计子实验 / 上限" in source
    assert "{childCount} / {MAX_FORMAL_CHILD_RUNS}" in source
    assert "超过 100 个子实验硬上限" not in source
