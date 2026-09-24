from __future__ import annotations

import csv
from pathlib import Path

from backend.app.services.v5_fairness_validator import write_artifacts


def test_formal_matrix_accepts_method_specific_fields_after_first_row(tmp_path: Path) -> None:
    rows = [
        {
            "child_run_id": "ordinary-first",
            "method_config_id": "hash_block_stm",
            "comparison_group_id": "g",
            "comparison_semantics_class": "stateful_local_legacy_v1",
            "runnable": True,
            "blockers": [],
        },
        {
            "child_run_id": "calvin-second",
            "method_config_id": "stateless_calvin",
            "comparison_group_id": "g",
            "comparison_semantics_class": "stateless_calvin_consensus_version_plan_adaptation_v4",
            "version_plan_policy": "pbft_bound_calvin_block_order_not_client_source_order",
            "runnable": True,
            "blockers": [],
        },
    ]
    write_artifacts(tmp_path, rows, {"passed": True})
    with (tmp_path / "formal_matrix.csv").open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        exported = list(reader)
        assert reader.fieldnames is not None
        assert "version_plan_policy" in reader.fieldnames
    assert len(exported) == 2
    assert exported[0]["version_plan_policy"] == ""
    assert exported[1]["version_plan_policy"] == "pbft_bound_calvin_block_order_not_client_source_order"


def test_formal_matrix_empty_rows_keeps_child_id_header(tmp_path: Path) -> None:
    write_artifacts(tmp_path, [], {"passed": True})
    header = (tmp_path / "formal_matrix.csv").read_text(encoding="utf-8").splitlines()[0]
    assert header == "child_run_id"
