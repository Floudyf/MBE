from __future__ import annotations

import json
from pathlib import Path

from backend.app.services.v5_metric_extractor import _apply_common_block_execution_timing
from backend.app.services import v5_optme_txallo_fidelity_closure as v16


def _write_json(path: Path, value) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def test_txallo_v2298_nanosecond_timing_survives_zero_millisecond_field(tmp_path: Path) -> None:
    _write_json(tmp_path / "compiled_run_plan.json", {"node_configs": [{"node_id": "n0", "leader": True}]})
    _write_json(tmp_path / "nodes/n0/block_execution_summary.json", {
        "shard_id": "s0",
        "block_executor_id": "serial_block_executor",
        "blocks": [{
            "block_execution_ms": 0,
            "transaction_execution_ms": 0,
            "transaction_execution_us": 0,
            "transaction_execution_ns": 750,
            "deterministic_materialization_ms": 0,
            "state_commitment_ms": 0,
        }],
    })
    metrics = {"source_artifacts": []}
    _apply_common_block_execution_timing(metrics, tmp_path)
    assert metrics["transaction_execution_ms"] == 0.00075
    assert metrics["business_execution_cpu_sum_ms"] == 0.00075
    assert metrics["execution_phase_timing_precision"] == "nanosecond_accumulated_then_reported_ms"

    enriched = v16.enrich_metrics(tmp_path, "stateless_txallo", {
        "method_id": "stateless_txallo",
        "finalized_tx_count": 1,
        "transaction_execution_ms": metrics["transaction_execution_ms"],
    })
    assert enriched["v16_business_execution_timer_complete"] is True
    assert enriched["v16_business_execution_timer_blocker"] is None
