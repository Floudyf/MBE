from __future__ import annotations

import json
from pathlib import Path

from backend.app.services import v5_metric_extractor
from backend.app.services.v5_plugin_manifest_store import STORE


def test_metatrack_routing_exposes_optional_micro_batch_without_picking_optimum() -> None:
    manifest = STORE.get("metatrack_coaccess_routing")
    # Preserve the existing paper-default manifest contract exactly.
    # micro_batch_size is optional: absence from default_config means runtime
    # inheritance (0 -> block_size), while the schema documents the optional knob.
    assert manifest.default_config == {"routing_epoch": 0}
    properties = manifest.config_schema.get("properties", {})
    assert properties["micro_batch_size"]["default"] == 0
    assert properties["micro_batch_size"]["maximum"] == 5000


def test_common_block_execution_timing_exports_effective_worker_truth(tmp_path: Path) -> None:
    run_dir = tmp_path
    (run_dir / "nodes" / "n0").mkdir(parents=True)
    (run_dir / "compiled_run_plan.json").write_text(
        json.dumps({"node_configs": [{"node_id": "n0", "shard_id": "s0", "leader": True, "role": "leader"}]}),
        encoding="utf-8",
    )
    (run_dir / "nodes" / "n0" / "block_execution_summary.json").write_text(
        json.dumps({
            "node_id": "n0",
            "shard_id": "s0",
            "block_executor_id": "metatrack_block_executor",
            "blocks": [{
                "block_execution_ms": 10,
                "transaction_execution_ms": 7,
                "deterministic_materialization_ms": 2,
                "state_commitment_ms": 1,
                "worker_count": 8,
            }],
        }),
        encoding="utf-8",
    )
    metrics = {"source_artifacts": []}
    v5_metric_extractor._apply_common_block_execution_timing(metrics, run_dir)
    assert metrics["configured_worker_count"] == 8
    assert metrics["worker_count"] == 8
