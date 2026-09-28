from __future__ import annotations
import json
from pathlib import Path
from backend.app.services.v5_metric_extractor import _apply_metatrack_projection_frontier_v612_metrics


def _write(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def test_projection_frontier_metrics_use_leaders_and_do_not_replica_multiply(tmp_path: Path) -> None:
    _write(tmp_path / "compiled_run_plan.json", {"node_configs": [
        {"node_id":"n0","leader":True}, {"node_id":"n1","leader":False}, {"node_id":"n4","leader":True}
    ]})
    _write(tmp_path / "nodes/n0/node_summary.json", {
        "blocks":[{"metatrack_projection_frontier_policy":"signed_projection_exact_version_dag_v612","metatrack_projection_frontier_projection_count":3,"metatrack_projection_frontier_layer_count":2,"metatrack_projection_frontier_max_layer_projection_width":2,"metatrack_projection_frontier_cross_projection_edge_count":1}],
        "runtime_metric_counts":{"metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count":4},
    })
    _write(tmp_path / "nodes/n1/node_summary.json", {
        "blocks":[{"metatrack_projection_frontier_policy":"signed_projection_exact_version_dag_v612","metatrack_projection_frontier_projection_count":999}],
        "runtime_metric_counts":{"metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count":999},
    })
    _write(tmp_path / "nodes/n4/node_summary.json", {
        "blocks":[{"metatrack_projection_frontier_policy":"signed_projection_exact_version_dag_v612","metatrack_projection_frontier_projection_count":2,"metatrack_projection_frontier_layer_count":1,"metatrack_projection_frontier_max_layer_projection_width":2,"metatrack_projection_frontier_cross_projection_edge_count":0}],
        "runtime_metric_counts":{"metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count":3},
    })
    metrics={"source_artifacts":[]}
    _apply_metatrack_projection_frontier_v612_metrics(metrics,tmp_path)
    assert metrics["metatrack_projection_frontier_projection_count"] == 5
    assert metrics["metatrack_projection_frontier_layer_count"] == 3
    assert metrics["metatrack_projection_frontier_max_layer_projection_width"] == 2
    assert metrics["metatrack_projection_frontier_cross_projection_edge_count"] == 1
    assert metrics["metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count"] == 7
    assert metrics["metatrack_projection_frontier_policy"] == "signed_projection_exact_version_dag_v612"
    assert set(metrics["source_artifacts"]) == {"nodes/n0/node_summary.json","nodes/n4/node_summary.json"}


def test_projection_frontier_source_contract_uses_signed_liveness_and_no_runtime_reinference() -> None:
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    helper = (root / "executor/v5/metatrack_projection_frontier_v612.go").read_text(encoding="utf-8")
    frontend = (root / "frontend/src/components/v5/V5MechanismAnalysis.tsx").read_text(encoding="utf-8")
    assert 'r.metaTrackBlockExecutorFlag("dependency_closed_consensus")' in runtime
    assert 'dependency.LivenessClass == metaTrackVersionClassDeadIntermediate' in runtime
    assert 'metatrack_projection_frontier_suppressed_remote_dead_intermediate_publish_count' in runtime
    assert 'transactionRequiresExactStateValue(item, dep.Key)' in helper
    assert 'threshold' in helper.lower()
    assert 'METATRACK_PROJECTION_FRONTIER_V612' in frontend
