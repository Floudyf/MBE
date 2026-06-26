import csv
import json

from backend.app.services.v3_go_runtime_runner import run_go_v3_runtime, run_metatrack_go_backed_ablation
from backend.app.services.v3_runtime.runtime import run_v3_single_chain_runtime


def test_go_v3_runtime_parity_artifacts_and_counts(tmp_path):
    result = run_go_v3_runtime(output_dir=tmp_path / "go_v3")
    python_result = run_v3_single_chain_runtime("single_chain_runtime_smoke", tmp_path / "python_v3")

    assert result.summary["truth_label"] == "modular_runtime"
    assert result.summary["runtime_mode"] in {"go_backed_minimal_runtime", "go_backed"}
    assert result.summary["tx_count"] == 24
    assert result.summary["success_count"] == 24
    assert result.summary["tx_count"] == python_result.summary.tx_count
    assert result.summary["success_count"] == python_result.summary.success_count
    assert result.summary["failure_count"] == python_result.summary.failure_count
    assert result.summary["block_count"] == python_result.summary.block_count
    for name in (
        "used_chain_profile.yaml",
        "used_plugin_profile.yaml",
        "used_experiment_profile.yaml",
        "runtime.log",
        "summary.csv",
        "summary.json",
        "report.md",
        "block_log.csv",
        "tx_results.csv",
        "state_commit_log.csv",
    ):
        assert (result.output_dir / name).exists()


def test_metatrack_go_backed_ablation_outputs_and_guards(tmp_path):
    result = run_metatrack_go_backed_ablation(tmp_path)
    output_dir = result["output_dir"]

    for name in (
        "metatrack_summary.csv",
        "metatrack_summary.json",
        "metatrack_latency.csv",
        "metatrack_mechanism_metrics.csv",
        "metatrack_ablation_report.md",
    ):
        assert (output_dir / name).exists()

    rows = _csv_rows(output_dir / "metatrack_mechanism_metrics.csv")
    combos = {row["plugin_combination"] for row in rows}
    assert combos == {"baseline_hash_only", "co_access_only", "co_access_dual_track", "full_MetaTrack"}
    required = {
        "throughput_tps",
        "avg_latency_ms",
        "p95_latency_ms",
        "p99_latency_ms",
        "remote_fetch_count",
        "cross_shard_ratio",
        "fast_track_count",
        "conservative_track_count",
        "aggregated_update_count",
        "aggregation_ratio",
        "conflict_count",
        "queue_wait_ms",
        "block_commit_latency_ms",
    }
    assert required <= set(rows[0])
    dual = next(row for row in rows if row["plugin_combination"] == "co_access_dual_track")
    full = next(row for row in rows if row["plugin_combination"] == "full_MetaTrack")
    assert int(dual["fast_track_count"]) > 0
    assert int(dual["conservative_track_count"]) > 0
    assert int(full["aggregated_update_count"]) > 0

    summary = json.loads((output_dir / "metatrack_summary.json").read_text(encoding="utf-8"))
    fairness_keys = {(row["tx_count"], row["chain_profile_id"], row["stage"], row["backend_type"]) for row in summary}
    assert len(fairness_keys) == 1

    produced = {path.name for path in output_dir.iterdir()}
    assert "fabric_validation_summary.csv" not in produced
    assert "metaflow_events.csv" not in produced
    assert "control_decisions.csv" not in produced


def test_metatrack_full_and_baseline_final_state_equivalence(tmp_path):
    result = run_metatrack_go_backed_ablation(tmp_path)
    output_dir = result["output_dir"]

    baseline_state = _final_state(output_dir / "baseline_hash_only" / "state_commit_log.csv")
    full_state = _final_state(output_dir / "full_MetaTrack" / "state_commit_log.csv")

    assert baseline_state == full_state


def _csv_rows(path):
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _final_state(path):
    state = {}
    for row in _csv_rows(path):
        state[row["state_key"]] = int(row["new_value"])
    return state
