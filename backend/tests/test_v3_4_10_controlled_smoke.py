from __future__ import annotations

import csv
import json

from backend.app.services.v3_controlled_smoke_runner import (
    CONTROLLED_PRESET_ORDER,
    get_controlled_artifact_path,
    run_v3_4_10_controlled_smoke,
)


def test_controlled_smoke_runs_all_metatrack_presets(tmp_path) -> None:
    result = run_v3_4_10_controlled_smoke(root=tmp_path)
    run_dir = tmp_path / result["run_id"]

    assert result["status"] == "completed"
    assert result["stage"] == "V3.4.10"
    assert result["current_stage"] == "V3.11 CrossShard Protocol Closure"
    assert result["latest_runtime_stage"] == "V3.4.10"
    assert result["latest_completed_runtime_stage"] == "cross-shard Relay MVP with state machine, source lock, relay certificate, target verification, target commit, source finalization, timeout/refund/abort paths"
    assert result["closure_stage"] == "V3.4.11"
    assert result["current_capability"] == "runnable relay_mvp cross-shard protocol MVP with artifacts and frontend result summary"
    assert result["runtime_truth"] == "relay_mvp_not_production_atomic_commit"
    assert result["next_stage"] == "V3.12 Runtime Realism Closure"
    assert result["preset_order"] == CONTROLLED_PRESET_ORDER
    assert [row["preset_id"] for row in result["run_index"]] == CONTROLLED_PRESET_ORDER
    assert [row["preset_id"] for row in result["aggregate_summary"]] == CONTROLLED_PRESET_ORDER
    assert {artifact["name"] for artifact in result["artifacts"]} == {
        "run_index.csv",
        "aggregate_summary.csv",
        "ablation_report.md",
        "realism_readiness.json",
        "realism_readiness.md",
        "node_address_table.csv",
        "topology.json",
        "launch_nodes_windows.bat",
        "launch_nodes_linux.sh",
        "launcher_readme.md",
        "node_process_status.csv",
        "node_process_manifest.json",
        "node_process_log_sample.log",
        "tcp_adapter_status.csv",
        "network_send_log.csv",
        "network_receive_log.csv",
        "typed_message_log.csv",
        "consensus_network_light_log.csv",
        "network_consensus_summary.json",
        "pbft_state_log.csv",
        "pbft_message_log.csv",
        "quorum_log.csv",
        "finalized_block_log.csv",
        "consensus_network_log.csv",
        "pbft_network_summary.json",
        "cross_shard_tx_log.csv",
        "cross_shard_message_log.csv",
        "relay_preview_log.csv",
        "cross_shard_status.csv",
        "cross_shard_summary.json",
        "relay_state_machine_log.csv",
        "source_lock_log.csv",
        "relay_certificate_log.csv",
        "relay_proof_verification_log.csv",
        "target_verification_log.csv",
        "target_commit_log.csv",
        "source_finalize_log.csv",
        "cross_shard_timeout_refund_log.csv",
        "cross_shard_failure_log.csv",
        "relay_mvp_summary.json",
        "state_storage_log.csv",
        "state_version_log.csv",
        "state_root_log.csv",
        "state_proof_log.csv",
        "state_proof_verification_log.csv",
        "witness_log.csv",
        "witness_verification_log.csv",
        "state_authenticity_summary.json",
        "benchmark_template_catalog.json",
        "baseline_profile_catalog.json",
        "benchmark_plan.json",
        "benchmark_run_index.csv",
        "sweep_matrix.csv",
        "sweep_summary.csv",
        "sweep_summary.json",
        "baseline_comparison.csv",
        "reproducibility_manifest.json",
        "benchmark_report.md",
        "benchmark_summary.json",
    }

    with (run_dir / "run_index.csv").open(encoding="utf-8", newline="") as stream:
        run_rows = list(csv.DictReader(stream))
    assert [row["preset_id"] for row in run_rows] == CONTROLLED_PRESET_ORDER
    assert all(row["run_status"] == "completed" for row in run_rows)

    with (run_dir / "aggregate_summary.csv").open(encoding="utf-8", newline="") as stream:
        aggregate_rows = list(csv.DictReader(stream))
    assert "cross_shard_ratio" in aggregate_rows[0]
    assert "avg_commit_latency_ms" in aggregate_rows[0]
    assert "state_root_count" in aggregate_rows[0]
    assert "benchmark_template_selected" in aggregate_rows[0]
    assert "paper_grade_benchmark" in aggregate_rows[0]

    readiness = json.loads((run_dir / "realism_readiness.json").read_text(encoding="utf-8"))
    assert readiness["current_stage"] == "V3.11 CrossShard Protocol Closure"
    assert readiness["latest_runtime_stage"] == "V3.4.10"
    assert readiness["latest_completed_runtime_stage"] == "cross-shard Relay MVP with state machine, source lock, relay certificate, target verification, target commit, source finalization, timeout/refund/abort paths"
    assert len(readiness["modules"]) == 12
    assert "not BlockEmulator backend" in readiness["not_real_chain_claims"]
    assert "not Fabric/EVM live backend" in readiness["not_real_chain_claims"]
    assert "not paper-grade benchmark evidence" in readiness["not_real_chain_claims"]

    artifact_path = get_controlled_artifact_path(result["run_id"], "aggregate_summary.csv", root=tmp_path)
    assert artifact_path == run_dir / "aggregate_summary.csv"
    launcher_path = get_controlled_artifact_path(result["run_id"], "node_address_table.csv", root=tmp_path)
    assert launcher_path == run_dir / "node_address_table.csv"
