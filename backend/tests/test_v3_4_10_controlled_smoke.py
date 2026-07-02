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
    assert result["current_stage"] == "V3.6.2 V3.6 Closure"
    assert result["latest_runtime_stage"] == "V3.4.10"
    assert result["latest_completed_runtime_stage"] == "configurable NetworkAdapter with consensus-light over typed message runtime"
    assert result["closure_stage"] == "V3.4.11"
    assert result["current_capability"] == "configurable NetworkAdapter with consensus-light proposal/vote preview over typed messages"
    assert result["runtime_truth"] == "network_adapter_consensus_light_preview_not_real_pbft"
    assert result["next_stage"] == "V3.7 ConsensusRuntime and BlockEmulator-aligned PBFT Preview"
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
    }

    with (run_dir / "run_index.csv").open(encoding="utf-8", newline="") as stream:
        run_rows = list(csv.DictReader(stream))
    assert [row["preset_id"] for row in run_rows] == CONTROLLED_PRESET_ORDER
    assert all(row["run_status"] == "completed" for row in run_rows)

    with (run_dir / "aggregate_summary.csv").open(encoding="utf-8", newline="") as stream:
        aggregate_rows = list(csv.DictReader(stream))
    assert "cross_shard_ratio" in aggregate_rows[0]
    assert "avg_commit_latency_ms" in aggregate_rows[0]

    readiness = json.loads((run_dir / "realism_readiness.json").read_text(encoding="utf-8"))
    assert readiness["current_stage"] == "V3.6.2 V3.6 Closure"
    assert readiness["latest_runtime_stage"] == "V3.4.10"
    assert readiness["latest_completed_runtime_stage"] == "configurable NetworkAdapter with consensus-light over typed message runtime"
    assert len(readiness["modules"]) == 11
    assert "not BlockEmulator backend" in readiness["not_real_chain_claims"]
    assert "not Fabric/EVM live backend" in readiness["not_real_chain_claims"]

    artifact_path = get_controlled_artifact_path(result["run_id"], "aggregate_summary.csv", root=tmp_path)
    assert artifact_path == run_dir / "aggregate_summary.csv"
    launcher_path = get_controlled_artifact_path(result["run_id"], "node_address_table.csv", root=tmp_path)
    assert launcher_path == run_dir / "node_address_table.csv"
