from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest
from backend.app.services import v3_composer_draft_runner as draft_runner


def valid_draft() -> V3ComposerDraftRequest:
    plugins = {
        "Workload": ("fixed", "synthetic_hotspot"),
        "TxPool": ("fixed", "fifo_pool"),
        "BlockProducer": ("fixed", "time_or_count_block_producer"),
        "Consensus": ("fixed", "simple_leader"),
        "CommitteeEpoch": ("disabled", "disabled"),
        "Routing": ("variable", "co_access_sharding"),
        "Execution": ("variable", "dual_track_execution"),
        "StateAccess": ("variable", "access_list_prefetch"),
        "StateStorage": ("fixed", "hash_state_storage"),
        "Commit": ("variable", "hot_update_aggregation_commit"),
        "MetricsReport": ("output", "basic_metrics"),
    }
    return V3ComposerDraftRequest(
        template_id="metatrack_ablation",
        modules={
            module_id: V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin)
            for module_id, (status, plugin) in plugins.items()
        },
    )


def test_run_draft_smoke_writes_single_draft_artifacts(monkeypatch, tmp_path: Path) -> None:
    calls: list[dict] = []

    def fake_run_go_v3_runtime(**kwargs):
        output_dir = kwargs["output_dir"]
        calls.append(kwargs)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "summary.csv").write_text("tx_count,success_count\n1,1\n", encoding="utf-8")
        (output_dir / "latency.csv").write_text("tx_id,latency_ms\n0,1\n", encoding="utf-8")
        (output_dir / "runtime.log").write_text("draft smoke complete\n", encoding="utf-8")
        for name in ("node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv", "node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md", "node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log", "tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv", "consensus_network_light_log.csv", "network_consensus_summary.json", "pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv", "consensus_network_log.csv", "pbft_network_summary.json", "cross_shard_tx_log.csv", "cross_shard_message_log.csv", "relay_preview_log.csv", "cross_shard_status.csv", "cross_shard_summary.json", "relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json", "state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json", "benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"):
            (output_dir / name).write_text("id\n1\n", encoding="utf-8")
        summary = {
            "tx_count": 1,
            "success_count": 1,
            "shard_count": 4,
            "validators_per_shard": 4,
            "logical_node_count": 25,
            "validator_node_count": 16,
            "executor_node_count": 4,
            "storage_node_count": 4,
            "supervisor_node_count": 1,
            "message_count": 8,
            "network_message_count": 4,
            "consensus_message_count": 4,
            "node_event_count": 29,
            "launcher_mode": "local_multi_process_launcher_preview",
            "launcher_script_count": 2,
            "launchable_node_count": 25,
            "node_address_count": 25,
            "windows_launcher_available": True,
            "linux_launcher_available": True,
            "launcher_preview_only": True,
            "node_process_entrypoint_available": True,
            "node_process_preview_available": True,
            "node_process_status_available": True,
            "node_process_manifest_available": True,
            "node_process_preview_only": True,
            "network_adapter_selected": "in_memory_message_bus",
            "tcp_preview_enabled": False,
            "tcp_listen_node_count": 0,
            "tcp_send_count": 1,
            "tcp_receive_count": 1,
            "typed_message_count": 1,
            "network_error_count": 0,
            "consensus_over_network_enabled": True,
            "consensus_runtime_selected": "simple_leader",
            "proposal_preview_count": 4,
            "vote_preview_count": 4,
            "light_quorum_reached_count": 1,
            "consensus_network_error_count": 0,
            "consensus_network_path": "in_memory_typed_message_bus",
            "pbft_view": 0,
            "pbft_sequence": 1,
            "pbft_preprepare_count": 0,
            "pbft_prepare_count": 0,
            "pbft_commit_count": 0,
            "pbft_quorum_reached_count": 0,
            "pbft_finalized_block_count": 0,
            "pbft_consensus_latency_ms": 0,
            "pbft_preview_enabled": False,
            "pbft_quorum_threshold": 0,
            "pbft_over_network_enabled": False,
            "pbft_network_path": "in_memory",
            "pbft_network_message_count": 0,
            "pbft_network_error_count": 0,
            "pbft_preprepare_network_count": 0,
            "pbft_prepare_network_count": 0,
            "pbft_commit_network_count": 0,
            "pbft_finalized_network_count": 0,
            "pbft_network_quorum_reached_count": 0,
            "cross_shard_protocol_selected": "none",
            "cross_shard_message_count": 0,
            "relay_preview_count": 0,
            "cross_shard_completed_count": 0,
            "cross_shard_failed_count": 0,
            "cross_shard_avg_latency_ms": 0,
            "relay_mvp_enabled": False,
            "relay_mvp_tx_count": 0,
            "relay_source_lock_count": 0,
            "relay_certificate_count": 0,
            "relay_proof_verified_count": 0,
            "relay_proof_failed_count": 0,
            "relay_target_verified_count": 0,
            "relay_target_commit_count": 0,
            "relay_source_finalized_count": 0,
            "relay_timeout_count": 0,
            "relay_refund_count": 0,
            "relay_abort_count": 0,
            "relay_success_count": 0,
            "relay_failed_count": 0,
            "relay_avg_latency_ms": 0,
            "relay_mvp_truth": "relay_mvp_not_production_atomic_commit",
            "state_backend_selected": "memory_kv",
            "persistent_state_enabled": False,
            "state_root_enabled": True,
            "state_root_count": 1,
            "state_key_count": 1,
            "state_update_count": 1,
            "state_proof_generated_count": 1,
            "state_proof_verified_count": 1,
            "state_proof_failed_count": 0,
            "witness_generated_count": 1,
            "witness_verified_count": 1,
            "witness_failed_count": 0,
            "state_authenticity_error_count": 0,
            "benchmark_template_selected": "full_stack_v3_template",
            "baseline_profile_selected": "baseline_simple_chain",
            "benchmark_run_count": 1,
            "sweep_parameter_count": 7,
            "repeat_count": 1,
            "benchmark_artifact_count": 12,
            "baseline_comparison_count": 7,
            "reproducibility_manifest_available": True,
            "benchmark_report_available": True,
            "paper_grade_benchmark": False,
        }
        (output_dir / "summary.json").write_text('{"tx_count": 1, "success_count": 1, "logical_node_count": 25}\n', encoding="utf-8")
        return SimpleNamespace(output_dir=output_dir, summary=summary)

    monkeypatch.setattr(draft_runner, "run_go_v3_runtime", fake_run_go_v3_runtime)

    result = draft_runner.run_v3_composer_draft_smoke(valid_draft(), root=tmp_path)

    assert result["status"] == "completed"
    assert result["stage"] == "V3.11 CrossShard Protocol Closure"
    assert result["current_stage"] == "V3.11 CrossShard Protocol Closure"
    assert result["latest_runtime_stage"] == "cross-shard Relay MVP with state machine, source lock, relay certificate, target verification, target commit, source finalization, timeout/refund/abort paths"
    assert result["latest_completed_runtime_stage"] == "cross-shard Relay MVP with state machine, source lock, relay certificate, target verification, target commit, source finalization, timeout/refund/abort paths"
    assert result["current_capability"] == "runnable relay_mvp cross-shard protocol MVP with artifacts and frontend result summary"
    assert result["runtime_truth"] == "relay_mvp_not_production_atomic_commit"
    assert result["run_mode"] == "draft_smoke"
    assert result["topology_summary"]["logical_node_count"] == 25
    assert len(calls) == 1
    assert calls[0]["plugin_profile_id"] == draft_runner.DRAFT_PLUGIN_PROFILE_ID
    run_dir = Path(result["output_dir"])
    for name in (
        "composer_draft.json",
        "normalized_draft.json",
        "draft_validation.json",
        "generated_experiment_profile.json",
        "generated_experiment_profile.yaml",
        "generated_plugin_profile.yaml",
        "summary.csv",
        "latency.csv",
        "runtime.log",
        "node_topology.csv",
        "node_log.csv",
        "network_log.csv",
        "consensus_message_log.csv",
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
        "aggregate_summary.csv",
        "baseline_comparison.csv",
        "reproducibility_manifest.json",
        "benchmark_report.md",
        "benchmark_summary.json",
        "quorum_log.csv",
        "finalized_block_log.csv",
    ):
        assert (run_dir / name).is_file()
    artifact_names = {artifact["name"] for artifact in result["artifacts"]}
    assert {"composer_draft.json", "normalized_draft.json", "generated_experiment_profile.json", "summary.csv", "runtime.log", "node_topology.csv", "node_log.csv", "network_log.csv", "consensus_message_log.csv", "node_address_table.csv", "topology.json", "launch_nodes_windows.bat", "launch_nodes_linux.sh", "launcher_readme.md", "node_process_status.csv", "node_process_manifest.json", "node_process_log_sample.log", "tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv", "consensus_network_light_log.csv", "network_consensus_summary.json", "pbft_state_log.csv", "pbft_message_log.csv", "quorum_log.csv", "finalized_block_log.csv", "consensus_network_log.csv", "pbft_network_summary.json", "cross_shard_tx_log.csv", "cross_shard_message_log.csv", "relay_preview_log.csv", "cross_shard_status.csv", "cross_shard_summary.json", "relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json", "state_storage_log.csv", "state_version_log.csv", "state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json", "benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "reproducibility_manifest.json", "benchmark_report.md", "benchmark_summary.json"} <= artifact_names
    generated = (run_dir / "generated_experiment_profile.json").read_text(encoding="utf-8")
    assert '"cross_shard_protocol": "none"' in generated
    assert '"state_backend": "memory_kv"' in generated
    assert '"benchmark_template": "full_stack_v3_template"' in generated
    assert '"baseline_profile": "baseline_simple_chain"' in generated


def test_run_draft_smoke_invalid_draft_does_not_start_runner(monkeypatch, tmp_path: Path) -> None:
    started = False

    def fake_run_go_v3_runtime(**_kwargs):
        nonlocal started
        started = True
        raise AssertionError("runner should not start")

    monkeypatch.setattr(draft_runner, "run_go_v3_runtime", fake_run_go_v3_runtime)
    invalid = valid_draft()
    invalid.modules["Consensus"] = V3ComposerDraftModule(module_id="Consensus", status="disabled", plugin="simple_leader")

    with pytest.raises(draft_runner.DraftSmokeNotRunnable):
        draft_runner.run_v3_composer_draft_smoke(invalid, root=tmp_path)

    assert started is False


def test_build_plugin_profile_maps_pbft_preview_to_consensus_runtime() -> None:
    validation = draft_runner.validate_v3_composer_draft(valid_draft())
    assert validation.normalized_draft is not None
    normalized = dict(validation.normalized_draft)
    normalized["plugin_selection"] = dict(normalized["plugin_selection"]) | {"Consensus": "blockemulator_aligned_pbft_preview"}

    profile = draft_runner.build_plugin_profile(normalized)
    plugins = profile["profiles"][0]["plugins"]

    assert plugins["ConsensusPlugin"] == "pbft_light_model"
    assert plugins["ConsensusRuntimePlugin"] == "blockemulator_aligned_pbft_preview"
