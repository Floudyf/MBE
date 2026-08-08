from __future__ import annotations

import json
import zipfile
from pathlib import Path

from backend.app.services import v5_reproducibility_bundle
from backend.app.services.v5_fairness_validator import validate as validate_fairness
from backend.app.services.v5_paper_exporter import paper_result_analysis
from backend.app.services.v5_real_cluster_runner import (
    _completion_gate,
    _global_business_state_digest,
    _global_final_state_digest,
    _metatrack_control_plane_evidence,
    _initial_state_digest,
    _root_failure,
    _state_home_mapping_digest,
)
from backend.app.services.v5_formal_scheduler import _apply_state_equivalence_gate


def _fairness_row(method: str, semantic_class: str) -> dict:
    return {
        "child_run_id": f"child-{method}",
        "method_config_id": method,
        "comparison_group_id": "comparison:11:0:base:",
        "comparison_semantics_class": semantic_class,
        "seed": 11,
        "repeat_index": 0,
        "execution_backend": "real_cluster",
        "estimated_transactions": 10_000,
        "workload_snapshot_digest": "workload",
        "topology_snapshot_digest": "topology",
        "fault_snapshot_digest": "fault",
        "fairness_key": "fairness",
        "block_size": 2000,
        "block_interval_ms": 100,
        "runnable": True,
        "blockers": [],
    }


def test_fairness_keeps_runs_runnable_but_prohibits_cross_semantics_uplift() -> None:
    rows = [
        _fairness_row("hash_serial", "stateful_local_legacy_v1"),
        _fairness_row("metatrack_serial", "stateless_remote_home_v1"),
    ]
    checked, result = validate_fairness(rows)
    assert result["passed"] is True
    assert result["performance_comparison_valid"] is False
    assert result["incomparable_groups"][0]["semantic_classes"] == [
        "stateful_local_legacy_v1",
        "stateless_remote_home_v1",
    ]
    assert all(row["runnable"] is True for row in checked)
    assert all(row["performance_comparison_valid"] is False for row in checked)


def test_fairness_rejects_semantic_policy_mismatch_within_same_class() -> None:
    left = _fairness_row("stateless_hash_serial", "stateless_remote_home_v1")
    right = _fairness_row("metatrack_serial", "stateless_remote_home_v1")
    left.update(
        {
            "state_access_semantics": "stateless_remote_home",
            "state_home_mapping_policy": "deterministic_state_key_sharding",
            "remote_fetch_policy": "home_leader_witness_fetch",
            "remote_writeback_policy": "home_shard_consensus_delta",
            "proof_policy": "state_root_witness_digest",
            "legacy_cross_shard_protocol": False,
            "measurement_boundary": "client_submit_to_stateless_direct_terminal",
        }
    )
    right.update(left)
    right["method_config_id"] = "metatrack_serial"
    right["child_run_id"] = "child-metatrack"
    right["remote_fetch_policy"] = "different_fetch_policy"

    checked, result = validate_fairness([left, right])

    assert result["passed"] is False
    assert checked[1]["runnable"] is False
    assert "semantic fairness mismatch: remote_fetch_policy" in checked[1]["blockers"]


def test_fairness_allows_stateless_hash_vs_metatrack_direct_comparison() -> None:
    rows = [
        _fairness_row("stateless_hash_serial", "stateless_remote_home_v1"),
        _fairness_row("metatrack_serial", "stateless_remote_home_v1"),
    ]
    checked, result = validate_fairness(rows)
    assert result["passed"] is True
    assert result["performance_comparison_valid"] is True
    assert all(row["performance_comparison_valid"] is True for row in checked)


def test_paper_analysis_marks_mixed_execution_semantics_incomparable() -> None:
    group = {
        "run_group_id": "v5grp_incomparable",
        "fairness_validation": {"passed": True, "performance_comparison_valid": False},
    }
    result = paper_result_analysis(group, [])
    assert result["analysis_status"] == "incomparable"
    assert result["performance_comparison_valid"] is False
    assert "prohibited" in result["comparison_note"]


def test_paper_analysis_honors_state_equivalence_gate() -> None:
    group = {
        "run_group_id": "v5grp_state_mismatch",
        "fairness_validation": {
            "passed": True,
            "performance_comparison_valid": True,
        },
        "state_equivalence_validation": {
            "passed": False,
            "performance_comparison_valid": False,
        },
        "performance_comparison_valid": False,
    }
    result = paper_result_analysis(group, [])
    assert result["analysis_status"] == "incomparable"
    assert result["performance_comparison_valid"] is False
    assert "logical-state equivalence" in result["comparison_note"]


def test_root_failure_prefers_explicit_executor_failure(tmp_path: Path) -> None:
    (tmp_path / "stalled_runtime_report.json").write_text(
        json.dumps({"reason": "failed_deterministic_execution: block-stm scheduler stalled"}),
        encoding="utf-8",
    )
    assert _root_failure(tmp_path, "drain no-progress timeout") == (
        "failed_deterministic_execution: block-stm scheduler stalled"
    )


def test_root_failure_is_empty_without_failure_evidence(tmp_path: Path) -> None:
    assert _root_failure(tmp_path, "") == ""


def test_failed_runtime_diagnostics_are_included_in_bundle(tmp_path: Path, monkeypatch) -> None:
    group_dir = tmp_path / "group"
    group_dir.mkdir()
    (group_dir / "run_group.json").write_text("{}", encoding="utf-8")
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "stalled_runtime_report.json").write_text(
        '{"reason":"deterministic failure"}', encoding="utf-8"
    )
    (runtime_dir / "node_runtime_status.json").write_text(
        '{"fatal_execution_error":"deterministic failure"}', encoding="utf-8"
    )
    (runtime_dir / "ignored.bin").write_bytes(b"ignored")

    monkeypatch.setattr(
        v5_reproducibility_bundle,
        "children",
        lambda _group_id: [
            {
                "child_run_id": "v5child_failed",
                "status": "failed",
                "result": {"run_id": "v5_failed"},
            }
        ],
    )
    monkeypatch.setattr(
        v5_reproducibility_bundle.v5_real_cluster_runner,
        "run_dir",
        lambda _run_id: runtime_dir,
    )

    bundle = v5_reproducibility_bundle.build(
        group_dir, {"run_group_id": "v5grp_failed"}
    )
    with zipfile.ZipFile(bundle) as archive:
        names = set(archive.namelist())
    assert "runtime_diagnostics/v5child_failed/stalled_runtime_report.json" in names
    assert "runtime_diagnostics/v5child_failed/node_runtime_status.json" in names
    assert not any(name.endswith("ignored.bin") for name in names)


def _write_chain(path: Path, node_id: str, shard_id: str, before: str, after: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "node_id,shard_id,height,state_root_before,state_root_after\n"
        f"{node_id},{shard_id},1,{before},{after}\n",
        encoding="utf-8",
    )


def test_runtime_state_digests_are_replica_deduplicated_and_deterministic(
    tmp_path: Path,
) -> None:
    _write_chain(tmp_path / "nodes/n0/committed_chain.csv", "n0", "s0", "g0", "f0")
    _write_chain(tmp_path / "nodes/n1/committed_chain.csv", "n1", "s0", "g0", "f0")
    _write_chain(tmp_path / "nodes/n4/committed_chain.csv", "n4", "s1", "g1", "f1")
    _write_chain(tmp_path / "nodes/n5/committed_chain.csv", "n5", "s1", "g1", "f1")
    placement = tmp_path / "client/placement_plan.csv"
    placement.parent.mkdir(parents=True)
    placement.write_text(
        "batch_index,state_key,home_shard,execution_shard\n"
        "0,key-b,s1,s0\n"
        "0,key-a,s0,s1\n"
        "1,key-a,s0,s0\n",
        encoding="utf-8",
    )
    summary = {
        "node_summaries": [
            {"node_id": "n5", "shard_id": "s1", "state_root": "f1"},
            {"node_id": "n0", "shard_id": "s0", "state_root": "f0"},
            {"node_id": "n4", "shard_id": "s1", "state_root": "f1"},
            {"node_id": "n1", "shard_id": "s0", "state_root": "f0"},
        ]
    }
    assert _initial_state_digest(tmp_path)
    assert _state_home_mapping_digest(tmp_path)
    assert _global_final_state_digest(summary)
    assert _state_home_mapping_digest(tmp_path) == _state_home_mapping_digest(tmp_path)


def test_runtime_state_digests_reject_replica_divergence(tmp_path: Path) -> None:
    _write_chain(tmp_path / "nodes/n0/committed_chain.csv", "n0", "s0", "g0", "f0")
    _write_chain(tmp_path / "nodes/n1/committed_chain.csv", "n1", "s0", "other", "f0")
    assert _initial_state_digest(tmp_path) == ""
    assert (
        _global_final_state_digest(
            {
                "node_summaries": [
                    {"shard_id": "s0", "state_root": "f0"},
                    {"shard_id": "s0", "state_root": "other"},
                ]
            }
        )
        == ""
    )


def _completed_child(
    child_id: str,
    method_id: str,
    semantic_class: str,
    *,
    initial: str = "initial",
    mapping: str = "mapping",
    final: str = "final",
) -> dict:
    return {
        **_fairness_row(method_id, semantic_class),
        "child_run_id": child_id,
        "status": "completed",
        "paper_candidate": True,
        "metrics": {
            "submitted_unique_tx_count": 100,
            "terminal_unique_tx_count": 100,
            "incomplete_unique_tx_count": 0,
            "finalized_unique_logical_tx_count": 100,
            "cross_shard_failed_unique_count": 0,
            "lifecycle_complete": True,
        },
        "result": {
            "summary": {
                "initial_state_digest": initial,
                "state_home_mapping_digest": mapping,
                "global_final_state_digest": final,
                "finality_evidence": {
                    "submitted_unique_tx_count": 100,
                    "terminal_unique_tx_count": 100,
                    "incomplete_unique_tx_count": 0,
                    "finalized_unique_logical_tx_count": 100,
                    "cross_shard_failed_unique_count": 0,
                },
            }
        },
    }


def test_state_equivalence_gate_passes_fair_stateless_pair() -> None:
    items, result = _apply_state_equivalence_gate(
        [
            _completed_child(
                "hash", "stateless_hash_serial", "stateless_remote_home_v1"
            ),
            _completed_child(
                "metatrack", "metatrack_serial", "stateless_remote_home_v1"
            ),
        ]
    )
    assert result["passed"] is True
    assert result["performance_comparison_valid"] is True
    assert result["pairwise_logical_state_equivalent"] is True
    assert all(item["pairwise_logical_state_equivalent"] is True for item in items)
    assert all(item["paper_candidate"] is True for item in items)


def test_state_equivalence_gate_rejects_final_state_mismatch() -> None:
    items, result = _apply_state_equivalence_gate(
        [
            _completed_child(
                "hash",
                "stateless_hash_serial",
                "stateless_remote_home_v1",
                final="hash-final",
            ),
            _completed_child(
                "metatrack",
                "metatrack_serial",
                "stateless_remote_home_v1",
                final="metatrack-final",
            ),
        ]
    )
    assert result["passed"] is False
    assert result["performance_comparison_valid"] is False
    reference_report = next(
        report
        for report in result["cohorts"]
        if report.get("equivalence_scope") == "reference_equivalence"
    )
    assert reference_report["status"] == "failed"
    assert "global_final_state_digest" in reference_report["mismatched_digests"]
    by_id = {item["child_run_id"]: item for item in items}
    assert by_id["hash"]["paper_candidate"] is True
    assert by_id["hash"]["pairwise_logical_state_equivalent"] is True
    assert by_id["metatrack"]["paper_candidate"] is False
    assert by_id["metatrack"]["pairwise_logical_state_equivalent"] is False
    assert by_id["metatrack"]["reference_state_equivalent"] is False


def test_state_equivalence_gate_isolates_valid_stateless_pair_from_metatrack_mismatch() -> None:
    items, result = _apply_state_equivalence_gate(
        [
            _completed_child(
                "hash-serial",
                "stateless_hash_serial",
                "stateless_remote_home_v1",
                final="reference-final",
            ),
            _completed_child(
                "hash-blockstm",
                "stateless_hash_block_stm",
                "stateless_remote_home_v1",
                final="reference-final",
            ),
            _completed_child(
                "meta-serial",
                "metatrack_serial",
                "stateless_remote_home_v1",
                final="metatrack-final",
            ),
            _completed_child(
                "meta-blockstm",
                "metatrack_block_stm",
                "stateless_remote_home_v1",
                final="metatrack-final",
            ),
        ]
    )

    by_id = {item["child_run_id"]: item for item in items}
    assert by_id["hash-serial"]["paper_candidate"] is True
    assert by_id["hash-blockstm"]["paper_candidate"] is True
    assert by_id["hash-serial"]["pairwise_logical_state_equivalent"] is True
    assert by_id["hash-blockstm"]["pairwise_logical_state_equivalent"] is True
    assert by_id["meta-serial"]["paper_candidate"] is False
    assert by_id["meta-blockstm"]["paper_candidate"] is False
    assert by_id["meta-serial"]["reference_state_equivalent"] is False
    assert by_id["meta-blockstm"]["reference_state_equivalent"] is False
    assert result["performance_comparison_valid"] is False


def test_state_equivalence_gate_requires_home_mapping_for_stateless_pair() -> None:
    items, result = _apply_state_equivalence_gate(
        [
            _completed_child(
                "hash",
                "stateless_hash_serial",
                "stateless_remote_home_v1",
                mapping="",
            ),
            _completed_child(
                "metatrack",
                "metatrack_serial",
                "stateless_remote_home_v1",
                mapping="",
            ),
        ]
    )
    assert result["passed"] is False
    assert "state_home_mapping_digest" in result["cohorts"][0][
        "missing_digests"
    ]
    assert all(item["pairwise_logical_state_equivalent"] is False for item in items)


def test_completion_gate_requires_cross_method_state_digests(tmp_path: Path) -> None:
    result = _completion_gate(tmp_path, {})
    assert "real_cluster_summary.json:missing_initial_state_digest" in result["blockers"]
    assert "real_cluster_summary.json:missing_global_final_state_digest" in result["blockers"]


def test_completion_gate_requires_home_mapping_for_stateless_runtime(tmp_path: Path) -> None:
    result = _completion_gate(
        tmp_path,
        {
            "initial_state_digest": "initial",
            "global_final_state_digest": "final",
            "cross_shard_execution_mode": "stateless_direct_execution",
        },
    )
    assert "real_cluster_summary.json:missing_state_home_mapping_digest" in result["blockers"]


def test_state_equivalence_gate_excludes_invalid_runs_without_poisoning_valid_pair() -> None:
    invalid_hash = _completed_child(
        "invalid-hash", "stateless_hash_serial", "stateless_remote_home_v1", final="wrong"
    )
    invalid_hash["metrics"]["finalized_unique_logical_tx_count"] = 20
    invalid_hash["metrics"]["cross_shard_failed_unique_count"] = 80
    invalid_hash["metrics"]["lifecycle_complete"] = False
    invalid_hash["result"]["summary"]["finality_evidence"]["finalized_unique_logical_tx_count"] = 20
    invalid_hash["result"]["summary"]["finality_evidence"]["cross_shard_failed_unique_count"] = 80
    meta_serial = _completed_child(
        "meta-serial", "metatrack_serial", "stateless_remote_home_v1", final="correct"
    )
    meta_blockstm = _completed_child(
        "meta-blockstm", "metatrack_block_stm", "stateless_remote_home_v1", final="correct"
    )

    items, result = _apply_state_equivalence_gate([invalid_hash, meta_serial, meta_blockstm])

    by_id = {item["child_run_id"]: item for item in items}
    assert by_id["invalid-hash"]["individual_result_valid"] is False
    assert by_id["invalid-hash"]["pairwise_logical_state_equivalent"] is None
    assert by_id["meta-serial"]["pairwise_logical_state_equivalent"] is True
    assert by_id["meta-blockstm"]["pairwise_logical_state_equivalent"] is True
    meta_report = next(
        report
        for report in result["cohorts"]
        if report.get("method_family") == "metatrack"
    )
    hash_report = next(
        report
        for report in result["cohorts"]
        if report.get("method_family") == "stateless_hash_reference"
    )
    assert meta_report["valid_completed_child_count"] == 2
    assert meta_report["invalid_completed_child_count"] == 0
    assert hash_report["valid_completed_child_count"] == 0
    assert hash_report["invalid_completed_child_count"] == 1
    assert result["pairwise_logical_state_equivalent"] is True


def test_native_metatrack_multishard_state_ready_remains_paper_candidate() -> None:
    hash_serial = _completed_child(
        "hash", "stateless_hash_serial", "stateless_remote_home_v1", final="same"
    )
    metatrack = _completed_child(
        "meta", "metatrack_serial", "stateless_remote_home_v1", final="same"
    )
    hash_serial["topology_point"] = {"nodes": 8, "shards": 2, "validators_per_shard": 4}
    metatrack["topology_point"] = {"nodes": 8, "shards": 2, "validators_per_shard": 4}

    items, result = _apply_state_equivalence_gate([hash_serial, metatrack])

    by_id = {item["child_run_id"]: item for item in items}
    assert result["pairwise_logical_state_equivalent"] is True
    assert by_id["hash"]["paper_candidate"] is True
    assert by_id["meta"]["pairwise_logical_state_equivalent"] is True
    assert by_id["meta"]["paper_candidate"] is True
    assert by_id["meta"]["comparison_eligibility_status"] == "passed"


def test_metatrack_block_stm_multishard_hybrid_prefetch_barrier_is_not_paper_candidate() -> None:
    hash_serial = _completed_child(
        "hash", "stateless_hash_serial", "stateless_remote_home_v1", final="same"
    )
    hybrid = _completed_child(
        "hybrid", "metatrack_block_stm", "stateless_remote_home_v1", final="same"
    )
    hash_serial["topology_point"] = {"nodes": 8, "shards": 2, "validators_per_shard": 4}
    hybrid["topology_point"] = {"nodes": 8, "shards": 2, "validators_per_shard": 4}

    items, result = _apply_state_equivalence_gate([hash_serial, hybrid])

    by_id = {item["child_run_id"]: item for item in items}
    assert result["pairwise_logical_state_equivalent"] is True
    assert by_id["hash"]["paper_candidate"] is True
    assert by_id["hybrid"]["pairwise_logical_state_equivalent"] is True
    assert by_id["hybrid"]["paper_candidate"] is False
    assert by_id["hybrid"]["comparison_eligibility_status"] == (
        "metatrack_block_stm_multi_shard_prefetch_barrier_hybrid_boundary"
    )



def test_global_business_state_digest_is_diagnostic_and_requires_replica_consistency() -> None:
    summary = {
        "node_summaries": [
            {"node_id": "n0", "shard_id": "s0", "business_state_digest": "a"},
            {"node_id": "n1", "shard_id": "s0", "business_state_digest": "a"},
            {"node_id": "n2", "shard_id": "s1", "business_state_digest": "b"},
            {"node_id": "n3", "shard_id": "s1", "business_state_digest": "b"},
        ]
    }
    assert _global_business_state_digest(summary)
    summary["node_summaries"][1]["business_state_digest"] = "different"
    assert _global_business_state_digest(summary) == ""


def test_metatrack_control_plane_evidence_deduplicates_pbft_replicas(tmp_path: Path) -> None:
    client = tmp_path / "client"
    client.mkdir()
    (client / "transaction_placement.csv").write_text(
        "execution_shard,reason,predicted_remote_reads,predicted_remote_writes\n"
        "s0,majority_place:coverage=3,1,2\n"
        "s1,majority_place:coverage=2:queue_tie_load=0,2,1\n",
        encoding="utf-8",
    )
    (client / "placement_score.csv").write_text(
        "state_key,candidate_shard,coaccess_affinity,admissible,capacity,projected_load,current_state_load,score\n"
        "k,s0,2,true,10,3,1,2\n",
        encoding="utf-8",
    )
    node = lambda node_id, shard, native, waves: {
        "node_id": node_id,
        "shard_id": shard,
        "state_ready_wait_count": native,
        "state_ready_resume_count": native - 1,
        "state_prefetch_wait_ms": native * 2,
        "remote_state_fetch_count": native + 1,
        "remote_state_fetch_completed_count": native,
        "state_ready_scheduler_mode": "transaction_level_suspend_resume",
        "versioned_state_ready_wave_count": waves,
        "versioned_state_ready_wait_observation_count": waves + 1,
        "versioned_state_ready_resolved_token_count": waves + 2,
        "versioned_state_probe_count": waves + 3,
        "versioned_state_probe_latency_ms": waves + 4,
        "versioned_state_ready_max_wave_width": waves,
        "versioned_state_ready_scheduler_mode": "per_transaction_per_key_version_frontier",
    }
    summary = {"node_summaries": [node("n0", "s0", 3, 4), node("n1", "s0", 3, 4), node("n2", "s1", 5, 6), node("n3", "s1", 5, 6)]}
    evidence = _metatrack_control_plane_evidence(tmp_path, summary)
    assert evidence["metatrack_execution_shard_transaction_counts"] == {"s0": 1, "s1": 1}
    assert evidence["metatrack_max_execution_shard_share"] == 0.5
    assert evidence["metatrack_placement_score_row_count"] == 1
    assert evidence["metatrack_state_ready_wait_count"] == 8
    assert evidence["metatrack_remote_state_fetch_count"] == 10
    assert evidence["versioned_state_ready_wave_count"] == 10
    assert evidence["versioned_state_ready_max_wave_width"] == 6
    assert evidence["metatrack_state_ready_scheduler_mode"] == "transaction_level_suspend_resume"
    assert evidence["versioned_state_ready_scheduler_mode"] == "per_transaction_per_key_version_frontier"
