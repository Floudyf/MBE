# MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
from backend.app.services.v5_formal_dto import V5FormalChildResponse, V5FormalRunGroupDetailResponse, child_detail, group_detail


def _fixture() -> tuple[dict, dict]:
    child = {
        "child_run_id": "v5child_fixture_serial",
        "run_group_id": "v5grp_fixture",
        "suite_type": "comparison_experiment",
        "method_config_id": "hash_serial",
        "method": {"method_id": "hash_serial", "display_name": "Stateful Hash + Serial Reference"},
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8},
        "workload_point": {},
        "fault_point": {},
        "seed": 11,
        "repeat_index": 0,
        "status": "completed",
        "execution_status": "completed",
        "artifact_status": "complete",
        "formal_eligibility": True,
        "paper_candidate": True,
        "comparison_semantics_class": "stateful_local_legacy_v1",
        "performance_comparison_valid": False,
        "direct_cross_semantic_performance_comparison_valid": False,
        "pairwise_logical_state_equivalent": True,
        "individual_result_valid": True,
        "metrics": {"end_to_end_tps": 1008.06},
        "result": {
            "run_id": "v5_fixture_run",
            "status": "completed",
            "summary": {"runtime_truth": "v5_real_cluster_candidate", "no_fallback": True},
            "no_fallback": True,
            "artifacts": [
                {"name": "network_metrics_summary.json", "size_bytes": 101, "truth_category": "runtime_artifact", "download_url": "/network"},
                {"name": "resource_usage_summary.json", "size_bytes": 202, "truth_category": "runtime_artifact", "download_url": "/resource"},
                {"name": "aggregate/mechanism_metrics_summary.json", "size_bytes": 303, "truth_category": "runtime_artifact", "download_url": "/mechanism"},
                {"name": "workload_replay_summary.json", "size_bytes": 404, "truth_category": "runtime_artifact", "download_url": "/workload"},
                {"name": "transaction_lifecycle.csv", "size_bytes": 505, "truth_category": "runtime_artifact", "download_url": "/lifecycle"},
            ],
        },
    }
    group = {
        "run_group_id": "v5grp_fixture",
        "status": "completed",
        "execution_backend": "real_cluster",
        "runtime_truth": "v5_real_cluster_candidate",
        "total_child_runs": 1,
        "completed_child_runs": 1,
        "performance_comparison_valid": False,
        "direct_cross_semantic_performance_comparison_valid": False,
        "within_semantic_cohort_state_equivalence_valid": True,
        "pairwise_logical_state_equivalent": True,
        "fairness_validation": {
            "passed": True,
            "performance_comparison_valid": False,
            "direct_cross_semantic_performance_comparison_valid": False,
        },
        "state_equivalence_validation": {
            "passed": True,
            "performance_comparison_valid": False,
            "within_semantic_cohort_state_equivalence_valid": True,
            "pairwise_logical_state_equivalent": True,
            "cohorts": [
                {
                    "comparison_semantics_class": "stateful_local_legacy_v1",
                    "child_run_ids": ["v5child_fixture_serial"],
                    "status": "passed",
                }
            ],
        },
        "plan": {
            "name": "fixture",
            "suites": ["comparison_experiment"],
            "methods": [{"method_id": "hash_serial", "display_name": "Stateful Hash + Serial Reference"}],
            "source_label": "user",
            "tags": [],
        },
        "formal_experiment_profile": {
            "worker_execution_truth": {
                "hash_serial": {"requested_worker_count": 8, "effective_worker_count": 1}
            }
        },
    }
    return group, child


def test_group_detail_preserves_comparison_truth_semantic_cohort_and_compact_child_evidence() -> None:
    group, child = _fixture()
    payload = group_detail(group, [child])
    # Exercise the same Pydantic response-model boundary used by the API; extra
    # truth/evidence fields must survive validation/serialization.
    payload = V5FormalRunGroupDetailResponse.model_validate(payload).model_dump()
    public_group = payload["group"]
    public_child = payload["children"][0]

    assert public_group["performance_comparison_valid"] is False
    assert public_group["direct_cross_semantic_performance_comparison_valid"] is False
    assert public_group["state_equivalence_validation"]["cohorts"][0]["comparison_semantics_class"] == "stateful_local_legacy_v1"
    assert public_child["comparison_semantics_class"] == "stateful_local_legacy_v1"
    assert public_child["direct_cross_semantic_performance_comparison_valid"] is False
    assert public_child["runtime_artifact_count"] == 5
    assert public_child["runtime_artifact_bytes"] == 1515

    names = {item["name"] for item in public_child["evidence_artifacts"]}
    assert "network_metrics_summary.json" in names
    assert "resource_usage_summary.json" in names
    assert "aggregate/mechanism_metrics_summary.json" in names
    assert "workload_replay_summary.json" in names
    assert "transaction_lifecycle.csv" not in names
    assert all(item["child_run_id"] == child["child_run_id"] for item in public_child["evidence_artifacts"])
    assert "artifacts" not in public_child["result"]  # group detail stays compact


def test_child_detail_keeps_full_runtime_artifact_metadata_for_audit_view() -> None:
    _, child = _fixture()
    child["result"]["artifacts"][0].update(
        {
            "artifact_role": "research_observability",
            "truth_scope": "child_run",
            "producer": "v5_observability_metrics",
            "schema_version": "mbe_v5_network_metrics_v1",
            "sha256": "abc123",
        }
    )
    payload = child_detail(child)
    payload = V5FormalChildResponse.model_validate(payload).model_dump()
    artifact = payload["result"]["artifacts"][0]
    assert artifact["artifact_role"] == "research_observability"
    assert artifact["truth_scope"] == "child_run"
    assert artifact["producer"] == "v5_observability_metrics"
    assert artifact["schema_version"] == "mbe_v5_network_metrics_v1"
    assert artifact["sha256"] == "abc123"
