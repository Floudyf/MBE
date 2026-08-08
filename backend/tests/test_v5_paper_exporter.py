import csv
import json

from backend.app.services.v5_paper_exporter import export


def _child(child_id: str, suite: str, method_id: str, name: str, *, scan: str = "", value: str = "", nodes: int = 4, fault: str = "disabled", block_size: int = 100, block_interval_ms: int = 75) -> dict:
    return {
        "child_run_id": child_id, "status": "completed", "suite_type": suite,
        "method_config_id": method_id, "method": {"display_name": name}, "method_role": "main" if method_id == "main" else "ablation",
        "scan_variable": scan, "scan_value": value, "topology_point": {"nodes": nodes, "shards": 1, "validators_per_shard": nodes},
        "workload_point": {"cross_shard_ratio": 0, "timeout_every": 0}, "fault_point": {"mode": fault}, "estimated_transactions": 20,
        "block_size": block_size, "block_interval_ms": block_interval_ms,
        "metrics": {
            "throughput_tps": 10.0 if method_id == "main" else 8.0,
            "end_to_end_tps": 10.0 if method_id == "main" else 8.0,
            "logical_finality_tps": 10.0 if method_id == "main" else 8.0,
            "p50_latency_ms": 1.0,
            "p95_latency_ms": 2.0,
            "p99_latency_ms": 3.0,
            "p95_finality_ms": 2.0,
            "p99_finality_ms": 3.0,
            "lifecycle_complete": True,
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "metric_completeness": "complete",
        },
        "result": {"summary": {
            "no_fallback": True,
            "state_root_consistent": True,
            "receipt_root_consistent": True,
            "plan_digest_consistent": True,
            "finality_evidence": {
                "submitted_unique_tx_count": 20,
                "terminal_unique_tx_count": 20,
                "finalized_unique_logical_tx_count": 20,
                "incomplete_unique_tx_count": 0,
                "cross_shard_requested_unique_count": 0,
                "cross_shard_finalized_unique_count": 0,
                "cross_shard_failed_unique_count": 0,
            },
        }},
    }


def test_paper_exports_are_grouped_by_suite_method_and_scan_not_copied(tmp_path):
    children = [
        _child("a", "comparison_experiment", "main", "Main"), _child("b", "comparison_experiment", "abl", "Ablation"),
        _child("c", "workload_sensitivity", "main", "Main", scan="tx_count", value="20"), _child("d", "workload_sensitivity", "main", "Main", scan="tx_count", value="40"),
        _child("e", "topology_scaling", "main", "Main", scan="topology", value="4", nodes=4), _child("f", "topology_scaling", "main", "Main", scan="topology", value="8", nodes=8),
        _child("g", "fault_recovery_experiment", "main", "Main", scan="fault_policy", value="disabled", fault="disabled"), _child("h", "fault_recovery_experiment", "main", "Main", scan="fault_policy", value="delay", fault="delay_only"),
    ]
    export(tmp_path, {"run_group_id": "v5grp_test"}, children)
    comparison = list(csv.DictReader((tmp_path / "comparison_summary.csv").open(encoding="utf-8")))
    sensitivity = list(csv.DictReader((tmp_path / "sensitivity_summary.csv").open(encoding="utf-8")))
    scaling = list(csv.DictReader((tmp_path / "scaling_summary.csv").open(encoding="utf-8")))
    faults = list(csv.DictReader((tmp_path / "fault_recovery_summary.csv").open(encoding="utf-8")))
    assert {row["method_config_id"] for row in comparison} == {"main", "abl"}
    assert {row["scan_value"] for row in sensitivity} == {"20", "40"}
    assert {row["topology_nodes"] for row in scaling} == {"4", "8"}
    assert {row["fault_mode"] for row in faults} == {"disabled", "delay_only"}
    assert (tmp_path / "comparison_summary.csv").read_text() != (tmp_path / "sensitivity_summary.csv").read_text()


def test_export_uses_base_workload_when_child_has_no_workload_override(tmp_path):
    children = [
        _child("comparison", "comparison_experiment", "main", "Main"),
        _child("scaling", "topology_scaling", "main", "Main", scan="topology", value="4"),
        _child("fault", "fault_recovery_experiment", "main", "Main", scan="fault_policy", value="disabled"),
    ]
    for child in children:
        child["workload_point"] = {}
    group = {"run_group_id": "v5grp_base_workload", "plan": {"base_spec": {"plugin_selections": [{"category": "workload", "config": {"cross_shard_ratio": 0.25, "timeout_every": 0}}]}}}
    export(tmp_path, group, children)
    for name in ("comparison_summary.csv", "scaling_summary.csv", "fault_recovery_summary.csv", "paper_table_data.csv"):
        rows = list(csv.DictReader((tmp_path / name).open(encoding="utf-8")))
        assert rows and all(row["cross_shard_ratio"] == "0.25" and row["timeout_every"] == "0" for row in rows)


def test_paper_exporter_keeps_block_production_as_analysis_dimension(tmp_path):
    children = [
        _child("block-a", "comparison_experiment", "main", "Main", block_size=100, block_interval_ms=75),
        _child("block-b", "comparison_experiment", "main", "Main", block_size=250, block_interval_ms=100),
    ]

    export(tmp_path, {"run_group_id": "v5grp_block_production"}, children)

    comparison = list(csv.DictReader((tmp_path / "comparison_summary.csv").open(encoding="utf-8")))
    table = list(csv.DictReader((tmp_path / "paper_table_data.csv").open(encoding="utf-8")))
    assert {(row["block_size"], row["block_interval_ms"]) for row in comparison} == {("100", "75"), ("250", "100")}
    assert {(row["block_size"], row["block_interval_ms"]) for row in table} == {("100", "75"), ("250", "100")}
    assert all(row["sample_count"] == "1" for row in comparison)


def _paper_child(child_id: str, method_id: str, name: str, *, tps: float = 10.0, p95: float = 20.0, p99: float = 30.0, complete: bool = True) -> dict:
    child = _child(child_id, "comparison_experiment", method_id, name)
    child["metrics"].update({
        "end_to_end_tps": tps,
        "logical_finality_tps": tps,
        "p95_finality_ms": p95,
        "p99_finality_ms": p99,
        "no_fallback": True,
        "state_root_consistent": True,
        "receipt_root_consistent": True,
        "plan_digest_consistent": True,
        "metric_completeness": "complete",
    })
    child["result"]["summary"]["no_fallback"] = True
    child["result"]["summary"]["state_root_consistent"] = True
    child["result"]["summary"]["receipt_root_consistent"] = True
    child["result"]["summary"]["plan_digest_consistent"] = True
    child["result"]["summary"]["finality_evidence"]["finalized_unique_logical_tx_count"] = 20
    child["result"]["summary"]["finality_evidence"]["cross_shard_failed_unique_count"] = 0
    child["metrics"]["lifecycle_complete"] = True
    if not complete:
        child["metrics"]["metric_completeness"] = "incomplete"
        child["metrics"]["missing"] = ["p95_finality_ms", "p99_finality_ms"]
        child["metrics"].pop("p95_finality_ms", None)
        child["metrics"].pop("p99_finality_ms", None)
    return child


def test_export_writes_paper_result_analysis_with_single_sample_note(tmp_path):
    child = _paper_child("paper-a", "hash_serial", "Baseline", tps=12.5, p99=42.0)

    overall = export(tmp_path, {"run_group_id": "v5grp_paper"}, [child])

    assert overall["completed_count"] == 1
    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "mbe_paper_result_analysis_v2"
    assert payload["analysis_status"] == "complete"
    row = payload["metrics"]["end_to_end_tps"][0]
    assert row["method_id"] == "hash_serial"
    assert row["valid_sample_count"] == 1
    assert row["mean"] == 12.5
    assert row["std"] is None
    assert row["ci95_low"] is None and row["ci95_high"] is None
    assert row["statistical_note"] == "single_sample_no_variance_or_ci"
    csv_rows = list(csv.DictReader((tmp_path / "aggregate" / "paper_result_analysis.csv").open(encoding="utf-8")))
    assert {item["metric"] for item in csv_rows} == {"end_to_end_tps", "p95_finality_ms", "p99_finality_ms"}


def test_paper_figure_data_uses_v5_2_metric_names(tmp_path):
    child = _paper_child("paper-a", "hash_serial", "Baseline", tps=12.5, p99=42.0)

    export(tmp_path, {"run_group_id": "v5grp_paper"}, [child])

    rows = list(csv.DictReader((tmp_path / "paper_figure_data.csv").open(encoding="utf-8")))
    metrics = {row["metric"] for row in rows}
    assert "end_to_end_tps" in metrics
    assert "p99_finality_ms" in metrics
    assert "throughput_tps" not in metrics
    assert "p99_latency_ms" not in metrics


def test_paper_result_analysis_excludes_incomplete_samples(tmp_path):
    accepted = _paper_child("paper-ok", "hash_serial", "Baseline", tps=10.0, p99=40.0)
    excluded = _paper_child("paper-missing", "hash_serial", "Baseline", complete=False)

    export(tmp_path, {"run_group_id": "v5grp_paper_excluded"}, [accepted, excluded])

    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert payload["analysis_status"] == "incomplete"
    assert payload["excluded_samples"][0]["child_run_id"] == "paper-missing"
    assert "metric_completeness_not_complete" in payload["excluded_samples"][0]["reasons"]
    assert payload["metrics"]["end_to_end_tps"][0]["valid_sample_count"] == 1
    assert payload["metrics"]["end_to_end_tps"][0]["excluded_sample_count"] == 0
    observed = payload["observed_metrics"]["end_to_end_tps"][0]
    assert observed["observed_sample_count"] == 2
    assert observed["sample_status"] == "completed_invalid"
    assert payload["status_counts"]["completed_invalid"] == 1


def test_paper_result_analysis_classifies_failed_invalid_and_comparison_excluded(tmp_path):
    eligible = _paper_child("eligible", "hash_serial", "Stateful")
    invalid = _paper_child("invalid", "stateless_hash_serial", "Stateless")
    invalid["metrics"]["lifecycle_complete"] = False
    invalid["result"]["summary"]["finality_evidence"]["finalized_unique_logical_tx_count"] = 5
    invalid["result"]["summary"]["finality_evidence"]["cross_shard_failed_unique_count"] = 15
    excluded = _paper_child("excluded", "hash_aria", "Aria")
    excluded["pairwise_logical_state_equivalent"] = False
    failed = _paper_child("failed", "stateless_hash_block_stm", "Stateless B-STM")
    failed["status"] = "failed"
    failed["execution_status"] = "failed"

    export(tmp_path, {"run_group_id": "classification"}, [eligible, invalid, excluded, failed])
    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))

    assert payload["status_counts"] == {
        "execution_failed": 1,
        "blocked_incompatible": 0,
        "completed_invalid": 1,
        "comparison_excluded": 1,
        "paper_eligible": 1,
    }
    strict_ids = {row["method_id"] for row in payload["metrics"]["end_to_end_tps"]}
    assert strict_ids == {"hash_serial"}
    observed_ids = {row["method_id"] for row in payload["observed_metrics"]["end_to_end_tps"]}
    assert observed_ids == {"hash_serial", "stateless_hash_serial", "hash_aria", "stateless_hash_block_stm"}
    failed_row = next(row for row in payload["observed_metrics"]["end_to_end_tps"] if row["method_id"] == "stateless_hash_block_stm")
    assert failed_row["mean"] is None
    assert failed_row["sample_status"] == "execution_failed"


def test_paper_result_analysis_separates_blocked_incompatible_from_execution_failed(tmp_path):
    eligible = _paper_child("eligible", "hash_serial", "Stateful")
    blocked = _paper_child("blocked", "hash_groundhog", "Groundhog")
    blocked["status"] = "blocked"
    blocked["execution_status"] = "blocked_incompatible_workload"
    blocked["error"] = "groundhog_cross_shard_unsupported"
    blocked["metrics"] = {}
    blocked["result"] = {}

    overall = export(tmp_path, {"run_group_id": "classification-blocked"}, [eligible, blocked])
    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))

    assert payload["status_counts"]["blocked_incompatible"] == 1
    assert payload["status_counts"]["execution_failed"] == 0
    assert overall["paper_valid_count"] == 1
    assert overall["blocked_count"] == 1
    assert overall["execution_failed_count"] == 0
    blocked_rows = list(csv.DictReader((tmp_path / "blocked_children.csv").open(encoding="utf-8")))
    failed_rows = list(csv.DictReader((tmp_path / "failed_children.csv").open(encoding="utf-8")))
    assert blocked_rows[0]["child_run_id"] == "blocked"
    assert blocked_rows[0]["sample_status"] == "blocked_incompatible"
    assert failed_rows == []


def test_invalid_completed_sample_is_preserved_but_excluded_from_all_paper_aggregates(tmp_path):
    eligible = _paper_child("eligible", "hash_serial", "Stateful", tps=100.0)
    invalid = _paper_child("invalid", "hash_groundhog", "Groundhog", tps=1500.0)
    invalid["metrics"]["lifecycle_complete"] = False
    invalid["result"]["summary"]["finality_evidence"]["finalized_unique_logical_tx_count"] = 10
    invalid["result"]["summary"]["finality_evidence"]["cross_shard_failed_unique_count"] = 10

    overall = export(tmp_path, {"run_group_id": "invalid-aggregate-isolation"}, [eligible, invalid])

    assert overall["count"] == 1
    assert overall["mean"] == 100.0
    assert overall["observed_completed_count"] == 2
    assert overall["completed_invalid_count"] == 1

    aggregate = list(csv.DictReader((tmp_path / "aggregate_summary.csv").open(encoding="utf-8")))
    assert aggregate[0]["mean"] == "100.0"
    comparison = list(csv.DictReader((tmp_path / "comparison_summary.csv").open(encoding="utf-8")))
    by_method = {row["method_config_id"]: row for row in comparison}
    assert by_method["hash_serial"]["mean_tps"] == "100.0"
    assert by_method["hash_groundhog"]["sample_count"] == "0"
    assert by_method["hash_groundhog"]["completed_invalid_count"] == "1"
    invalid_rows = list(csv.DictReader((tmp_path / "invalid_children.csv").open(encoding="utf-8")))
    assert invalid_rows[0]["child_run_id"] == "invalid"
    assert invalid_rows[0]["observed_tps"] == "1500.0"
    observed_rows = list(csv.DictReader((tmp_path / "observed_results.csv").open(encoding="utf-8")))
    assert {row["child_run_id"] for row in observed_rows} == {"eligible", "invalid"}
    paper_rows = list(csv.DictReader((tmp_path / "paper_valid_results.csv").open(encoding="utf-8")))
    assert [row["child_run_id"] for row in paper_rows] == ["eligible"]




def test_explicit_paper_candidate_false_excludes_individually_valid_sample(tmp_path):
    eligible = _paper_child("eligible-paper", "hash_serial", "Stateful", tps=100.0)
    singleton = _paper_child("singleton-groundhog", "hash_groundhog", "Groundhog", tps=42.0)
    singleton["paper_candidate"] = False
    singleton["comparison_eligibility_status"] = "insufficient_valid_runs"
    singleton["pairwise_logical_state_equivalent"] = None

    overall = export(tmp_path, {"run_group_id": "paper-candidate-authority"}, [eligible, singleton])

    assert overall["observed_completed_count"] == 2
    assert overall["individually_valid_completed_count"] == 2
    assert overall["paper_valid_count"] == 1
    paper_rows = list(csv.DictReader((tmp_path / "paper_valid_results.csv").open(encoding="utf-8")))
    assert [row["child_run_id"] for row in paper_rows] == ["eligible-paper"]
    observed_rows = {row["child_run_id"]: row for row in csv.DictReader((tmp_path / "observed_results.csv").open(encoding="utf-8"))}
    assert observed_rows["singleton-groundhog"]["sample_status"] == "comparison_excluded"
    assert observed_rows["singleton-groundhog"]["individual_result_valid"] == "True"
    excluded = list(csv.DictReader((tmp_path / "comparison_excluded_children.csv").open(encoding="utf-8")))
    assert excluded[0]["child_run_id"] == "singleton-groundhog"
    assert "paper_candidate_false:insufficient_valid_runs" in excluded[0]["reasons"]


def test_run_group_report_separates_individual_validity_from_paper_comparability(tmp_path):
    eligible = _paper_child("eligible-report", "hash_serial", "Stateful")
    comparison_excluded = _paper_child("excluded-report", "hash_groundhog", "Groundhog")
    comparison_excluded["pairwise_logical_state_equivalent"] = False

    overall = export(tmp_path, {"run_group_id": "report-labels"}, [eligible, comparison_excluded])

    assert overall["individually_valid_completed_count"] == 2
    assert overall["paper_valid_count"] == 1
    report = (tmp_path / "run_group_report.md").read_text(encoding="utf-8")
    assert "Individually valid completed: 2" in report
    assert "Within-semantic paper candidates: 1" in report
    assert "Direct cross-semantic performance comparison valid: true" in report
    assert "Paper-valid:" not in report


def test_cross_semantic_incomparable_group_suppresses_misleading_group_mean(tmp_path):
    stateful = _paper_child("stateful", "hash_serial", "Stateful", tps=100.0)
    stateless = _paper_child("stateless", "stateless_hash_serial", "Stateless", tps=200.0)
    group = {
        "run_group_id": "cross-semantic-no-aggregate",
        "performance_comparison_valid": False,
        "fairness_validation": {"passed": True, "performance_comparison_valid": False},
    }

    overall = export(tmp_path, group, [stateful, stateless])

    assert overall["paper_valid_count"] == 2
    assert overall["direct_cross_semantic_performance_comparison_valid"] is False
    assert overall["mean"] is None
    assert overall["median"] is None
    assert overall["ci95_low"] is None
    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert payload["analysis_status"] == "incomparable"
    assert payload["performance_comparison_valid"] is False
    report = (tmp_path / "run_group_report.md").read_text(encoding="utf-8")
    assert "Direct cross-semantic performance comparison valid: false" in report
