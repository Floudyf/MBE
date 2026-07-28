import csv
import json

from backend.app.services.v5_paper_exporter import export


def _child(child_id: str, suite: str, method_id: str, name: str, *, scan: str = "", value: str = "", nodes: int = 4, fault: str = "disabled") -> dict:
    return {
        "child_run_id": child_id, "status": "completed", "suite_type": suite,
        "method_config_id": method_id, "method": {"display_name": name}, "method_role": "main" if method_id == "main" else "ablation",
        "scan_variable": scan, "scan_value": value, "topology_point": {"nodes": nodes, "shards": 1, "validators_per_shard": nodes},
        "workload_point": {"cross_shard_ratio": 0, "timeout_every": 0}, "fault_point": {"mode": fault}, "estimated_transactions": 20,
        "metrics": {"throughput_tps": 10.0 if method_id == "main" else 8.0, "p50_latency_ms": 1.0, "p95_latency_ms": 2.0, "p99_latency_ms": 3.0},
        "result": {"summary": {"finality_evidence": {"submitted_unique_tx_count": 20, "terminal_unique_tx_count": 20, "incomplete_unique_tx_count": 0, "cross_shard_requested_unique_count": 0, "cross_shard_finalized_unique_count": 0}}},
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


def _paper_child(child_id: str, method_id: str, name: str, *, tps: float = 10.0, p99: float = 30.0, complete: bool = True) -> dict:
    child = _child(child_id, "comparison_experiment", method_id, name)
    child["metrics"].update({
        "end_to_end_tps": tps,
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
    if not complete:
        child["metrics"]["metric_completeness"] = "incomplete"
        child["metrics"]["missing"] = ["p99_finality_ms"]
        child["metrics"].pop("p99_finality_ms", None)
    return child


def test_export_writes_paper_result_analysis_with_single_sample_note(tmp_path):
    child = _paper_child("paper-a", "hash_serial", "Baseline", tps=12.5, p99=42.0)

    overall = export(tmp_path, {"run_group_id": "v5grp_paper"}, [child])

    assert overall["completed_count"] == 1
    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert payload["schema_version"] == "mbe_paper_result_analysis_v1"
    assert payload["analysis_status"] == "complete"
    row = payload["metrics"]["end_to_end_tps"][0]
    assert row["method_id"] == "hash_serial"
    assert row["valid_sample_count"] == 1
    assert row["mean"] == 12.5
    assert row["std"] is None
    assert row["ci95_low"] is None and row["ci95_high"] is None
    assert row["statistical_note"] == "single_sample_no_variance_or_ci"
    csv_rows = list(csv.DictReader((tmp_path / "aggregate" / "paper_result_analysis.csv").open(encoding="utf-8")))
    assert {item["metric"] for item in csv_rows} == {"end_to_end_tps", "p99_finality_ms"}


def test_paper_result_analysis_excludes_incomplete_samples(tmp_path):
    accepted = _paper_child("paper-ok", "hash_serial", "Baseline", tps=10.0, p99=40.0)
    excluded = _paper_child("paper-missing", "hash_serial", "Baseline", complete=False)

    export(tmp_path, {"run_group_id": "v5grp_paper_excluded"}, [accepted, excluded])

    payload = json.loads((tmp_path / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
    assert payload["analysis_status"] == "incomplete"
    assert payload["excluded_samples"][0]["child_run_id"] == "paper-missing"
    assert "metric_completeness_not_complete" in payload["excluded_samples"][0]["reasons"]
    assert payload["metrics"]["end_to_end_tps"][0]["valid_sample_count"] == 1
    assert payload["metrics"]["end_to_end_tps"][0]["excluded_sample_count"] == 1
