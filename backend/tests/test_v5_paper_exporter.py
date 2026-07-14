import csv

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
