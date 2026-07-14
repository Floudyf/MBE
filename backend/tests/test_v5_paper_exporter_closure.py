import csv

from backend.app.services.v5_paper_exporter import analysis, export


def _child(suite, point=None):
    return {"child_run_id": suite, "status": "completed", "suite_type": suite, "method_config_id": "method", "method": {"display_name": "Method"}, "method_role": "main", "topology_point": {"nodes": 4, "shards": 1, "validators_per_shard": 4}, "workload_point": point or {}, "fault_point": {"mode": "disabled"}, "estimated_transactions": 40, "metrics": {"throughput_tps": 10.0, "p50_latency_ms": 1.0, "p95_latency_ms": 2.0, "p99_latency_ms": 3.0}, "result": {"summary": {"finality_evidence": {"terminal_unique_tx_count": 40, "incomplete_unique_tx_count": 0}}}}


def _group():
    return {"run_group_id": "group", "plan": {"base_spec": {"plugin_selections": [{"category": "workload", "config": {"cross_shard_ratio": 0.25, "timeout_every": 0}}]}}}


def test_base_workload_is_retained_by_csv_and_analysis(tmp_path):
    children = [_child("comparison_experiment"), _child("topology_scaling"), _child("fault_recovery_experiment")]
    export(tmp_path, _group(), children)
    for name in ("comparison_summary.csv", "scaling_summary.csv", "fault_recovery_summary.csv", "paper_table_data.csv"):
        assert all(row["cross_shard_ratio"] == "0.25" and row["timeout_every"] == "0" for row in csv.DictReader((tmp_path / name).open(encoding="utf-8")))
    assert all(row["cross_shard_ratio"] == 0.25 for row in analysis(_group(), children)["groups"])


def test_sensitivity_workload_overrides_base_but_inherits_timeout(tmp_path):
    child = _child("workload_sensitivity", {"tx_count": 80, "cross_shard_ratio": 0.5})
    export(tmp_path, _group(), [child])
    row = next(csv.DictReader((tmp_path / "paper_table_data.csv").open(encoding="utf-8")))
    assert row["tx_count"] == "80" and row["cross_shard_ratio"] == "0.5" and row["timeout_every"] == "0"
