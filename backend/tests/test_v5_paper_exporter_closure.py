import csv

from backend.app.services.v5_paper_exporter import analysis, export, _with_metatrack_consensus_treatment_gate, _paper_exclusion_reasons, _sample_status_for_child


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


def test_metatrack_fixed_batch_consensus_treatment_gate_is_condition_matched_and_fail_closed():
    def item(method, seed, active=None, max_batches=None):
        metrics={}
        if active is not None: metrics["metatrack_consensus_window_aggregation_active"]=active
        if max_batches is not None: metrics["metatrack_consensus_window_max_route_batch_count"]=max_batches
        return {"suite_type":"ablation_experiment","method_config_id":method,"seed":seed,"repeat_index":0,"topology_point":{"nodes":8,"shards":2},"workload_point":{"tx_count":10000},"metrics":metrics}
    full11=item("metatrack_latest",11,True)
    a3_11=item("metatrack_ab_cons",11,max_batches=1)
    a3_12=item("metatrack_ab_cons",12,max_batches=1)
    gated=_with_metatrack_consensus_treatment_gate([full11,a3_11,a3_12])
    by={(x["method_config_id"],x["seed"]):x for x in gated}
    assert by[("metatrack_ab_cons",11)]["_metatrack_consensus_ablation_treatment_active"] is True
    assert by[("metatrack_ab_cons",12)]["_metatrack_consensus_ablation_treatment_active"] is False
    assert "metatrack_consensus_ablation_treatment_inactive" not in _paper_exclusion_reasons(by[("metatrack_ab_cons",11)], [])
    assert "metatrack_consensus_ablation_treatment_inactive" in _paper_exclusion_reasons(by[("metatrack_ab_cons",12)], [])


def test_metatrack_fixed_batch_consensus_treatment_gate_requires_fixed_route_batch_runtime():
    full={"suite_type":"ablation_experiment","method_config_id":"metatrack_latest","seed":7,"repeat_index":0,"topology_point":{},"workload_point":{},"metrics":{"metatrack_consensus_window_aggregation_active":True}}
    a3={"suite_type":"ablation_experiment","method_config_id":"metatrack_ab_cons","seed":7,"repeat_index":0,"topology_point":{},"workload_point":{},"metrics":{"metatrack_consensus_window_max_route_batch_count":2}}
    gated=_with_metatrack_consensus_treatment_gate([full,a3])
    assert gated[1]["_metatrack_consensus_ablation_treatment_active"] is False


def test_metatrack_fixed_batch_consensus_inactive_treatment_is_excluded_from_paper_aggregate():
    def valid(method, active=None, max_batches=None):
        metrics={
            "submitted_unique_tx_count":100,
            "terminal_unique_tx_count":100,
            "finalized_unique_logical_tx_count":100,
            "incomplete_unique_tx_count":0,
            "cross_shard_failed_unique_count":0,
            "lifecycle_complete":True,
            "no_fallback":True,
            "state_root_consistent":True,
            "receipt_root_consistent":True,
            "plan_digest_consistent":True,
            "metric_completeness":"complete",
            "end_to_end_tps":300.0,
            "logical_finality_tps":300.0,
            "fast_track_logical_tx_count":90,
            "conservative_track_logical_tx_count":10,
            "replica_deduplicated_remote_fetch_count":20,
            "replica_deduplicated_remote_writeback_count":20,
            "aggregation_group_count":10,
            "pre_aggregation_physical_op_count":40,
            "post_aggregation_physical_op_count":20,
            "p95_finality_ms":10.0,
            "p99_finality_ms":15.0,
        }
        if active is not None: metrics["metatrack_consensus_window_aggregation_active"]=active
        if max_batches is not None: metrics["metatrack_consensus_window_max_route_batch_count"]=max_batches
        return {
            "child_run_id":method,
            "status":"completed",
            "execution_status":"completed",
            "suite_type":"ablation_experiment",
            "method_config_id":method,
            "method":{"display_name":method},
            "method_role":"main" if method=="metatrack_latest" else "ablation",
            "seed":11,
            "repeat_index":0,
            "estimated_transactions":10000,
            "topology_point":{"nodes":8,"shards":2,"validators_per_shard":4,"worker_count":8},
            "workload_point":{"tx_count":10000},
            "fault_point":{"mode":"disabled"},
            "block_size":1000,
            "block_interval_ms":100,
            "metrics":metrics,
        }
    full=valid("metatrack_latest",active=False)
    a3=valid("metatrack_ab_cons",max_batches=1)
    # Treatment gating must not repair or mask an individually invalid sample.
    assert _sample_status_for_child(full)=="paper_eligible"
    assert _sample_status_for_child(a3)=="paper_eligible"
    gated=_with_metatrack_consensus_treatment_gate([full,a3])
    gated_a3=next(x for x in gated if x["method_config_id"]=="metatrack_ab_cons")
    assert _sample_status_for_child(gated_a3)=="comparison_excluded"
    result=analysis(_group(),[full,a3])
    a3_group=next(x for x in result["groups"] if x["method_config_id"]=="metatrack_ab_cons")
    assert a3_group["observed_completed_count"]==1
    assert a3_group["sample_count"]==0
    assert a3_group["mean_tps"] is None
