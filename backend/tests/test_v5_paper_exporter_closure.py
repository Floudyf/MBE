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


def _v26_consensus_child(method, seed=11, *, aggregation_active=None, blocks=None, preprepare=None, messages=None, configured=True):
    metrics={}
    if aggregation_active is not None: metrics["metatrack_consensus_window_aggregation_active"]=aggregation_active
    if blocks is not None: metrics["actual_committed_block_count"]=blocks
    if preprepare is not None: metrics["pbft_preprepare_count"]=preprepare
    if messages is not None: metrics["pbft_message_count"]=messages
    method_payload={"display_name":method,"plugin_overrides":{},"plugin_config_overrides":{}}
    if method=="metatrack_ab_cons":
        method_payload["plugin_overrides"]["block_producer"]="time_or_count_block_producer" if configured else "metatrack_nl_window_v669"
        method_payload["plugin_config_overrides"]["block_producer"]={"dependency_closed_consensus": False if configured else True}
    return {"suite_type":"ablation_experiment","method_config_id":method,"method":method_payload,"seed":seed,"repeat_index":0,"estimated_transactions":10000,"topology_point":{"nodes":8,"shards":2,"validators_per_shard":4,"worker_count":8},"workload_point":{"tx_count":10000},"fault_point":{"mode":"disabled"},"block_size":1000,"block_interval_ms":100,"metrics":metrics}


def test_metatrack_v26_consensus_treatment_gate_uses_runtime_separation():
    full=_v26_consensus_child("metatrack_latest",aggregation_active=True,blocks=88,preprepare=264,messages=2424)
    ab=_v26_consensus_child("metatrack_ab_cons",blocks=202,preprepare=606,messages=5574)
    gated=_with_metatrack_consensus_treatment_gate([full,ab])
    item=next(x for x in gated if x["method_config_id"]=="metatrack_ab_cons")
    assert item["_metatrack_consensus_ablation_treatment_active"] is True
    assert item["_metatrack_consensus_ablation_treatment_mode"]=="one_signed_projection_per_pbft"
    assert item["_metatrack_consensus_ablation_treatment_evidence"]["observed_more_blocks"] is True
    assert item["_metatrack_consensus_ablation_treatment_evidence"]["observed_more_pbft_work"] is True
    assert "metatrack_consensus_ablation_treatment_inactive" not in _paper_exclusion_reasons(item, [])


def test_metatrack_v26_consensus_treatment_gate_fails_closed_on_wrong_config_or_no_separation():
    full=_v26_consensus_child("metatrack_latest",aggregation_active=True,blocks=88,preprepare=264,messages=2424)
    wrong=_v26_consensus_child("metatrack_ab_cons",blocks=202,preprepare=606,messages=5574,configured=False)
    same=_v26_consensus_child("metatrack_ab_cons",seed=12,blocks=88,preprepare=264,messages=2424)
    full12=_v26_consensus_child("metatrack_latest",seed=12,aggregation_active=True,blocks=88,preprepare=264,messages=2424)
    gated=_with_metatrack_consensus_treatment_gate([full,wrong,full12,same])
    by={(x["method_config_id"],x["seed"]):x for x in gated if x["method_config_id"]=="metatrack_ab_cons"}
    assert by[("metatrack_ab_cons",11)]["_metatrack_consensus_ablation_treatment_active"] is False
    assert by[("metatrack_ab_cons",12)]["_metatrack_consensus_ablation_treatment_active"] is False


def test_metatrack_v26_consensus_gate_no_longer_requires_max_route_batch_count_metric():
    full=_v26_consensus_child("metatrack_latest",aggregation_active=True,blocks=16,preprepare=48,messages=432)
    ab=_v26_consensus_child("metatrack_ab_cons",blocks=22,preprepare=66,messages=594)
    assert "metatrack_consensus_window_max_route_batch_count" not in ab["metrics"]
    gated=_with_metatrack_consensus_treatment_gate([full,ab])
    item=next(x for x in gated if x["method_config_id"]=="metatrack_ab_cons")
    assert item["_metatrack_consensus_ablation_treatment_active"] is True

