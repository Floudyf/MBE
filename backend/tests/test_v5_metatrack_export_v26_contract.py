from backend.app.services.v5_paper_exporter import _with_metatrack_consensus_treatment_gate, _paper_exclusion_reasons


def child(method, *, seed=11, aggregation=None, blocks=None, preprepare=None, messages=None, configured=True):
    metrics={}
    if aggregation is not None: metrics["metatrack_consensus_window_aggregation_active"]=aggregation
    if blocks is not None: metrics["actual_committed_block_count"]=blocks
    if preprepare is not None: metrics["pbft_preprepare_count"]=preprepare
    if messages is not None: metrics["pbft_message_count"]=messages
    method_payload={"display_name":method,"plugin_overrides":{},"plugin_config_overrides":{}}
    if method=="metatrack_ab_cons":
        method_payload["plugin_overrides"]["block_producer"]="time_or_count_block_producer" if configured else "metatrack_nl_window_v669"
        method_payload["plugin_config_overrides"]["block_producer"]={"dependency_closed_consensus": False if configured else True}
    return {"suite_type":"ablation_experiment","method_config_id":method,"method":method_payload,"seed":seed,"repeat_index":0,"estimated_transactions":10000,"topology_point":{"nodes":8,"shards":2,"validators_per_shard":4,"worker_count":8},"workload_point":{"tx_count":10000},"fault_point":{"mode":"disabled"},"block_size":1000,"block_interval_ms":100,"metrics":metrics}


def gated_ab(full, ab):
    rows=_with_metatrack_consensus_treatment_gate([full,ab])
    return next(x for x in rows if x["method_config_id"]=="metatrack_ab_cons")


def test_v26_actual_10000_shape_is_treatment_active():
    item=gated_ab(child("metatrack_latest",aggregation=True,blocks=88,preprepare=264,messages=2424),child("metatrack_ab_cons",blocks=202,preprepare=606,messages=5574))
    assert item["_metatrack_consensus_ablation_treatment_active"] is True
    assert item["_metatrack_consensus_ablation_treatment_mode"]=="one_signed_projection_per_pbft"
    assert "metatrack_consensus_ablation_treatment_inactive" not in _paper_exclusion_reasons(item, [])


def test_v26_actual_1000_shape_is_treatment_active_without_legacy_window_metric():
    ab=child("metatrack_ab_cons",blocks=22,preprepare=66,messages=594)
    assert "metatrack_consensus_window_max_route_batch_count" not in ab["metrics"]
    item=gated_ab(child("metatrack_latest",aggregation=True,blocks=16,preprepare=48,messages=432),ab)
    assert item["_metatrack_consensus_ablation_treatment_active"] is True


def test_v26_wrong_runtime_config_is_inactive():
    item=gated_ab(child("metatrack_latest",aggregation=True,blocks=88,preprepare=264,messages=2424),child("metatrack_ab_cons",blocks=202,preprepare=606,messages=5574,configured=False))
    assert item["_metatrack_consensus_ablation_treatment_active"] is False


def test_v26_missing_runtime_separation_is_inactive():
    item=gated_ab(child("metatrack_latest",aggregation=True,blocks=88,preprepare=264,messages=2424),child("metatrack_ab_cons",blocks=88,preprepare=264,messages=2424))
    assert item["_metatrack_consensus_ablation_treatment_active"] is False
