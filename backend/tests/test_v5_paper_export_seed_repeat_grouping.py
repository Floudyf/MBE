from backend.app.services.v5_paper_exporter import _group_key, _metric_rows_by_seed


def _child(seed: int, repeat: int, tps: float) -> dict:
    return {
        "child_run_id": f"c-{seed}-{repeat}",
        "suite_type": "comparison_experiment",
        "method_config_id": "hash_serial",
        "method": {"display_name": "Serial", "role": "baseline"},
        "method_role": "baseline",
        "seed": seed,
        "repeat_index": repeat,
        "scan_variable": "",
        "scan_value": "",
        "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 1},
        "workload_point": {"tx_count": 10000},
        "fault_point": {},
        "block_size": 1000,
        "block_interval_ms": 100,
        "metrics": {"end_to_end_tps": tps, "replay_mode": "max_throughput", "pacing_schedule": "none"},
    }


def test_group_key_keeps_seed_as_experimental_condition():
    a = _group_key(_child(11, 0, 100.0), {})
    b = _group_key(_child(22, 0, 100.0), {})
    assert a != b


def test_repeat_statistics_do_not_mix_seeds():
    rows = _metric_rows_by_seed([
        _child(11, 0, 100.0),
        _child(11, 1, 110.0),
        _child(22, 0, 200.0),
        _child(22, 1, 220.0),
    ], "end_to_end_tps", "tps")
    assert len(rows) == 2
    by_seed = {int(row["seed"]): row for row in rows}
    assert by_seed[11]["mean"] == 105.0
    assert by_seed[22]["mean"] == 210.0
    assert by_seed[11]["aggregation_scope"] == "same_seed_runtime_repeats"
    assert by_seed[22]["aggregation_scope"] == "same_seed_runtime_repeats"
