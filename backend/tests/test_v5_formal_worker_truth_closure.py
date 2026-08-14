from backend.app.api.v5_formal_experiments import _formal_experiment_profile


def test_formal_profile_preserves_worker_settings_contract_and_records_effective_truth():
    plan = {
        "worker_count": 8,
        "base_spec": {
            "tx_count": 10_000,
            "seed": 11,
            "topology": {"nodes": 8, "shards": 1, "validators_per_shard": 8},
            "workload_source": {"source_type": "dataset", "requested_tx_count": 10_000, "seed": 11},
            "plugin_selections": [
                {"category": "block_producer", "plugin_id": "time_or_count_block_producer", "config": {"block_size": 1000, "interval_ms": 400}},
            ],
        },
        "methods": [
            {
                "method_id": "hash_serial",
                "plugin_overrides": {"block_executor": "serial_block_executor"},
                "plugin_config_overrides": {"block_executor": {"worker_count": 1}},
            },
            {
                "method_id": "hash_batch_si",
                "plugin_overrides": {"block_executor": "batch_si_block_executor"},
                "plugin_config_overrides": {"block_executor": {"worker_count": 4, "partition_mode": "wrbp"}},
            },
        ],
        "seeds": [11],
        "repeats": 1,
        "suites": ["comparison_experiment"],
    }
    rows = [
        {"method_config_id": "hash_serial", "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8}, "runnable": True, "blockers": []},
        {"method_config_id": "hash_batch_si", "topology_point": {"nodes": 8, "shards": 1, "validators_per_shard": 8, "worker_count": 8}, "runnable": True, "blockers": []},
    ]
    profile = _formal_experiment_profile(plan, rows)

    # Backward-compatible v2 contract: existing consumers and regressions still
    # see the registered method configuration exactly as before.
    assert profile["worker_settings"]["hash_serial"] == {"worker_count": 1}
    assert profile["worker_settings"]["hash_batch_si"] == {"worker_count": 4, "partition_mode": "wrbp"}

    # Runtime truth is additive and unambiguous rather than mutating the legacy
    # worker_settings meaning.
    serial = profile["worker_execution_truth"]["hash_serial"]
    batch_si = profile["worker_execution_truth"]["hash_batch_si"]
    assert serial == {
        "registered_default_worker_count": 1,
        "requested_worker_count": 8,
        "effective_worker_count": 1,
        "effective_worker_counts": [1],
    }
    assert batch_si == {
        "registered_default_worker_count": 4,
        "requested_worker_count": 8,
        "effective_worker_count": 8,
        "effective_worker_counts": [8],
    }
