from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def _root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_txallo_manifest_uses_paper_g_ratio_snapshot_and_multilevel_louvain() -> None:
    sharding = STORE.get("txallo_account_sharding")
    assert sharding.default_config["allocation_mode"] == "paper_g_ratio_snapshot"
    assert "adaptive_chunk_records" not in sharding.default_config
    assert "deterministic_multilevel_louvain" in sharding.capabilities
    assert "paper_eq6_eq8_gain" in sharding.capabilities
    assert "a_txallo_core" in sharding.capabilities


def test_stateful_and_stateless_txallo_remain_plugin_compositions() -> None:
    stateful = ALL_BUILTIN_METHODS["stateful_txallo"]
    stateless = ALL_BUILTIN_METHODS["stateless_txallo"]
    assert stateful.plugin_overrides["sharding"] == stateless.plugin_overrides["sharding"] == "txallo_account_sharding"
    assert stateful.plugin_overrides["routing"] == "txallo_routing"
    assert stateless.plugin_overrides["routing"] == "stateless_txallo_routing"
    for method in (stateful, stateless):
        assert method.plugin_overrides["execution"] == "serial_execution_baseline"
        assert method.plugin_overrides["scheduler"] == "fifo_serial_scheduler"
        assert method.plugin_overrides["block_executor"] == "serial_block_executor"
        assert method.plugin_config_overrides["sharding"]["allocation_mode"] == "paper_g_ratio_snapshot"
        assert "adaptive_chunk_records" not in method.plugin_config_overrides["sharding"]


def test_txallo_core_and_bootstrap_have_v20_truth_boundaries() -> None:
    root = _root()
    core = (root / "executor/v5/txallo_core.go").read_text(encoding="utf-8")
    plugins = (root / "executor/v5/txallo_plugins.go").read_text(encoding="utf-8")
    runtime = (root / "executor/v5/runtime.go").read_text(encoding="utf-8")
    assert "MBE_TXALLO_PAPER_V20" in core
    assert "deterministicLouvainInitialization(g *txalloGraph) (map[string]int, int)" in core
    assert "Eq. (6)" in core and "Eq. (8)" in core
    assert "TxAllo incomplete mapping" in core
    assert "paper_g_ratio_snapshot" in plugins
    assert "adaptiveChunk" not in plugins
    assert 'configuredInt("adaptive_chunk_records"' not in plugins
    assert "dynamic_a_txallo_runtime_enabled" in plugins
    assert "node historical allocation bootstrap" in runtime
    assert "HistoricalAllocationBootstrapper" in runtime


def test_frontend_keeps_two_txallo_cards_as_plugin_compositions() -> None:
    root = _root()
    profile = (root / "frontend/src/v5MethodProfile.ts").read_text(encoding="utf-8")
    catalog = (root / "frontend/src/v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    analysis = (root / "frontend/src/components/v5/V5MechanismAnalysis.tsx").read_text(encoding="utf-8")
    assert profile.count('allocation_mode: "paper_g_ratio_snapshot"') == 2
    stateful = next(line for line in profile.splitlines() if 'method_id: "stateful_txallo"' in line)
    stateless = next(line for line in profile.splitlines() if 'method_id: "stateless_txallo"' in line)
    assert 'sharding: "txallo_account_sharding"' in stateful
    assert 'routing: "txallo_routing"' in stateful
    assert 'sharding: "txallo_account_sharding"' in stateless
    assert 'routing: "stateless_txallo_routing"' in stateless
    assert 'methodId: "stateful_txallo"' in catalog and 'methodId: "stateless_txallo"' in catalog
    assert "txallo_louvain_level_count" in analysis
    assert "txallo_mapping_complete" in analysis


def test_txallo_reproduction_docs_state_snapshot_and_runtime_identity() -> None:
    root = _root()
    source = (root / "docs/reproductions/txallo/source_lock.md").read_text(encoding="utf-8")
    mapping = (root / "docs/reproductions/txallo/mbe_mapping.md").read_text(encoding="utf-8")
    assert "paper-fidelity v20 closure" in source
    assert "paper_g_ratio_snapshot" in source
    assert "Node/runtime mapping identity" in mapping
    assert "PBFT is unchanged" in mapping


def test_v20_snapshot_fidelity_allows_paper_empty_louvain_shards() -> None:
    from backend.app.services.v5_optme_txallo_fidelity_closure import apply_group_fidelity_gate

    item = {
        "method_id": "stateful_txallo",
        "method": {
            "plugin_overrides": {
                "sharding": "txallo_account_sharding",
                "routing": "txallo_routing",
                "execution": "serial_execution_baseline",
                "scheduler": "fifo_serial_scheduler",
                "block_executor": "serial_block_executor",
            },
            "plugin_config_overrides": {
                "sharding": {"lambda": 0.0, "epsilon": 0.0, "allocation_mode": "paper_g_ratio_snapshot"},
                "block_executor": {"worker_count": 1},
            },
        },
        "topology_point": {"shards": 2, "worker_count": 1},
        "initial_state_digest": "i",
        "global_final_state_digest": "f",
        "state_home_mapping_digest": "h",
        "metrics": {
            "v16_logical_business_state_digest": "business",
            "logical_transaction_identity_digest": "workload",
            "v16_metric_coherence_passed": True,
            "v16_business_execution_timer_complete": True,
            "v16_effective_worker_count": 1,
            "v16_txallo_account_mapping_digest": "mapping",
            "v16_txallo_account_mapping_uniqueness_passed": True,
            "v16_txallo_account_mapping_completeness_passed": True,
            "v16_txallo_account_mapping_nonempty_shard_count": 1,
            "txallo_allocation_mode": "paper_g_ratio_snapshot",
            "txallo_history_transaction_count": 100,
            "txallo_lambda": 50.0,
            "txallo_eta": 2.0,
            "txallo_epsilon": 0.001,
            "txallo_g_txallo_run_count": 1,
            "txallo_a_txallo_run_count": 0,
            "txallo_mapping_complete": True,
            "txallo_louvain_level_count": 2,
        },
    }
    enriched, _ = apply_group_fidelity_gate([item], {})
    row = enriched[0]
    diag = row["v16_txallo_topology_diagnostics"]
    assert diag["allocation_mode"] == "paper_g_ratio_snapshot"
    assert diag["adaptive_history_accounting_consistent"] is True
    assert "txallo_account_mapping_uses_fewer_nonempty_shards_than_configured" not in row["v16_fidelity_blockers"]
    assert "txallo_paper_allows_empty_louvain_shards;fewer_nonempty_shards_observed" in row["v16_fidelity_warnings"]
