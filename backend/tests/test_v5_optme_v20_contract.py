import json
from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_optme_txallo_paperfaithful_v17 import compute_optme_transaction_evidence_v17


EXPECTED_COMMON = {
    "transaction_admission": "signature_nonce_admission",
    "txpool": "fifo_per_node_mempool",
    "sharding": "deterministic_state_key_sharding",
    "block_producer": "time_or_count_block_producer",
    "consensus": "pbft_style_consensus",
    "network": "localhost_tcp_typed_network",
    "execution": "optme_execution",
    "scheduler": "optme_scheduler",
    "state_access": "direct_state_access",
    "commit": "normal_commit",
    "metrics": "runtime_core_metrics",
    "observability": "node_network_consensus_observer",
}


def test_optme_v20_profiles_are_complete_plugin_compositions() -> None:
    stateful = ALL_BUILTIN_METHODS["stateful_optme"].plugin_overrides
    stateless = ALL_BUILTIN_METHODS["stateless_optme"].plugin_overrides
    for key, value in EXPECTED_COMMON.items():
        assert stateful[key] == value
        assert stateless[key] == value
    assert stateful["routing"] == "optme_global_routing"
    assert stateful["state_storage"] == "persistent_local_state_store"
    assert stateful["cross_shard"] == "optme_global_no_relay"
    assert stateful["block_executor"] == "optme_block_executor"
    assert stateless["routing"] == "stateless_optme_routing"
    assert stateless["state_storage"] == "optme_partition_state_store"
    assert stateless["cross_shard"] == "optme_global_no_relay"
    assert stateless["block_executor"] == "stateless_optme_block_executor"


def test_optme_v20_frontend_profile_matches_backend_contract() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5MethodProfile.ts").read_text(encoding="utf-8")
    for token in (
        "MBE_OPTME_V20_PLUGIN_PROFILE",
        'transaction_admission: "signature_nonce_admission"',
        'block_producer: "time_or_count_block_producer"',
        'consensus: "pbft_style_consensus"',
        'metrics: "runtime_core_metrics"',
        'observability: "node_network_consensus_observer"',
    ):
        assert token in source


def test_optme_v20_executor_filters_failed_simulation_and_emits_tx_truth() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "executor" / "v5" / "optme_executor.go").read_text(encoding="utf-8")
    for token in (
        "successfulDeltas",
        "author_source_simulation_filtered_mbe_terminal_failure",
        "provider.BuildObservedSchedule(observed, workers)",
        '"optme_transaction_evidence"',
        '"optme_early_detection_count"',
        '"optme_hierarchical_abort_count"',
        '"optme_rescheduled_transaction_count"',
    ):
        assert token in source


def test_optme_v20_tx_evidence_deduplicates_pbft_replicas(tmp_path: Path) -> None:
    row1 = {
        "tx_id": "physical-a", "logical_tx_id": "L1", "original_index": 0,
        "simulation_success": True, "early_detected": True, "hierarchical_aborted": False,
        "reordered": False, "main_sequence": 0, "reschedule_epoch": 1,
        "reexecuted": True, "reexecution_success": True, "second_pass_invalidated": False,
        "terminal_success": True,
    }
    row2 = {
        "tx_id": "physical-b", "logical_tx_id": "L2", "original_index": 1,
        "simulation_success": True, "early_detected": False, "hierarchical_aborted": True,
        "reordered": True, "main_sequence": 2, "reschedule_epoch": 0,
        "reexecuted": False, "reexecution_success": False, "second_pass_invalidated": False,
        "terminal_success": True,
    }
    for node in ("n0", "n1"):
        path = tmp_path / "nodes" / node / "block_execution_summary.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "block_executor_id": "optme_block_executor",
            "blocks": [{"optme_transaction_evidence": [row1, row2]}],
        }), encoding="utf-8")
    got = compute_optme_transaction_evidence_v17(tmp_path, {})
    assert got["status"] == "available"
    assert got["logical_transaction_count"] == 2
    assert got["logical_early_detection_count"] == 1
    assert got["logical_hierarchical_abort_count"] == 1
    assert got["logical_rescheduled_count"] == 1
    assert got["logical_reexecution_count"] == 1
    assert got["logical_reordered_count"] == 1
