from pathlib import Path

from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_optme_v22_formal_cards_share_one_ordering_contract_and_only_state_substrate_differs() -> None:
    stateful = ALL_BUILTIN_METHODS["stateful_optme"].plugin_overrides
    stateless = ALL_BUILTIN_METHODS["stateless_optme"].plugin_overrides
    for key in ("transaction_admission", "txpool", "sharding", "block_producer", "consensus", "network", "execution", "scheduler", "state_access", "commit", "metrics", "observability"):
        assert stateful[key] == stateless[key]
    assert stateful["routing"] == "optme_global_routing"
    assert stateful["block_executor"] == "optme_block_executor"
    assert stateful["state_storage"] == "persistent_local_state_store"
    assert stateless["routing"] == "stateless_optme_routing"
    assert stateless["block_executor"] == "stateless_optme_block_executor"
    assert stateless["state_storage"] == "optme_partition_state_store"
    assert stateful["cross_shard"] == stateless["cross_shard"] == "optme_global_no_relay"


def test_optme_v22_client_does_not_sign_workload_order_as_stateless_state_versions() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "executor" / "v5" / "client.go").read_text(encoding="utf-8")
    assert "MBE_OPTME_V22_CLIENT_NO_SOURCE_VERSIONS" in source
    assert "plugins.Routing.ID() == calvinStatelessRoutingID || plugins.Routing.ID() == optmeStatelessRoutingID" in source


def test_optme_v22_runtime_exits_legacy_exact_version_transport_and_uses_block_projection() -> None:
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    projection = (root / "executor" / "v5" / "optme_v22_projection.go").read_text(encoding="utf-8")
    plugins = (root / "executor" / "v5" / "optme_plugins.go").read_text(encoding="utf-8")
    assert "case txalloStatelessRoutingID:" in runtime
    assert "case optmeStatelessRoutingID, txalloStatelessRoutingID:" not in runtime
    assert "prepareOptMEStatelessProjection" in runtime
    assert "optmePartitionOwnedStateDelta" in runtime
    assert "StatelessVersionAdmission() bool { return false }" in plugins
    assert "optme_v22_block_start:" in projection
    assert "committedHeight > requiredHeight" in projection
    assert "optme_projection_snapshot_unavailable" in projection


def test_optme_v22_manifests_do_not_claim_exact_version_transport_for_optme() -> None:
    routing = STORE.get("stateless_optme_routing")
    executor = STORE.get("stateless_optme_block_executor")
    assert "block_start_snapshot_projection" in routing.capabilities
    assert "no_transaction_version_admission" in routing.capabilities
    assert "exact_state_version_transport" not in routing.capabilities
    assert "partition_owned_materialization" in executor.capabilities
    assert "exact_state_version_transport" not in executor.capabilities
    assert "exact_state_version_transport" in STORE.get("stateless_txallo_routing").capabilities
    assert STORE.get("optme_global_routing").truth_boundary == "optme_single_global_ordering_domain_v22"
    assert STORE.get("optme_partition_state_store").truth_boundary == "optme_stateless_partition_owned_storage_v22"


def test_optme_v22_compiler_maps_paper_cards_to_optme_global_without_changing_pbft_plugin() -> None:
    root = Path(__file__).resolve().parents[2]
    compiler = (root / "backend" / "app" / "services" / "v5_experiment_compiler.py").read_text(encoding="utf-8")
    assert "MBE_OPTME_V22_GLOBAL_ORDER_DOMAIN" in compiler
    assert 'shard_id = "optme-global"' in compiler
    assert 'consensus_domain_id = "optme-global"' in compiler
    formal = ALL_BUILTIN_METHODS["stateless_optme"].plugin_overrides
    assert formal["consensus"] == "pbft_style_consensus"
