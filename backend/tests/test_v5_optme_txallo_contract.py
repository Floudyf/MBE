from pathlib import Path

from backend.app.models.v5_experiment_spec import V5ExperimentSpec, V5PluginSelection, V5Topology
from backend.app.services.v5_compatibility_engine import validate
from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import CATEGORIES, STORE
from backend.app.services.v5_experiment_compiler import TXALLO_CLIENT_ARTIFACTS, OPTME_TXALLO_STATE_HOME_EVIDENCE_ARTIFACTS
from backend.app.services.v5_metric_extractor import _apply_method_preserving_remote_transport_truth


def test_optme_txallo_methods_are_registered_with_exact_names():
    expected = {
        "stateful_optme": "OptME",
        "stateless_optme": "Stateless-OptME",
        "stateful_txallo": "TxAllo",
        "stateless_txallo": "Stateless-TxAllo",
    }
    for method_id, display in expected.items():
        assert ALL_BUILTIN_METHODS[method_id].display_name == display


def test_optme_source_and_stateless_boundary_are_explicit():
    execution = STORE.get("optme_execution")
    scheduler = STORE.get("optme_scheduler")
    stateless = STORE.get("stateless_optme_block_executor")
    assert execution.source["repository_commit"] == "4cac103bd98440670d71219dfa185b8516ea6512"
    assert "first_updater_wins" in scheduler.capabilities
    assert "stateless_projection" in stateless.capabilities
    assert "metatrack" not in stateless.truth_boundary.lower()


def test_txallo_is_account_allocation_not_metatrack_state_placement():
    sharding = STORE.get("txallo_account_sharding")
    routing = STORE.get("txallo_routing")
    stateless = STORE.get("stateless_txallo_routing")
    assert "txallo_account_graph" in sharding.capabilities
    assert "pre_evaluation_history_only" in sharding.capabilities
    assert "no_future_batch_training" in routing.capabilities
    assert "stateless_direct_execution" in stateless.capabilities
    assert sharding.source["arxiv"] == "2212.11584"


def test_method_pairs_share_algorithm_plugins_only_change_state_substrate():
    a = ALL_BUILTIN_METHODS["stateful_optme"]
    b = ALL_BUILTIN_METHODS["stateless_optme"]
    assert a.plugin_overrides["execution"] == b.plugin_overrides["execution"] == "optme_execution"
    assert a.plugin_overrides["scheduler"] == b.plugin_overrides["scheduler"] == "optme_scheduler"
    x = ALL_BUILTIN_METHODS["stateful_txallo"]
    y = ALL_BUILTIN_METHODS["stateless_txallo"]
    assert x.plugin_overrides["sharding"] == y.plugin_overrides["sharding"] == "txallo_account_sharding"
    assert x.plugin_overrides["execution"] == y.plugin_overrides["execution"] == "serial_execution_baseline"


def test_txallo_formal_artifact_contract_requires_all_client_evidence():
    assert TXALLO_CLIENT_ARTIFACTS == [
        "txallo_allocation_summary.json",
        "txallo_account_mapping.csv",
        "txallo_transaction_placement.csv",
    ]
    assert OPTME_TXALLO_STATE_HOME_EVIDENCE_ARTIFACTS == ["placement_plan.csv"]

def test_optme_txallo_manifests_preserve_porygon_append_order_contract():
    items = STORE.list()
    first_porygon = min(i for i, item in enumerate(items) if item.plugin_id.startswith("porygon_"))
    last_non_porygon = max(i for i, item in enumerate(items) if not item.plugin_id.startswith("porygon_"))
    assert first_porygon > last_non_porygon

def test_all_four_optme_txallo_cards_are_visible_in_method_comparison() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    expected = {
        "stateful_optme": "OptME",
        "stateless_optme": "Stateless-OptME",
        "stateful_txallo": "TxAllo",
        "stateless_txallo": "Stateless-TxAllo",
    }
    for method_id, title in expected.items():
        line = next(item for item in source.splitlines() if f'methodId: "{method_id}"' in item)
        assert f'title: "{title}"' in line
        assert 'comparisonVisible: true' in line

def _selections_for_builtin(method_id: str) -> list[V5PluginSelection]:
    method = ALL_BUILTIN_METHODS[method_id]
    out: list[V5PluginSelection] = []
    for category in CATEGORIES:
        default = next(item for item in STORE.list() if item.category == category)
        plugin_id = method.plugin_overrides.get(category, default.plugin_id)
        manifest = STORE.get(plugin_id)
        config = {**manifest.default_config, **method.plugin_config_overrides.get(category, {})}
        out.append(V5PluginSelection(category=category, plugin_id=plugin_id, config=config))
    workload = next(item for item in out if item.category == "workload")
    workload.plugin_id = "deterministic_signed_synthetic"
    workload.config = {"cross_shard_ratio": 0.0, "timeout_every": 0}
    return out


def test_stateful_optme_accepts_common_mbe_physical_shard_counts() -> None:
    for shards in (1, 2, 4, 8):
        spec = V5ExperimentSpec(
            name=f"optme-stateful-{shards}-shards",
            execution_backend="real_cluster",
            plugin_selections=_selections_for_builtin("stateful_optme"),
            topology=V5Topology(nodes=8, shards=shards, validators_per_shard=max(1, 8 // shards)),
            tx_count=20, seed=1, duration_ms=9000,
        )
        result = validate(spec)
        assert result.valid, result.blockers
        assert not any("requires exactly 1 physical PBFT shard" in item for item in result.blockers)


# MBE_OPTME_V22_TEST_CONTRACT
def test_optme_topology_truth_boundary_is_one_global_ordering_domain() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    line = next(item for item in source.splitlines() if 'methodId: "stateful_optme"' in item)
    assert "optme-global PBFT 排序域" in line
    assert "逻辑状态分区数" in line



def test_stateless_optme_exits_source_order_exact_versions_while_txallo_keeps_them() -> None:
    root = Path(__file__).resolve().parents[2]
    optme = (root / "executor" / "v5" / "optme_plugins.go").read_text(encoding="utf-8")
    txallo = (root / "executor" / "v5" / "txallo_plugins.go").read_text(encoding="utf-8")
    runtime = (root / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    client = (root / "executor" / "v5" / "client.go").read_text(encoding="utf-8")
    assert "StatelessVersionAdmission() bool { return false }" in optme
    assert "StatelessVersionAdmission() bool { return p.stateless }" in txallo
    assert "case txalloStatelessRoutingID:" in runtime
    assert "case optmeStatelessRoutingID, txalloStatelessRoutingID:" not in runtime
    assert "plugins.Routing.ID() == calvinStatelessRoutingID || plugins.Routing.ID() == optmeStatelessRoutingID" in client
    assert "prepareOptMEStatelessProjection" in runtime


def test_txallo_stateful_is_no_longer_custom_unknown_semantics_fallback() -> None:
    routing = STORE.get("txallo_routing")
    assert "stateful_local_execution" in routing.capabilities



def test_optme_txallo_client_emits_standard_state_home_evidence_without_metatrack_artifact_relabeling() -> None:
    root = Path(__file__).resolve().parents[2]
    client = (root / "executor" / "v5" / "client.go").read_text(encoding="utf-8")
    for token in (
        "optmeTxAlloNeedsStateHomeEvidence",
        "optmeTxAlloStateHomeEvidenceRows",
        'filepath.Join(outDir, "placement_plan.csv")',
        '"execution_shard_local_namespace"',
        '"deterministic_persistent_state_home"',
    ):
        assert token in client
    # TxAllo keeps its own artifact family; placement_plan is correctness evidence,
    # not a MetaTrack routing artifact.
    txallo = (root / "executor" / "v5" / "txallo_plugins.go").read_text(encoding="utf-8")
    assert 'BatchRoutingArtifactFamily() string { return "txallo" }' in txallo


def test_stateless_optme_manifest_uses_block_projection_while_txallo_keeps_exact_versions() -> None:
    optme_routing = STORE.get("stateless_optme_routing")
    optme_executor = STORE.get("stateless_optme_block_executor")
    txallo_routing = STORE.get("stateless_txallo_routing")
    assert "block_start_snapshot_projection" in optme_routing.capabilities
    assert "no_transaction_version_admission" in optme_routing.capabilities
    assert "exact_state_version_transport" not in optme_routing.capabilities
    assert "partition_owned_materialization" in optme_executor.capabilities
    assert "preserve_optme_scheduler" in optme_executor.capabilities
    assert "exact_state_version_transport" in txallo_routing.capabilities
    assert "preserve_txallo_fifo_scheduler" in txallo_routing.capabilities



def test_v10_method_preserving_writebehind_is_isolated_from_other_algorithms() -> None:
    root = Path(__file__).resolve().parents[2]
    runtime = (root / "executor" / "v5" / "runtime.go").read_text(encoding="utf-8")
    extractor = (root / "backend" / "app" / "services" / "v5_metric_extractor.py").read_text(encoding="utf-8")
    for token in (
        'methodPreservingVersionedRemoteHomeOrigin = "optme_txallo_versioned_remote_home_v10"',
        "method_preserving_source_local_version_admission_hit_count",
        "method_preserving_source_local_version_fetch_count",
        "method_preserving_source_local_version_cache_publish_count",
        "method_preserving_home_version_committed_publish_count",
        "request.ApplyOrigin != methodPreservingVersionedRemoteHomeOrigin",
        "publishReadyMethodPreservingHomeVersions",
        "method_preserving_home_version_receipt_publish_count",
        "request.PreviousVersion != currentVersion",
    ):
        assert token in runtime
    # Existing MetaTrack/legacy origin remains present and is not renamed to the
    # OptME/TxAllo-specific v10 origin.
    assert 'applyOrigin := "versioned_remote_home"' in runtime
    assert "case txalloStatelessRoutingID:" in runtime
    for metric in (
        "method_preserving_source_local_version_admission_hit_count",
        "method_preserving_home_version_wait_count",
        "method_preserving_source_local_version_fetch_count",
        "method_preserving_source_local_version_cache_publish_count",
        "method_preserving_home_version_receipt_publish_count",
        "method_preserving_home_version_committed_publish_count",
    ):
        assert metric in extractor

def test_v14_method_preserving_network_truth_overrides_legacy_zero_remote_metrics() -> None:
    metrics = {
        "physical_remote_fetch_count": 0,
        "physical_remote_writeback_count": 0,
        "network_message_types": {
            "V5_STATE_FETCH_REQUEST": {"message_count": 101},
            "V5_STATE_FETCH_RESPONSE": {"message_count": 97},
            "V5_STATE_DELTA_APPLY": {"message_count": 43},
            "V5_STATE_DELTA_APPLY_ACK": {"message_count": 43},
        },
    }
    _apply_method_preserving_remote_transport_truth(metrics, "stateless_txallo")
    assert metrics["physical_remote_fetch_count"] == 101
    assert metrics["physical_remote_writeback_count"] == 43
    assert metrics["physical_remote_operation_count"] == 144
    assert metrics["remote_state_fetch_count"] == 101
    assert metrics["remote_state_fetch_completed_count"] == 97
    assert metrics["method_preserving_artifact_physical_remote_fetch_count"] == 0
    assert "source_local_exact_version_hits_excluded" in metrics["method_preserving_remote_transport_truth_scope"]


def test_v14_method_preserving_network_truth_is_isolated_from_other_methods() -> None:
    metrics = {
        "physical_remote_fetch_count": 7,
        "network_message_types": {"V5_STATE_FETCH_REQUEST": {"message_count": 101}},
    }
    _apply_method_preserving_remote_transport_truth(metrics, "metatrack")
    assert metrics["physical_remote_fetch_count"] == 7
    assert "method_preserving_physical_remote_fetch_request_count" not in metrics

# MBE_OPTME_V22_6_STATEFUL_CARD_CONTRACT
def test_optme_frontend_catalog_contains_live_stateful_and_stateless_cards() -> None:
    root = Path(__file__).resolve().parents[2]
    source = (root / "frontend" / "src" / "v5FormalExperimentCatalog.ts").read_text(encoding="utf-8")
    assert r'// MBE_OPTME_V22_CATALOG\n  { methodId: "stateful_optme"' not in source
    stateful = [line for line in source.splitlines() if 'methodId: "stateful_optme"' in line]
    stateless = [line for line in source.splitlines() if 'methodId: "stateless_optme"' in line]
    assert len(stateful) == 1
    assert len(stateless) == 1
    assert stateful[0].lstrip().startswith('{ methodId: "stateful_optme"')
    assert stateless[0].lstrip().startswith('{ methodId: "stateless_optme"')
    assert 'family: "stateful"' in stateful[0]
    assert 'comparisonVisible: true' in stateful[0]
    assert 'mainVisible: true' in stateful[0]
