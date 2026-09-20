from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE


def test_porygon_builtin_method_is_registered_as_additive_baseline() -> None:
    method = ALL_BUILTIN_METHODS["stateless_porygon"]
    assert method.display_name == "Porygon"
    assert method.role == "baseline"
    assert method.plugin_overrides == {
        "routing": "porygon_stateless_routing",
        "block_producer": "porygon_transaction_block_producer",
        "execution": "porygon_execution",
        "scheduler": "porygon_pipeline_scheduler",
        "block_executor": "porygon_block_executor",
        "state_access": "porygon_remote_state_access",
        "cross_shard": "porygon_cross_shard_coordinator",
        "commit": "normal_commit",
    }
    assert method.plugin_config_overrides["scheduler"]["pipeline_enabled"] is True
    assert method.plugin_config_overrides["scheduler"]["cross_batch_witness"] is True


def test_porygon_manifests_are_real_cluster_plugins_with_paper_source() -> None:
    expected = {
        "porygon_stateless_routing": "routing",
        "porygon_transaction_block_producer": "block_producer",
        "porygon_execution": "execution",
        "porygon_pipeline_scheduler": "scheduler",
        "porygon_block_executor": "block_executor",
        "porygon_remote_state_access": "state_access",
        "porygon_cross_shard_coordinator": "cross_shard",
    }
    for plugin_id, category in expected.items():
        manifest = STORE.get(plugin_id)
        assert manifest.category == category
        assert manifest.supported_backends == ["real_cluster"]
        assert manifest.source.get("source_type") == "paper_reimplementation"
        assert manifest.source.get("doi") == "10.1109/ICDE60146.2024.00153"
        assert manifest.source.get("reproduction_dossier") == "docs/reproductions/porygon/source_lock.md"


def test_existing_builtin_method_payloads_remain_present() -> None:
    # Porygon is additive; these IDs must still resolve exactly as independent methods.
    for method_id in (
        "hash_serial",
        "hash_block_stm",
        "hash_aria",
        "hash_groundhog",
        "hash_cg",
        "hash_acg",
        "hash_bsx",
        "hash_batch_si",
        "metatrack_serial",
    ):
        assert method_id in ALL_BUILTIN_METHODS



def test_porygon_does_not_change_legacy_first_plugin_defaults() -> None:
    # Several historical backend helpers intentionally use the first manifest in
    # each category as the compatibility/default seed. Porygon must be additive
    # and therefore must never become first in an existing category.
    expected_first = {
        "workload": "deterministic_signed_synthetic",
        "transaction_admission": "signature_nonce_admission",
        "txpool": "fifo_per_node_mempool",
        "sharding": "deterministic_state_key_sharding",
        "routing": "hash_routing_baseline",
        "block_producer": "time_or_count_block_producer",
        "consensus": "pbft_style_consensus",
        "network": "localhost_tcp_typed_network",
        "execution": "serial_execution_baseline",
        "scheduler": "fifo_serial_scheduler",
        "block_executor": "serial_block_executor",
        "state_access": "direct_state_access",
        "state_storage": "persistent_local_state_store",
        "cross_shard": "relay_certificate_protocol",
        "commit": "normal_commit",
        "fault_injection": "faults_disabled",
        "metrics": "runtime_core_metrics",
        "observability": "node_network_consensus_observer",
    }
    first: dict[str, str] = {}
    for manifest in STORE.list():
        first.setdefault(manifest.category, manifest.plugin_id)
    assert first == expected_first


def test_porygon_manifests_are_appended_after_legacy_manifests() -> None:
    items = STORE.list()
    first_porygon = min(i for i, item in enumerate(items) if item.plugin_id.startswith("porygon_"))
    last_legacy = max(i for i, item in enumerate(items) if not item.plugin_id.startswith("porygon_"))
    assert first_porygon > last_legacy
