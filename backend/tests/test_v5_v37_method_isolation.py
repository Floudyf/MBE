from backend.app.services.v5_formal_plan_validator import ALL_BUILTIN_METHODS, BUILTIN_METHODS, STATELESS_BUILTIN_METHODS, CALVIN_BUILTIN_METHODS, PORYGON_BUILTIN_METHODS, BATCH_SI_BUILTIN_METHODS
from backend.app.services.v5_plugin_manifest_store import STORE

# MBE_V37_METHOD_ISOLATION_REGRESSION

def test_v37_blockstm_fidelity_knobs_are_method_scoped() -> None:
    stateful = BUILTIN_METHODS["hash_block_stm"].plugin_config_overrides["block_executor"]
    assert stateful["scheduler_mode"] == "priority_heap_v1"
    assert stateful["dependency_wait_mode"] == "suspend_same_incarnation_v1"

    stateless = STATELESS_BUILTIN_METHODS["stateless_hash_block_stm"].plugin_config_overrides["block_executor"]
    assert stateless["scheduler_mode"] == "priority_heap_v1"
    assert stateless["dependency_wait_mode"] == "suspend_same_incarnation_v1"
    assert stateless["versioned_wave_policy"] == "maximal_compatible_exact_version_v2"

    # MetaTrack's compatibility Block-STM method deliberately keeps the previous
    # defaults when it exists in the current dirty-worktree registry.  Some
    # later local MetaTrack revisions intentionally remove this compatibility
    # method; v37 must preserve that pre-existing state rather than recreate it.
    mt_method = ALL_BUILTIN_METHODS.get("metatrack_block_stm")
    if mt_method is not None:
        mt = mt_method.plugin_config_overrides["block_executor"]
        assert "scheduler_mode" not in mt
        assert "dependency_wait_mode" not in mt
        assert "versioned_wave_policy" not in mt


def test_v37_stateless_serial_is_strictly_single_transaction_per_version_ready_wave() -> None:
    serial = STATELESS_BUILTIN_METHODS["stateless_hash_serial"].plugin_config_overrides["block_executor"]
    assert serial == {"worker_count": 1, "versioned_wave_policy": "strict_single_tx_block_order_v1"}
    assert BUILTIN_METHODS["hash_serial"].plugin_config_overrides["block_executor"] == {"worker_count": 1}


def test_v37_frozen_families_keep_their_executor_identity() -> None:
    assert CALVIN_BUILTIN_METHODS["stateful_calvin"].plugin_overrides["block_executor"] == "calvin_block_executor"
    assert CALVIN_BUILTIN_METHODS["stateless_calvin"].plugin_overrides["block_executor"] == "stateless_calvin_block_executor"
    assert PORYGON_BUILTIN_METHODS["stateless_porygon"].plugin_overrides["block_executor"] == "porygon_block_executor"
    assert BATCH_SI_BUILTIN_METHODS["hash_batch_si"].plugin_overrides["block_executor"] == "batch_si_block_executor"


def test_v37_groundhog_dead_scan_multiplier_is_not_emitted_by_formal_profile() -> None:
    producer = BUILTIN_METHODS["hash_groundhog"].plugin_config_overrides["block_producer"]
    assert producer == {"ordered_set_limit": 64}
    manifest = STORE.get("groundhog_block_producer")
    assert "candidate_scan_multiplier" not in manifest.default_config
    # Historical saved profiles remain schema-compatible, but the field is explicitly deprecated/ignored.
    prop = manifest.config_schema["properties"]["candidate_scan_multiplier"]
    assert prop["deprecated"] is True
