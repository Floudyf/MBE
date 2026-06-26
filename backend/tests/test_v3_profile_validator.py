from copy import deepcopy

from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_validator import (
    validate_chain_profile,
    validate_experiment_profile,
    validate_plugin_profile,
)


def test_chain_profile_planned_modular_runtime_is_valid_not_runnable():
    profile = load_profile_store().chains["chain_x_default"]

    result = validate_chain_profile(profile)

    assert result["valid"] is True
    assert result["status"] == "planned"
    assert result["runnable"] is False
    assert result["valid"] is True


def test_fabric_validation_profile_is_planned_not_runnable():
    store = load_profile_store()

    chain_result = validate_chain_profile(store.chains["fabric_validation_planned"])
    experiment_result = validate_experiment_profile(store.experiments["fabric_validation_profile_preview"], store)

    assert chain_result["valid"] is True
    assert chain_result["runnable"] is False
    assert experiment_result["valid"] is True
    assert experiment_result["runnable"] is False


def test_reject_missing_required_chain_field():
    profile = deepcopy(load_profile_store().chains["chain_x_default"])
    del profile["block"]["block_interval_ms"]

    result = validate_chain_profile(profile)

    assert result["valid"] is False
    assert "block.block_interval_ms" in " ".join(result["errors"])


def test_reject_chain_profile_result_like_fields():
    profile = deepcopy(load_profile_store().chains["chain_x_default"])
    profile["metrics"]["max_tps"] = 100000

    result = validate_chain_profile(profile)

    assert result["valid"] is False
    assert "result-like field" in " ".join(result["errors"])


def test_reject_unknown_plugin_id():
    profile = deepcopy(load_profile_store().plugins["baseline_hash_only"])
    profile["plugins"]["ShardingPlugin"] = "magic_sharding"

    result = validate_plugin_profile(profile)

    assert result["valid"] is False
    assert "unknown plugin id ShardingPlugin:magic_sharding" in result["errors"]


def test_planned_capability_declared_runnable_becomes_invalid():
    profile = deepcopy(load_profile_store().chains["chain_x_default"])
    profile["capability"]["runnable"] = True

    result = validate_chain_profile(profile)

    assert result["valid"] is False
    assert result["status"] == "invalid"
    assert any("planned profile must not be runnable" in error for error in result["errors"])


def test_committee_bridge_basic_is_not_production_bridge():
    profile = load_profile_store().plugins["committee_baseline"]

    result = validate_plugin_profile(profile)

    assert result["valid"] is True
    assert "not a production bridge" in profile["description"].lower()


def test_committee_bridge_basic_rejects_production_bridge_description():
    profile = deepcopy(load_profile_store().plugins["committee_baseline"])
    profile["description"] = "Production bridge using committee_bridge_basic."

    result = validate_plugin_profile(profile)

    assert result["valid"] is False
    assert any("production bridge" in error for error in result["errors"])


def test_public_chain_imported_trace_semantic_unknown_remains_not_live_runnable():
    profile = deepcopy(load_profile_store().experiments["fabric_validation_profile_preview"])
    profile["profile_id"] = "public_chain_live_bad_preview"
    profile["experiment"]["truth_label"] = "public_chain_imported_trace_semantic_unknown"
    profile["experiment"]["backend_type"] = "evm_live_planned"
    profile["experiment"]["runnable"] = True
    profile["capability"]["backend_type"] = "evm_live_planned"
    profile["capability"]["runnable"] = True

    result = validate_experiment_profile(profile, load_profile_store())

    assert result["valid"] is False
    assert "only V3.2 smoke and V3.3 Go-backed MetaTrack smoke may be declared runnable" in result["errors"]
    assert any("evm_live_planned" in reason for reason in result["blocking_reasons"])
