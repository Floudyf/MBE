from copy import deepcopy

from backend.app.services.v3_profile_loader import load_profile_store
from backend.app.services.v3_profile_preview import preview_profile
from backend.app.services.v3_profile_validator import normalized_chain_role_config, validate_chain_profile, validate_experiment_profile


def test_role_separated_chain_profile_normalizes_roles():
    store = load_profile_store()
    profile = store.chains["single_chain_research_default"]

    result = validate_chain_profile(profile)
    roles = normalized_chain_role_config(profile)

    assert result["valid"] is True
    assert roles["consensus"]["domain_count"] == 1
    assert roles["consensus"]["validator_count"] == 4
    assert roles["execution"]["shard_count"] == 4
    assert roles["state"]["storage_unit_count"] == 4
    assert roles["state"]["placement_policy"] == "hash_state_storage"
    assert roles["routing"]["plugin"] == "hash_sharding"
    assert roles["committee"]["enabled"] is False
    assert roles["committee"]["status"] == "planned"


def test_old_chain_profile_gets_role_defaults():
    profile = load_profile_store().chains["chain_x_default"]

    roles = normalized_chain_role_config(profile)

    assert roles["consensus"]["domain_ids"] == ["consensus_0"]
    assert roles["execution"]["shard_count"] == profile["sharding"]["shard_count"]
    assert roles["state"]["storage_unit_count"] == profile["sharding"]["shard_count"]
    assert roles["state"]["placement_policy"] == "hash_state_storage"
    assert roles["routing"]["routing_scope"] == "execution_shard"


def test_single_chain_role_separation_smoke_validates_and_previews_runnable():
    store = load_profile_store()

    result = validate_experiment_profile(store.experiments["single_chain_role_separation_smoke"], store)
    preview = preview_profile("experiment_profile", "single_chain_role_separation_smoke", store)

    assert result["valid"] is True
    assert result["runnable"] is True
    assert preview["role_summary"]["consensus_domain_count"] == 1
    assert preview["role_summary"]["execution_shard_count"] == 4
    assert preview["role_summary"]["state_storage_unit_count"] == 4
    assert preview["execution"]["starts_fabric"] is False
    assert preview["execution"]["calls_go_executor"] is False


def test_planned_committee_cannot_be_enabled():
    profile = deepcopy(load_profile_store().chains["single_chain_research_default"])
    profile["committee"]["enabled"] = True
    profile["committee"]["status"] = "planned"

    result = validate_chain_profile(profile)

    assert result["valid"] is False
    assert any("committee" in error for error in result["errors"])
