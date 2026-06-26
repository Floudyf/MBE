from pathlib import Path

import pytest

from backend.app.services.v3_profile_loader import (
    V3ProfileLoadError,
    load_profile_store,
    load_profiles_from_dir,
    profile_inventory,
)


def test_load_valid_chain_profile():
    store = load_profile_store()

    profile = store.chains["chain_x_default"]

    assert profile["profile_type"] == "chain_profile"
    assert profile["chain"]["chain_id"] == "chain_x"
    assert profile["capability"]["status"] == "planned"
    assert profile["capability"]["runnable"] is False


def test_load_valid_plugin_profiles_from_collection():
    store = load_profile_store()

    assert store.plugins["baseline_hash_only"]["plugins"]["ShardingPlugin"] == "hash_sharding"
    assert store.plugins["full_MetaTrack"]["plugins"]["CommitPlugin"] == "hot_update_aggregation_commit"
    assert store.plugins["metaflow_afs_fda"]["plugins"]["CrossChainProtocolPlugin"] == "metaflow_afs_fda"


def test_load_valid_experiment_profile():
    store = load_profile_store()

    profile = store.experiments["metatrack_ablation_profile_preview"]

    assert profile["profile_type"] == "experiment_profile"
    assert profile["chain_profile"] == "chain_x_default"
    assert "full_MetaTrack" in profile["plugin_profiles"]["proposed"]


def test_inventory_lists_all_profile_types():
    inventory = profile_inventory(load_profile_store())

    assert "chain_x_default" in inventory["chain_profiles"]
    assert "baseline_hash_only" in inventory["plugin_profiles"]
    assert "metaflow_dual_chain_profile_preview" in inventory["experiment_profiles"]


def test_loader_rejects_duplicate_profile_ids(tmp_path: Path):
    profiles_dir = tmp_path / "plugins"
    profiles_dir.mkdir()
    (profiles_dir / "a.yaml").write_text(
        """
profile_type: plugin_profile
plugin_profile_id: duplicate
domain: metatrack
status: planned
min_stage: v3.3
runnable: false
plugins:
  ShardingPlugin: hash_sharding
""".strip(),
        encoding="utf-8",
    )
    (profiles_dir / "b.yaml").write_text(
        """
profile_type: plugin_profile
plugin_profile_id: duplicate
domain: metatrack
status: planned
min_stage: v3.3
runnable: false
plugins:
  ShardingPlugin: co_access_sharding
""".strip(),
        encoding="utf-8",
    )

    profiles = load_profiles_from_dir(profiles_dir)

    from backend.app.services.v3_profile_loader import _index_profiles

    with pytest.raises(V3ProfileLoadError, match="duplicate profile id"):
        _index_profiles(profiles, "plugin_profile_id")
