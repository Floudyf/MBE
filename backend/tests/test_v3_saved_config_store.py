from __future__ import annotations

from pathlib import Path

from backend.app.models.v3_saved_config import V3SavedConfigCreateRequest, V3SavedConfigUpdateRequest
from backend.app.services.v3_saved_config_store import create_saved_config, delete_saved_config, get_saved_config, list_saved_configs, update_saved_config


def test_saved_config_crud(tmp_path: Path) -> None:
    created = create_saved_config(
        V3SavedConfigCreateRequest(
            config_kind="method",
            name="MetaTrack full method",
            description="test method",
            tags=["metatrack", "method"],
            payload={"modules": {"Routing": {"plugin": "metatrack_coaccess_routing"}}, "topology": {"network_adapter": "localhost_tcp_preview"}},
            validation_status="runnable",
            last_smoke_run_id="run_1",
        ),
        root=tmp_path,
    )

    assert created["config_id"].startswith("v3cfg_")
    assert created["truth_boundary"] == "local_emulator_config_not_production_chain"
    assert list_saved_configs(root=tmp_path)[0]["name"] == "MetaTrack full method"
    assert get_saved_config(created["config_id"], root=tmp_path)["validation_status"] == "runnable"

    updated = update_saved_config(created["config_id"], V3SavedConfigUpdateRequest(description="updated", tags=["formal"]), root=tmp_path)
    assert updated["description"] == "updated"
    assert updated["version"] == 2

    deleted = delete_saved_config(created["config_id"], root=tmp_path)
    assert deleted["deleted"] is True
    assert list_saved_configs(root=tmp_path) == []


def test_saved_workload_and_topology_payloads(tmp_path: Path) -> None:
    workload = create_saved_config(
        V3SavedConfigCreateRequest(
            config_kind="workload",
            name="scene hotspot workload",
            payload={"workload_source": "metaverse", "metaverse_scenario": "scene_hotspot", "hotspot_ratio": 0.8},
        ),
        root=tmp_path,
    )
    topology = create_saved_config(
        V3SavedConfigCreateRequest(
            config_kind="topology",
            name="local tcp merkle relay",
            payload={"topology": {"network_adapter": "localhost_tcp_preview", "cross_shard_protocol": "relay_mvp", "state_backend": "merkle_trie_mvp"}},
        ),
        root=tmp_path,
    )

    assert workload["config_kind"] == "workload"
    assert topology["payload"]["topology"]["state_backend"] == "merkle_trie_mvp"
    assert len(list_saved_configs(kind="workload", root=tmp_path)) == 1
