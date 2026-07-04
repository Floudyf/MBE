from __future__ import annotations

from typing import Any

from backend.app.services.v3_composer_catalog import CATALOG


BASE_MODULES: dict[str, str] = {
    "Workload": "synthetic_hotspot",
    "TxPool": "fifo_pool",
    "BlockProducer": "time_or_count_block_producer",
    "Consensus": "simple_leader",
    "CommitteeEpoch": "disabled",
    "StateStorage": "hash_state_storage",
    "MetricsReport": "basic_metrics",
}

METATRACK_FORMAL_BASELINES: dict[str, dict[str, Any]] = {
    "baseline_hash_serial": {
        "label": "Hash + serial execution",
        "plugins": {
            **BASE_MODULES,
            "Routing": "hash_sharding",
            "Execution": "serial_execution",
            "StateAccess": "direct_fetch",
            "Commit": "normal_commit",
        },
    },
    "baseline_hash_prefetch": {
        "label": "Hash + access-list prefetch",
        "plugins": {
            **BASE_MODULES,
            "Routing": "hash_sharding",
            "Execution": "serial_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "normal_commit",
        },
    },
    "baseline_hash_dual_track": {
        "label": "Hash + dual-track execution",
        "plugins": {
            **BASE_MODULES,
            "Routing": "hash_sharding",
            "Execution": "metatrack_dual_track_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "normal_commit",
        },
    },
    "baseline_hash_aggregation": {
        "label": "Hash + constrained aggregation",
        "plugins": {
            **BASE_MODULES,
            "Routing": "hash_sharding",
            "Execution": "metatrack_dual_track_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "constraint_checked_aggregation",
        },
    },
    "metatrack_full": {
        "label": "MetaTrack full mechanism set",
        "plugins": {
            **BASE_MODULES,
            "Routing": "metatrack_coaccess_routing",
            "Execution": "metatrack_dual_track_execution",
            "StateAccess": "access_list_prefetch",
            "Commit": "constraint_checked_aggregation",
        },
    },
}


def list_formal_baselines() -> list[dict[str, Any]]:
    validate_formal_baseline_registry()
    return [
        {"baseline_id": baseline_id, **definition}
        for baseline_id, definition in METATRACK_FORMAL_BASELINES.items()
    ]


def get_formal_baseline(baseline_id: str) -> dict[str, Any]:
    validate_formal_baseline_registry()
    if baseline_id not in METATRACK_FORMAL_BASELINES:
        raise KeyError(f"unknown formal baseline: {baseline_id}")
    return METATRACK_FORMAL_BASELINES[baseline_id]


def validate_formal_baseline_registry() -> None:
    for baseline_id, definition in METATRACK_FORMAL_BASELINES.items():
        plugins = definition.get("plugins", {})
        missing = sorted(set(CATALOG) - set(plugins))
        if missing:
            raise ValueError(f"{baseline_id} missing module plugins: {missing}")
        for module_id, plugin_id in plugins.items():
            module = CATALOG.get(module_id)
            if module is None:
                raise ValueError(f"{baseline_id} references unknown module {module_id}")
            capability = module.plugins.get(plugin_id)
            if capability is None:
                raise ValueError(f"{baseline_id} references unknown plugin {plugin_id} for {module_id}")
            if not capability.runnable or capability.preview_only or capability.planned:
                raise ValueError(f"{baseline_id} uses non-runnable plugin {plugin_id} for {module_id}")
