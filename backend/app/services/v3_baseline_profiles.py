from __future__ import annotations

from typing import Any


BASELINE_PROFILES: list[dict[str, Any]] = [
    {
        "baseline_id": "baseline_simple_chain",
        "description": "Simple-chain baseline against the full V3 modular runtime.",
        "disabled_features": ["node_topology_preview", "network_adapter_preview", "cross_shard_protocol", "state_authenticity"],
        "enabled_features": ["memory_kv", "simple_leader"],
        "comparison_target": "full_stack_v3_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "baseline_id": "baseline_hash_sharding",
        "description": "Hash-sharding baseline against coaccess / hotspot-aware routing.",
        "disabled_features": ["metatrack_coaccess_routing", "hotspot_aware_routing"],
        "enabled_features": ["hash_sharding"],
        "comparison_target": "metatrack_hotspot_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "baseline_id": "baseline_no_prefetch",
        "description": "No-prefetch baseline against access_list_prefetch / cached_state_access.",
        "disabled_features": ["access_list_prefetch", "cached_state_access"],
        "enabled_features": ["direct_fetch"],
        "comparison_target": "metatrack_hotspot_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "baseline_id": "baseline_no_cross_shard_protocol",
        "description": "No-cross-shard-protocol baseline against relay_preview skeleton.",
        "disabled_features": ["relay_preview"],
        "enabled_features": ["none"],
        "comparison_target": "cross_shard_relay_preview_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "baseline_id": "baseline_memory_kv",
        "description": "Memory KV baseline against persistent_kv / merkle_trie_mvp.",
        "disabled_features": ["persistent_kv", "merkle_trie_mvp"],
        "enabled_features": ["memory_kv"],
        "comparison_target": "state_authenticity_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "baseline_id": "baseline_no_state_authenticity",
        "description": "Baseline without state root/proof/witness artifacts against V3.9 state authenticity outputs.",
        "disabled_features": ["state_root", "proof_generation", "witness_artifacts"],
        "enabled_features": ["memory_kv"],
        "comparison_target": "state_authenticity_template",
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
]


def list_baseline_profiles() -> list[dict[str, Any]]:
    return [dict(item) for item in BASELINE_PROFILES]


def baseline_profile_ids() -> set[str]:
    return {str(item["baseline_id"]) for item in BASELINE_PROFILES}
