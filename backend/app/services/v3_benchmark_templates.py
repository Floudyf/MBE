from __future__ import annotations

from typing import Any


BENCHMARK_TEMPLATES: list[dict[str, Any]] = [
    {
        "template_id": "metatrack_hotspot_template",
        "description": "Hotspot workload template for MetaTrack routing, access-list/prefetch, and aggregation experiments.",
        "workload_profile": "asset_hotspot",
        "topology": "logical_node_topology_default",
        "consensus_runtime": "simple_leader",
        "network_adapter": "in_memory_message_bus",
        "cross_shard_protocol": "none",
        "state_backend": "memory_kv",
        "baseline_candidates": ["baseline_hash_sharding", "baseline_no_prefetch"],
        "sweep_parameters": ["tx_count", "hotspot_ratio", "hot_key_count", "seed"],
        "required_artifacts": ["summary.json", "routing_log.csv", "state_access_log.csv"],
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "template_id": "pbft_network_template",
        "description": "ConsensusRuntime / PBFT preview over NetworkAdapter benchmark template.",
        "workload_profile": "asset_hotspot",
        "topology": "logical_node_topology_default",
        "consensus_runtime": "blockemulator_aligned_pbft_preview",
        "network_adapter": "localhost_tcp_preview",
        "cross_shard_protocol": "none",
        "state_backend": "memory_kv",
        "baseline_candidates": ["baseline_simple_chain"],
        "sweep_parameters": ["shard_count", "network_adapter", "consensus_runtime", "seed"],
        "required_artifacts": ["pbft_message_log.csv", "consensus_network_log.csv"],
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "template_id": "cross_shard_relay_preview_template",
        "description": "CrossShardProtocol relay_preview skeleton observation template.",
        "workload_profile": "asset_hotspot",
        "topology": "logical_node_topology_default",
        "consensus_runtime": "simple_leader",
        "network_adapter": "in_memory_message_bus",
        "cross_shard_protocol": "relay_preview",
        "state_backend": "memory_kv",
        "baseline_candidates": ["baseline_no_cross_shard_protocol"],
        "sweep_parameters": ["shard_count", "cross_shard_protocol", "seed"],
        "required_artifacts": ["cross_shard_tx_log.csv", "relay_preview_log.csv"],
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "template_id": "state_authenticity_template",
        "description": "State authenticity template for persistent_kv / merkle_trie_mvp proof and witness artifacts.",
        "workload_profile": "asset_hotspot",
        "topology": "logical_node_topology_default",
        "consensus_runtime": "simple_leader",
        "network_adapter": "in_memory_message_bus",
        "cross_shard_protocol": "none",
        "state_backend": "merkle_trie_mvp",
        "baseline_candidates": ["baseline_memory_kv", "baseline_no_state_authenticity"],
        "sweep_parameters": ["state_backend", "tx_count", "seed"],
        "required_artifacts": ["state_root_log.csv", "state_proof_verification_log.csv", "witness_log.csv"],
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
    {
        "template_id": "full_stack_v3_template",
        "description": "Full-stack V3 local controlled smoke benchmark template combining V3.5-V3.9 capabilities.",
        "workload_profile": "asset_hotspot",
        "topology": "logical_node_topology_default",
        "consensus_runtime": "blockemulator_aligned_pbft_preview",
        "network_adapter": "localhost_tcp_preview",
        "cross_shard_protocol": "relay_preview",
        "state_backend": "merkle_trie_mvp",
        "baseline_candidates": ["baseline_simple_chain", "baseline_memory_kv", "baseline_no_cross_shard_protocol"],
        "sweep_parameters": ["tx_count", "shard_count", "network_adapter", "consensus_runtime", "cross_shard_protocol", "state_backend", "seed"],
        "required_artifacts": ["benchmark_report.md", "reproducibility_manifest.json", "benchmark_summary.json"],
        "truth_boundary": "benchmark_template_hardening_not_paper_grade_benchmark",
    },
]


def list_benchmark_templates() -> list[dict[str, Any]]:
    return [dict(item) for item in BENCHMARK_TEMPLATES]


def benchmark_template_ids() -> set[str]:
    return {str(item["template_id"]) for item in BENCHMARK_TEMPLATES}
