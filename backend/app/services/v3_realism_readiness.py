from __future__ import annotations

import json
from pathlib import Path
from typing import Any


MODULE_READINESS: list[dict[str, Any]] = [
    {
        "module_id": "Workload",
        "runtime_status": "runnable",
        "realism_level": "runtime_realized",
        "implemented_plugins": ["synthetic_hotspot"],
        "artifact_logs": ["generated_experiment_profile.json", "summary.json"],
        "light_model_limitations": ["Synthetic local workload, not imported live-chain traffic."],
        "missing_for_real_emulator": ["Trace provenance controls", "network arrival realism"],
        "next_step": "Keep workload fixed for controlled smoke comparisons.",
    },
    {
        "module_id": "TxPool",
        "runtime_status": "runnable",
        "realism_level": "runtime_realized",
        "implemented_plugins": ["fifo_pool"],
        "artifact_logs": ["txpool_log.csv"],
        "light_model_limitations": ["Local FIFO queue, not a network mempool."],
        "missing_for_real_emulator": ["Peer gossip", "propagation delay", "multi-node admission"],
        "next_step": "Add more policies only after fairness controls stay stable.",
    },
    {
        "module_id": "BlockProducer",
        "runtime_status": "runnable",
        "realism_level": "runtime_realized",
        "implemented_plugins": ["time_or_count_block_producer"],
        "artifact_logs": ["block_log.csv"],
        "light_model_limitations": ["Virtual-time producer, not a proposer network."],
        "missing_for_real_emulator": ["Multi-proposer scheduling", "networked proposal propagation"],
        "next_step": "Keep time/count producer fixed in MetaTrack controlled smoke.",
    },
    {
        "module_id": "Consensus",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["simple_leader", "poa_light", "pbft_light_model"],
        "artifact_logs": ["consensus_log.csv"],
        "light_model_limitations": ["PBFT-light models stages and message counts only."],
        "missing_for_real_emulator": ["Real PBFT/HotStuff/Raft", "TCP messages", "fault tolerance"],
        "next_step": "Keep simple_leader fixed for MetaTrack ablation fairness.",
    },
    {
        "module_id": "CommitteeEpoch",
        "runtime_status": "planned",
        "realism_level": "config_only",
        "implemented_plugins": ["disabled"],
        "artifact_logs": [],
        "light_model_limitations": ["No committee lifecycle runtime."],
        "missing_for_real_emulator": ["Epoch changes", "validator membership", "committee lifecycle"],
        "next_step": "Remain disabled during V3.4 controlled smoke.",
    },
    {
        "module_id": "Routing",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["hash_sharding", "metatrack_coaccess_routing", "hotspot_aware_routing"],
        "artifact_logs": ["routing_log.csv"],
        "light_model_limitations": ["Routing/sharding decision model only; no cross-shard protocol."],
        "missing_for_real_emulator": ["Relay", "broker", "2PC", "shard-to-shard messages"],
        "next_step": "Use MetaTrack co-access routing as preset-controlled variable.",
    },
    {
        "module_id": "Execution",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["serial_execution", "parallel_light_execution", "metatrack_dual_track_execution"],
        "artifact_logs": ["execution_log.csv"],
        "light_model_limitations": ["Logical scheduling and dependency stats only."],
        "missing_for_real_emulator": ["Real threads", "rollback engine", "Block-STM/Calvin"],
        "next_step": "Use dual-track execution only in selected MetaTrack presets.",
    },
    {
        "module_id": "StateAccess",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["direct_fetch", "remote_state_access_model", "cached_state_access", "access_list_prefetch"],
        "artifact_logs": ["state_access_log.csv"],
        "light_model_limitations": ["Cache/prefetch/proof fields are estimates only."],
        "missing_for_real_emulator": ["Real proof/witness", "MPT", "remote storage IO"],
        "next_step": "Use access-list prefetch only in selected MetaTrack presets.",
    },
    {
        "module_id": "StateStorage",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["hash_state_storage"],
        "artifact_logs": ["state_commit_log.csv"],
        "light_model_limitations": ["In-memory logical storage, no persistent state tree."],
        "missing_for_real_emulator": ["Persistent KV", "state root", "snapshot", "Merkle proof"],
        "next_step": "Keep fixed for controlled smoke comparisons.",
    },
    {
        "module_id": "Commit",
        "runtime_status": "runnable",
        "realism_level": "deterministic_light_model",
        "implemented_plugins": ["normal_commit", "conservative_commit", "hot_update_aggregation", "constraint_checked_aggregation"],
        "artifact_logs": ["state_commit_log.csv"],
        "light_model_limitations": ["Aggregation and constraints are local deterministic models."],
        "missing_for_real_emulator": ["Real database locking", "persistent state-tree validation", "rollback"],
        "next_step": "Use constraint-checked aggregation only in full MetaTrack preset.",
    },
    {
        "module_id": "MetricsReport",
        "runtime_status": "runnable",
        "realism_level": "runtime_realized",
        "implemented_plugins": ["basic_metrics"],
        "artifact_logs": ["summary.csv", "summary.json", "aggregate_summary.csv"],
        "light_model_limitations": ["Smoke-level summaries, not paper-ready evidence."],
        "missing_for_real_emulator": ["Long-run experiment database", "statistical sweeps"],
        "next_step": "Use aggregate smoke metrics for readiness inspection only.",
    },
]


def build_realism_readiness() -> dict[str, Any]:
    return {
        "stage": "V3.4.10",
        "current_stage": "V3.5.1",
        "latest_runtime_stage": "V3.4.10",
        "closure_stage": "V3.4.11",
        "runtime_truth": "single_process_logical_node_topology_runtime",
        "next_stage": "V3.5.2 Local Multi-process Launcher Preview",
        "backend_truth": "local Go-backed modular research chain Draft Smoke",
        "not_real_chain_claims": [
            "not real on-chain execution",
            "not a real multi-node network",
            "not real PBFT/HotStuff/Raft",
            "not a real cross-shard protocol",
            "not real proof/witness generation",
            "not MPT/state root",
            "not persistent KV",
            "not BlockEmulator backend",
            "not Fabric/EVM live backend",
        ],
        "modules": MODULE_READINESS,
    }


def write_realism_readiness(output_dir: Path) -> dict[str, Any]:
    payload = build_realism_readiness()
    (output_dir / "realism_readiness.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    lines = [
        "# V3.4.10 Realism Readiness Check",
        "",
        "This is an internal readiness check for the local Go-backed Draft Smoke runtime.",
        "Current repository closure stage is V3.4.11; the latest runtime capability remains the V3.4.10 controlled smoke runner.",
        "It is not a real-chain, Fabric/EVM live, BlockEmulator-backed, or multi-node emulator claim.",
        "",
        "| module_id | runtime_status | realism_level | implemented_plugins | next_step |",
        "| --- | --- | --- | --- | --- |",
    ]
    for item in payload["modules"]:
        lines.append(
            "| {module_id} | {runtime_status} | {realism_level} | {plugins} | {next_step} |".format(
                module_id=item["module_id"],
                runtime_status=item["runtime_status"],
                realism_level=item["realism_level"],
                plugins=", ".join(item["implemented_plugins"]),
                next_step=item["next_step"],
            )
        )
    lines.extend([
        "",
        "Still missing for real emulator scope: real multi-node networking, real BFT/Raft consensus, real cross-shard relay/broker/2PC, real proof/witness/MPT/state root, persistent KV, Fabric/EVM live backend, and BlockEmulator adapter.",
    ])
    (output_dir / "realism_readiness.md").write_text("\n".join(lines) + "\n", encoding="utf-8")
    return payload
