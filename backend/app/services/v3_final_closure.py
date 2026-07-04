from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any


V3_FINAL_TRUTH = "v3_final_emulator_closure_not_production_system"
FAULT_TRUTH = "deterministic_fault_injection_mvp_not_byzantine_adversary"
OBSERVABILITY_TRUTH = "local_observability_summary_not_production_monitoring"
REPRODUCIBILITY_TRUTH = "reproducibility_bundle_not_paper_grade_result_claim"
ALIGNMENT_TRUTH = "frontend_reads_backend_stage_metadata_with_v3_final_fallback"
STAGE = "V3-final Fault, Observability, and Reproducibility Closure"

COMPONENTS = [
    "Workload",
    "TxPool",
    "BlockProducer",
    "ConsensusRuntime",
    "CommitteeEpoch",
    "Routing/Sharding",
    "Execution",
    "StateAccess",
    "StateStorage",
    "Commit",
    "MetricsReport",
    "NetworkAdapter",
    "NodeProcessRuntime",
    "RelayMVP",
    "MetaverseExperimentSuite",
    "FaultInjection",
    "Observability",
    "ReproducibilityBundle",
]

CATALOG_GROUPS = {
    "Core Runtime": ["summary.json", "summary.csv", "runtime.log", "block_log.csv", "tx_results.csv"],
    "TxPool / BlockProducer / Consensus": ["txpool_log.csv", "consensus_log.csv", "consensus_message_log.csv", "pbft_message_log.csv", "finalized_block_log.csv"],
    "Network / PBFT Preview": ["tcp_adapter_status.csv", "network_send_log.csv", "network_receive_log.csv", "typed_message_log.csv", "consensus_network_log.csv", "pbft_network_summary.json"],
    "Relay MVP": ["relay_state_machine_log.csv", "source_lock_log.csv", "relay_certificate_log.csv", "relay_proof_verification_log.csv", "target_verification_log.csv", "target_commit_log.csv", "source_finalize_log.csv", "cross_shard_timeout_refund_log.csv", "cross_shard_failure_log.csv", "relay_mvp_summary.json"],
    "State Authenticity": ["state_root_log.csv", "state_proof_log.csv", "state_proof_verification_log.csv", "witness_log.csv", "witness_verification_log.csv", "state_authenticity_summary.json"],
    "Local Multi-process Runtime": ["address_table.json", "multi_process_manifest.json", "node_process_log.csv", "node_lifecycle_log.csv", "network_message_log.csv", "node_process_status.json", "local_multi_process_summary.json"],
    "Committee / Epoch": ["shard_assignment_log.csv", "committee_assignment_log.csv", "committee_summary.json", "epoch_log.csv", "reconfiguration_plan.json", "reshard_plan_log.csv", "reconfiguration_summary.json"],
    "Metaverse Workload": ["metaverse_workload_catalog.json", "metaverse_workload_config.json", "metaverse_trace_meta.json", "scenario_summary.csv", "hotspot_distribution.csv", "cross_scene_transfer_log.csv", "offchain_confirmation_log.csv", "cross_metaverse_transfer_log.csv", "metaverse_experiment_summary.json"],
    "Benchmark Matrix": ["benchmark_template_catalog.json", "baseline_profile_catalog.json", "benchmark_plan.json", "benchmark_run_index.csv", "sweep_matrix.csv", "sweep_summary.csv", "sweep_summary.json", "aggregate_summary.csv", "baseline_comparison.csv", "benchmark_summary.json"],
    "Paper Export": ["paper_table_latency.csv", "paper_table_throughput.csv", "paper_table_cross_shard.csv", "paper_table_offchain_confirmation.csv", "paper_figure_data.csv", "paper_export_manifest.json"],
    "Fault Injection": ["fault_injection_config.json", "fault_injection_log.csv", "node_failure_log.csv", "node_recovery_log.csv", "network_fault_log.csv", "target_congestion_log.csv", "relay_fault_observation_log.csv", "fault_injection_summary.json"],
    "Observability": ["observability_summary.json", "observability_timeline.csv", "component_health_summary.csv", "runtime_component_status.json"],
    "Reproducibility": ["final_artifact_catalog.json", "final_artifact_catalog.md", "v3_final_reproducibility_manifest.json", "v3_reproducibility_guide.md", "v3_experiment_manual.md", "v3_paper_experiment_mapping.md", "v3_final_summary.json"],
}


def write_v3_final_closure_artifacts(output_dir: Path, topology: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    config = _config(topology)
    node_ids = _node_ids(topology)
    fault_payload = _write_fault_artifacts(output_dir, config, node_ids)
    catalog = _catalog(output_dir)
    observability_payload = _write_observability_artifacts(output_dir, config, summary, fault_payload, catalog)
    repro_payload = _write_reproducibility_artifacts(output_dir, config, summary, catalog)

    metrics = {
        "v3_final_enabled": True,
        "stage_alignment_ok": True,
        "frontend_backend_alignment_truth": ALIGNMENT_TRUTH,
        "v3_final_truth": V3_FINAL_TRUTH,
        **fault_payload["metrics"],
        **observability_payload,
        **repro_payload,
    }
    _write_json(output_dir / "v3_final_summary.json", {
        "stage": STAGE,
        "summary_metrics": metrics,
        "truth_boundary": _truth_boundary(),
    })
    return metrics


def _config(topology: dict[str, Any]) -> dict[str, Any]:
    return {
        "fault_injection_enabled": bool(topology.get("fault_injection_enabled", False)),
        "fault_profile": str(topology.get("fault_profile", "none")),
        "fault_seed": int(topology.get("fault_seed", 42)),
        "fault_start_round": int(topology.get("fault_start_round", 1)),
        "fault_duration_rounds": int(topology.get("fault_duration_rounds", 1)),
        "failed_node_count": int(topology.get("failed_node_count", 1)),
        "message_delay_ms": int(topology.get("message_delay_ms", 0)),
        "message_drop_ratio": float(topology.get("message_drop_ratio", 0.0)),
        "target_congestion_ratio": float(topology.get("target_congestion_ratio", 0.0)),
        "relay_fault_mode": str(topology.get("relay_fault_mode", "none")),
        "observability_enabled": bool(topology.get("observability_enabled", True)),
        "observability_level": str(topology.get("observability_level", "basic")),
        "reproducibility_bundle_enabled": bool(topology.get("reproducibility_bundle_enabled", True)),
        "paper_mapping_enabled": bool(topology.get("paper_mapping_enabled", True)),
        "final_artifact_catalog_enabled": bool(topology.get("final_artifact_catalog_enabled", True)),
        "shard_count": int(topology.get("shard_count", 4)),
    }


def _write_fault_artifacts(output_dir: Path, config: dict[str, Any], node_ids: list[str]) -> dict[str, Any]:
    profile = config["fault_profile"] if config["fault_injection_enabled"] else "none"
    active = profile != "none"
    selected_nodes = _selected_nodes(node_ids, config["fault_seed"], config["failed_node_count"]) if active else []
    common_rows: list[dict[str, Any]] = []
    failure_rows: list[dict[str, Any]] = []
    recovery_rows: list[dict[str, Any]] = []
    network_rows: list[dict[str, Any]] = []
    congestion_rows: list[dict[str, Any]] = []
    relay_rows: list[dict[str, Any]] = []

    if profile in {"node_failure", "node_recovery", "mixed_fault"}:
        for idx, node_id in enumerate(selected_nodes):
            failure_rows.append({
                "event_time_ms": (config["fault_start_round"] * 1000) + idx,
                "node_id": node_id,
                "shard_id": _shard_from_node(node_id),
                "fault_round": config["fault_start_round"],
                "status": "failed",
                "reason": "deterministic_node_failure",
                "truth": FAULT_TRUTH,
            })
            common_rows.append(_fault_row(len(common_rows), failure_rows[-1]["event_time_ms"], profile, "node_failure", node_id, True, "deterministic_node_failure"))
    if profile in {"node_recovery", "mixed_fault"}:
        recovery_round = config["fault_start_round"] + config["fault_duration_rounds"]
        for idx, node_id in enumerate(selected_nodes):
            recovery_rows.append({
                "event_time_ms": (recovery_round * 1000) + idx,
                "node_id": node_id,
                "shard_id": _shard_from_node(node_id),
                "recovery_round": recovery_round,
                "status": "recovered",
                "reason": "deterministic_node_recovery",
                "truth": FAULT_TRUTH,
            })
            common_rows.append(_fault_row(len(common_rows), recovery_rows[-1]["event_time_ms"], profile, "node_recovery", node_id, False, "deterministic_node_recovery"))
    if profile in {"network_delay", "network_drop", "mixed_fault"}:
        for idx in range(max(1, min(8, len(node_ids) or 1))):
            event_type = "drop" if profile == "network_drop" or (profile == "mixed_fault" and _ratio_hit(idx, config["fault_seed"], config["message_drop_ratio"] or 0.25)) else "delay"
            network_rows.append({
                "event_time_ms": config["fault_start_round"] * 1000 + 100 + idx,
                "message_id": f"fault-msg-{idx}",
                "event_type": event_type,
                "delay_ms": config["message_delay_ms"] if event_type == "delay" else 0,
                "delivered": event_type != "drop",
                "reason": "deterministic_drop" if event_type == "drop" else "deterministic_delay",
                "truth": FAULT_TRUTH,
            })
            common_rows.append(_fault_row(len(common_rows), network_rows[-1]["event_time_ms"], profile, f"network_{event_type}", network_rows[-1]["message_id"], event_type != "drop", network_rows[-1]["reason"]))
    if profile in {"target_congestion", "mixed_fault"}:
        for idx in range(max(1, config["shard_count"])):
            if profile == "target_congestion" or _ratio_hit(idx, config["fault_seed"] + 3, config["target_congestion_ratio"] or 0.25):
                congestion_rows.append({
                    "event_time_ms": config["fault_start_round"] * 1000 + 200 + idx,
                    "target_shard_id": idx,
                    "congestion_ratio": config["target_congestion_ratio"] or 0.25,
                    "status": "congested",
                    "reason": "deterministic_target_congestion",
                    "truth": FAULT_TRUTH,
                })
                common_rows.append(_fault_row(len(common_rows), congestion_rows[-1]["event_time_ms"], profile, "target_congestion", f"shard{idx}", True, "deterministic_target_congestion"))
    if profile in {"relay_fault", "mixed_fault"}:
        modes = ["proof_fail", "timeout", "target_reject"] if config["relay_fault_mode"] == "none" else [config["relay_fault_mode"]]
        for idx, mode in enumerate(modes):
            relay_rows.append({
                "event_time_ms": config["fault_start_round"] * 1000 + 300 + idx,
                "relay_tx_id": f"relay-fault-{idx}",
                "relay_fault_mode": mode,
                "observed_status": "failed" if mode != "timeout" else "refunded",
                "reason": f"deterministic_{mode}",
                "truth": FAULT_TRUTH,
            })
            common_rows.append(_fault_row(len(common_rows), relay_rows[-1]["event_time_ms"], profile, "relay_fault", relay_rows[-1]["relay_tx_id"], False, relay_rows[-1]["reason"]))

    _write_json(output_dir / "fault_injection_config.json", {**config, "effective_fault_profile": profile, "truth": FAULT_TRUTH})
    _write_csv(output_dir / "fault_injection_log.csv", ["event_id", "event_time_ms", "fault_profile", "event_type", "target", "active", "reason", "truth"], common_rows)
    _write_csv(output_dir / "node_failure_log.csv", ["event_time_ms", "node_id", "shard_id", "fault_round", "status", "reason", "truth"], failure_rows)
    _write_csv(output_dir / "node_recovery_log.csv", ["event_time_ms", "node_id", "shard_id", "recovery_round", "status", "reason", "truth"], recovery_rows)
    _write_csv(output_dir / "network_fault_log.csv", ["event_time_ms", "message_id", "event_type", "delay_ms", "delivered", "reason", "truth"], network_rows)
    _write_csv(output_dir / "target_congestion_log.csv", ["event_time_ms", "target_shard_id", "congestion_ratio", "status", "reason", "truth"], congestion_rows)
    _write_csv(output_dir / "relay_fault_observation_log.csv", ["event_time_ms", "relay_tx_id", "relay_fault_mode", "observed_status", "reason", "truth"], relay_rows)

    metrics = {
        "fault_injection_enabled": config["fault_injection_enabled"],
        "fault_profile": profile,
        "fault_event_count": len(common_rows),
        "node_failure_count": len(failure_rows),
        "node_recovery_count": len(recovery_rows),
        "network_delay_event_count": sum(1 for row in network_rows if row["event_type"] == "delay"),
        "network_drop_event_count": sum(1 for row in network_rows if row["event_type"] == "drop"),
        "target_congestion_event_count": len(congestion_rows),
        "relay_fault_event_count": len(relay_rows),
        "fault_injection_truth": FAULT_TRUTH,
    }
    _write_json(output_dir / "fault_injection_summary.json", {
        **metrics,
        "effective_fault_profile": profile,
        "selected_nodes": selected_nodes,
        "truth_boundary": "Deterministic local MVP fault metadata only; not a Byzantine adversary or production fault tolerance model.",
    })
    return {"metrics": metrics, "events": common_rows}


def _write_observability_artifacts(output_dir: Path, config: dict[str, Any], summary: dict[str, Any], fault_payload: dict[str, Any], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    fault_count = int(fault_payload["metrics"]["fault_event_count"])
    files = {path.name for path in output_dir.iterdir() if path.is_file()}
    timeline = [
        _timeline_row(0, "runtime", "stage", "info", "summary.json", STAGE),
        _timeline_row(10, "artifact", "catalog", "info", "final_artifact_catalog.json", "final artifact catalog generated"),
        _timeline_row(20, "fault", "fault_summary", "warning" if fault_count else "info", "fault_injection_summary.json", f"{fault_count} deterministic fault events"),
    ]
    if "network_message_log.csv" in files or "typed_message_log.csv" in files:
        timeline.append(_timeline_row(30, "network", "network_path", "info", "network_message_log.csv", "local NetworkAdapter/message path observed"))
    if "relay_mvp_summary.json" in files:
        timeline.append(_timeline_row(40, "relay", "relay_mvp", "info", "relay_mvp_summary.json", "Relay MVP observable artifacts available"))
    if "metaverse_experiment_summary.json" in files:
        timeline.append(_timeline_row(50, "metaverse", "metaverse_suite", "info", "metaverse_experiment_summary.json", "controlled metaverse workload artifacts available"))
    if config["observability_level"] == "detailed":
        timeline.extend(_timeline_row(100 + idx, "component", "health", "info", "component_health_summary.csv", component) for idx, component in enumerate(COMPONENTS))

    health_rows = []
    warning_count = 0
    error_count = 0
    for component in COMPONENTS:
        component_files = _component_artifacts(component)
        artifact_count = sum(1 for name in component_files if name in files)
        warnings = 1 if component == "FaultInjection" and fault_count else 0
        errors = 0
        status = "warning" if warnings else "healthy"
        warning_count += warnings
        error_count += errors
        health_rows.append({
            "component": component,
            "status": status,
            "warning_count": warnings,
            "error_count": errors,
            "artifact_count": artifact_count,
            "last_event": "deterministic_fault_observed" if warnings else "artifact_checked",
            "truth": OBSERVABILITY_TRUTH,
        })

    _write_csv(output_dir / "observability_timeline.csv", ["event_time_ms", "component", "event_type", "severity", "source_artifact", "message", "truth"], timeline)
    _write_csv(output_dir / "component_health_summary.csv", ["component", "status", "warning_count", "error_count", "artifact_count", "last_event", "truth"], health_rows)
    status_payload = {
        "stage": STAGE,
        "runtime_truth": V3_FINAL_TRUTH,
        "components": health_rows,
        "truth": OBSERVABILITY_TRUTH,
    }
    _write_json(output_dir / "runtime_component_status.json", status_payload)
    metrics = {
        "observability_enabled": config["observability_enabled"],
        "observability_level": config["observability_level"],
        "component_health_count": len(health_rows),
        "component_warning_count": warning_count,
        "component_error_count": error_count,
        "observability_truth": OBSERVABILITY_TRUTH,
    }
    _write_json(output_dir / "observability_summary.json", {
        **metrics,
        "component_count": len(health_rows),
        "component_healthy_count": sum(1 for row in health_rows if row["status"] == "healthy"),
        "fault_event_count": fault_count,
        "network_event_count": int(summary.get("network_message_count", 0) or summary.get("consensus_message_count", 0) or 0),
        "relay_event_count": int(summary.get("relay_mvp_tx_count", 0) or summary.get("relay_success_count", 0) or 0),
        "metaverse_event_count": int(summary.get("metaverse_tx_count", 0) or 0),
        "artifact_group_count": len({item["group"] for item in catalog}),
        "observability_truth": OBSERVABILITY_TRUTH,
    })
    return metrics


def _write_reproducibility_artifacts(output_dir: Path, config: dict[str, Any], summary: dict[str, Any], catalog: list[dict[str, Any]]) -> dict[str, Any]:
    catalog_available = bool(config["final_artifact_catalog_enabled"])
    if catalog_available:
        _write_json(output_dir / "final_artifact_catalog.json", {"stage": STAGE, "truth": V3_FINAL_TRUTH, "artifacts": catalog})
        _write_text(output_dir / "final_artifact_catalog.md", _catalog_md(catalog))

    manifest_available = bool(config["reproducibility_bundle_enabled"])
    if manifest_available:
        _write_json(output_dir / "v3_final_reproducibility_manifest.json", {
            "repo_stage": STAGE,
            "latest_known_commit": "record with `git log -1 --oneline` after final commit",
            "expected_versions": {"python": "3.12.x", "go": "1.26.1", "node": "22 LTS", "typescript": "5.x"},
            "validation_commands": [
                "git diff --check",
                "cd frontend && npm.cmd run build && cd ..",
                "cd executor && go test ./... && cd ..",
                "python -m pytest backend/tests -q",
                "python -m pytest tests -q",
                "python scripts/v0_sanity.py",
            ],
            "key_entrypoints": ["backend/app/main.py", "backend/app/services/v3_composer_draft_runner.py", "frontend/src/pages/V3ComposerPage.tsx"],
            "recommended_smoke_sequence": ["Validate draft", "Run Draft Smoke", "Download summary.json", "Inspect v3_final_summary.json", "Inspect reproducibility guide/manual/mapping"],
            "truth_boundary": _truth_boundary(),
        })
        _write_text(output_dir / "v3_reproducibility_guide.md", _guide_md())
        _write_text(output_dir / "v3_experiment_manual.md", _manual_md())

    mapping_available = bool(config["paper_mapping_enabled"])
    if mapping_available:
        _write_text(output_dir / "v3_paper_experiment_mapping.md", _paper_mapping_md())

    return {
        "reproducibility_bundle_enabled": manifest_available,
        "final_artifact_catalog_available": catalog_available,
        "reproducibility_manifest_available": manifest_available,
        "reproducibility_guide_available": manifest_available,
        "experiment_manual_available": manifest_available,
        "paper_mapping_available": mapping_available,
        "reproducibility_truth": REPRODUCIBILITY_TRUTH,
    }


def _catalog(output_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for group, filenames in CATALOG_GROUPS.items():
        for filename in filenames:
            rows.append({
                "filename": filename,
                "group": group,
                "stage_added": _stage_for_group(group),
                "purpose": _purpose_for_group(group),
                "available": (output_dir / filename).is_file(),
                "required_for_v3_final": group in {"Fault Injection", "Observability", "Reproducibility"},
                "truth_boundary": _truth_for_group(group),
            })
    return rows


def _node_ids(topology: dict[str, Any]) -> list[str]:
    shards = int(topology.get("shard_count", 4))
    validators = int(topology.get("validators_per_shard", 4))
    executors = int(topology.get("executors_per_shard", 1))
    storage = int(topology.get("storage_nodes_per_shard", 1))
    nodes = [f"shard{s}-validator{v}" for s in range(shards) for v in range(validators)]
    nodes.extend(f"shard{s}-executor{e}" for s in range(shards) for e in range(executors))
    nodes.extend(f"shard{s}-storage{idx}" for s in range(shards) for idx in range(storage))
    if bool(topology.get("supervisor_enabled", True)):
        nodes.append("supervisor0")
    return nodes or ["logical-node0"]


def _selected_nodes(nodes: list[str], seed: int, count: int) -> list[str]:
    if count <= 0:
        return []
    indexed = sorted(((idx * 1103515245 + seed) % 2147483647, node) for idx, node in enumerate(nodes))
    return [node for _, node in indexed[: min(count, len(indexed))]]


def _fault_row(event_id: int, event_time_ms: int, profile: str, event_type: str, target: str, active: bool, reason: str) -> dict[str, Any]:
    return {
        "event_id": f"fault-{event_id}",
        "event_time_ms": event_time_ms,
        "fault_profile": profile,
        "event_type": event_type,
        "target": target,
        "active": active,
        "reason": reason,
        "truth": FAULT_TRUTH,
    }


def _timeline_row(event_time_ms: int, component: str, event_type: str, severity: str, source_artifact: str, message: str) -> dict[str, Any]:
    return {
        "event_time_ms": event_time_ms,
        "component": component,
        "event_type": event_type,
        "severity": severity,
        "source_artifact": source_artifact,
        "message": message,
        "truth": OBSERVABILITY_TRUTH,
    }


def _component_artifacts(component: str) -> list[str]:
    if component == "NetworkAdapter":
        return ["network_message_log.csv", "typed_message_log.csv", "network_send_log.csv", "network_receive_log.csv"]
    if component == "FaultInjection":
        return ["fault_injection_summary.json", "fault_injection_log.csv"]
    if component == "Observability":
        return ["observability_summary.json", "observability_timeline.csv", "component_health_summary.csv"]
    if component == "ReproducibilityBundle":
        return ["v3_final_reproducibility_manifest.json", "v3_reproducibility_guide.md", "v3_experiment_manual.md"]
    if component == "MetaverseExperimentSuite":
        return ["metaverse_experiment_summary.json", "scenario_summary.csv"]
    if component == "RelayMVP":
        return ["relay_mvp_summary.json", "relay_state_machine_log.csv"]
    return []


def _ratio_hit(idx: int, seed: int, ratio: float) -> bool:
    if ratio <= 0:
        return False
    if ratio >= 1:
        return True
    return ((idx * 37 + seed * 17) % 10000) < int(ratio * 10000)


def _shard_from_node(node_id: str) -> int:
    if node_id.startswith("shard"):
        text = node_id[5:].split("-", 1)[0]
        if text.isdigit():
            return int(text)
    return 0


def _stage_for_group(group: str) -> str:
    if group in {"Fault Injection", "Observability", "Reproducibility"}:
        return "V3-final"
    if group == "Metaverse Workload":
        return "V3.13"
    if group in {"Local Multi-process Runtime", "Committee / Epoch"}:
        return "V3.12"
    if group == "Relay MVP":
        return "V3.11"
    return "V3"


def _purpose_for_group(group: str) -> str:
    return {
        "Fault Injection": "Deterministic local fault events for closure smoke and observability.",
        "Observability": "Local component/timeline health summary for emulator runs.",
        "Reproducibility": "Final catalog, manifest, guide, manual, and paper mapping.",
    }.get(group, f"{group} artifacts used by the V3 local emulator.")


def _truth_for_group(group: str) -> str:
    if group == "Fault Injection":
        return FAULT_TRUTH
    if group == "Observability":
        return OBSERVABILITY_TRUTH
    if group == "Reproducibility":
        return REPRODUCIBILITY_TRUTH
    if group == "Metaverse Workload":
        return "controlled_metaverse_workload_not_real_platform_trace"
    if group in {"Local Multi-process Runtime", "Committee / Epoch"}:
        return "local_multi_process_runtime_mvp_not_production_cluster"
    if group == "Relay MVP":
        return "relay_mvp_not_production_atomic_commit"
    return V3_FINAL_TRUTH


def _truth_boundary() -> str:
    return (
        "V3-final implements a local emulator closure with deterministic fault injection, observability, and "
        "reproducibility artifacts. It is not multi-server deployment, not a production cluster, not production "
        "PBFT/HotStuff/Raft, not BlockEmulator backend, not Fabric/EVM live backend, and does not prove paper-grade performance."
    )


def _catalog_md(catalog: list[dict[str, Any]]) -> str:
    lines = ["# V3-final Artifact Catalog", "", "| group | filename | available | truth_boundary |", "| --- | --- | --- | --- |"]
    for item in catalog:
        lines.append(f"| {item['group']} | {item['filename']} | {item['available']} | {item['truth_boundary']} |")
    lines.extend(["", _truth_boundary()])
    return "\n".join(lines) + "\n"


def _guide_md() -> str:
    sections = [
        "V3-final goal",
        "Environment and versions",
        "Draft Smoke entrypoint",
        "Recommended validation commands",
        "Fault injection configuration",
        "Observability outputs",
        "Artifact catalog",
        "Reproducibility manifest",
        "Controlled smoke sequence",
        "Reading paper export scaffolds",
        "Known non-goals",
        "Truth boundary",
    ]
    lines = ["# V3-final Reproducibility Guide", ""]
    for section in sections:
        lines.extend([f"## {section}", _guide_text(section), ""])
    return "\n".join(lines).rstrip() + "\n"


def _manual_md() -> str:
    sections = [
        "Purpose",
        "Choose a template",
        "Configure topology",
        "Configure fault injection",
        "Run draft smoke",
        "Inspect observability",
        "Download final artifacts",
        "Interpret boundaries",
    ]
    lines = ["# V3 Experiment Manual", ""]
    for section in sections:
        lines.extend([f"## {section}", _manual_text(section), ""])
    return "\n".join(lines).rstrip() + "\n"


def _paper_mapping_md() -> str:
    sections = [
        "Scope",
        "Runtime metrics",
        "Metaverse workload organization",
        "Baseline matrix scaffolding",
        "Fault observation",
        "Observability evidence",
        "Reproducibility package",
        "Limitations",
    ]
    lines = ["# V3 Paper Experiment Mapping", ""]
    for section in sections:
        lines.extend([f"## {section}", _paper_text(section), ""])
    lines.append("This mapping explains how MBE artifacts support experiment organization. It does not claim that current outputs are paper-grade final results.")
    return "\n".join(lines).rstrip() + "\n"


def _guide_text(section: str) -> str:
    if section == "Truth boundary":
        return _truth_boundary()
    return "Use the generated local artifacts as deterministic emulator evidence; keep version, config, and summary files together for repeatable inspection."


def _manual_text(section: str) -> str:
    if section == "Interpret boundaries":
        return _truth_boundary()
    return "Operate through the V3 Composer draft workflow and inspect the generated artifact group for this step."


def _paper_text(section: str) -> str:
    if section == "Limitations":
        return _truth_boundary()
    return "Map this artifact family to experiment organization only; treat values as local emulator outputs unless independently validated."


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def _write_csv(path: Path, fields: list[str], rows: list[dict[str, Any]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})
