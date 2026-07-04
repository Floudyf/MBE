from __future__ import annotations

import csv
import json
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from backend.app.models.v3_composer_draft import V3ComposerDraftModule, V3ComposerDraftRequest, V3RuntimeTopology
from backend.app.services.artifact_manager import get_artifact_path
from backend.app.services.job_manager import JobManager
from backend.app.services.v3_composer_draft_runner import run_v3_composer_draft_smoke
from backend.app.services.v3_experiment_templates import STANDARD_MODULE_ORDER, get_template
from backend.app.services.v3_realism_readiness import write_realism_readiness


ROOT = Path(__file__).resolve().parents[3]
CONTROLLED_SMOKE_ROOT = ROOT / "experiments" / "runs" / "v3_4_10_controlled_smoke"
METATRACK_TEMPLATE_ID = "metatrack_ablation"
CURRENT_STAGE = "V3.13 Metaverse Experiment Suite Closure"
LATEST_RUNTIME_STAGE = "controlled metaverse workload suite with scenario templates, baseline matrix, multi-seed sweep, and paper export artifacts"
CLOSURE_STAGE = "V3.13"
LATEST_COMPLETED_RUNTIME_STAGE = LATEST_RUNTIME_STAGE
CURRENT_CAPABILITY = "metaverse workload catalog, scenario templates, controlled benchmark matrix, multi-seed sweep MVP, and paper table/figure data export"
RUNTIME_TRUTH = "controlled_metaverse_workload_not_real_platform_trace"
NEXT_STAGE = "V3-final Fault, Observability, and Reproducibility Closure"
CONTROLLED_PRESET_ORDER = [
    "metatrack_baseline_smoke",
    "metatrack_routing_only_smoke",
    "metatrack_routing_execution_smoke",
    "metatrack_routing_execution_state_access_smoke",
    "metatrack_full_smoke",
]
AGGREGATE_FIELDS = [
    "preset_id",
    "ablation_stage",
    "enabled_metatrack_components",
    "cross_shard_ratio",
    "cross_shard_protocol_selected",
    "relay_mvp_tx_count",
    "relay_success_count",
    "relay_failed_count",
    "relay_refund_count",
    "fast_track_count",
    "conservative_track_count",
    "remote_state_access_ratio",
    "cache_hit_rate",
    "prefetch_hit_rate",
    "aggregation_ratio",
    "constraint_failed_count",
    "avg_execution_latency_ms",
    "avg_state_access_latency_ms",
    "avg_commit_latency_ms",
    "state_backend_selected",
    "state_root_count",
    "state_proof_verified_count",
    "witness_verified_count",
    "benchmark_template_selected",
    "baseline_profile_selected",
    "benchmark_run_count",
    "repeat_count",
    "paper_grade_benchmark",
    "metaverse_scenario_selected",
    "metaverse_tx_count",
    "metaverse_cross_scene_count",
    "metaverse_cross_shard_count",
    "metaverse_offchain_confirmation_count",
    "metaverse_cross_metaverse_count",
    "baseline_count",
    "seed_count",
    "paper_table_available",
]
CONTROLLED_ARTIFACTS = [
    "run_index.csv",
    "aggregate_summary.csv",
    "ablation_report.md",
    "realism_readiness.json",
    "realism_readiness.md",
    "node_address_table.csv",
    "topology.json",
    "launch_nodes_windows.bat",
    "launch_nodes_linux.sh",
    "launcher_readme.md",
    "node_process_status.csv",
    "node_process_manifest.json",
    "node_process_log_sample.log",
    "tcp_adapter_status.csv",
    "network_send_log.csv",
    "network_receive_log.csv",
    "typed_message_log.csv",
    "consensus_network_light_log.csv",
    "network_consensus_summary.json",
    "pbft_state_log.csv",
    "pbft_message_log.csv",
    "quorum_log.csv",
    "finalized_block_log.csv",
    "consensus_network_log.csv",
    "pbft_network_summary.json",
    "cross_shard_tx_log.csv",
    "cross_shard_message_log.csv",
    "relay_preview_log.csv",
    "cross_shard_status.csv",
    "cross_shard_summary.json",
    "relay_state_machine_log.csv",
    "source_lock_log.csv",
    "relay_certificate_log.csv",
    "relay_proof_verification_log.csv",
    "target_verification_log.csv",
    "target_commit_log.csv",
    "source_finalize_log.csv",
    "cross_shard_timeout_refund_log.csv",
    "cross_shard_failure_log.csv",
    "relay_mvp_summary.json",
    "state_storage_log.csv",
    "state_version_log.csv",
    "state_root_log.csv",
    "state_proof_log.csv",
    "state_proof_verification_log.csv",
    "witness_log.csv",
    "witness_verification_log.csv",
    "state_authenticity_summary.json",
    "benchmark_template_catalog.json",
    "baseline_profile_catalog.json",
    "benchmark_plan.json",
    "benchmark_run_index.csv",
    "sweep_matrix.csv",
    "sweep_summary.csv",
    "sweep_summary.json",
    "baseline_comparison.csv",
    "reproducibility_manifest.json",
    "benchmark_report.md",
    "benchmark_summary.json",
    "metaverse_workload_catalog.json",
    "metaverse_workload_config.json",
    "metaverse_trace_meta.json",
    "scenario_summary.csv",
    "hotspot_distribution.csv",
    "cross_scene_transfer_log.csv",
    "offchain_confirmation_log.csv",
    "cross_metaverse_transfer_log.csv",
    "metaverse_experiment_summary.json",
    "baseline_matrix.csv",
    "multi_seed_summary.csv",
    "benchmark_suite_summary.json",
    "paper_table_latency.csv",
    "paper_table_throughput.csv",
    "paper_table_cross_shard.csv",
    "paper_table_offchain_confirmation.csv",
    "paper_figure_data.csv",
    "paper_export_manifest.json",
]


class ControlledSmokeError(ValueError):
    """Raised when a controlled smoke run cannot be created safely."""


def controlled_smoke_job_manager(root: Path = CONTROLLED_SMOKE_ROOT) -> JobManager:
    return JobManager(root)


def run_v3_4_10_controlled_smoke(root: Path = CONTROLLED_SMOKE_ROOT) -> dict[str, Any]:
    template = get_template(METATRACK_TEMPLATE_ID)
    presets_by_id = {str(preset["preset_id"]): preset for preset in template.get("presets", [])}
    missing = [preset_id for preset_id in CONTROLLED_PRESET_ORDER if preset_id not in presets_by_id]
    if missing:
        raise ControlledSmokeError(f"missing MetaTrack controlled smoke presets: {', '.join(missing)}")

    manager = controlled_smoke_job_manager(root)
    metadata = manager.create_run(
        source="v3_4_10_controlled_smoke",
        experiment_name="metatrack_controlled_smoke",
        data_truth_label="modular_runtime",
        stage=CURRENT_STAGE,
        extra_metadata={
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "run_mode": "controlled_smoke",
            "experiment_template": METATRACK_TEMPLATE_ID,
            "preset_order": CONTROLLED_PRESET_ORDER,
        },
    )
    run_id = metadata["run_id"]
    run_dir = manager.run_dir(run_id)
    manager.mark_running(run_id)
    preset_root = run_dir / "preset_runs"

    try:
        run_rows: list[dict[str, Any]] = []
        aggregate_rows: list[dict[str, Any]] = []
        child_results: list[dict[str, Any]] = []
        for preset_id in CONTROLLED_PRESET_ORDER:
            preset = presets_by_id[preset_id]
            request = build_preset_draft_request(template, preset)
            result = run_v3_composer_draft_smoke(request, root=preset_root)
            summary = result.get("summary", {})
            child_results.append(result)
            child_run_id = str(result["run_id"])
            child_output_dir = Path(str(result["output_dir"]))
            run_rows.append({
                "run_id": child_run_id,
                "preset_id": preset_id,
                "preset_name": str(preset.get("preset_name", "")),
                "ablation_stage": str(preset.get("ablation_stage", "")),
                "enabled_metatrack_components": _join(preset.get("enabled_metatrack_components", [])),
                "run_status": str(result.get("status", "")),
                "summary_path": str(child_output_dir / "summary.json"),
                "artifact_dir": str(child_output_dir),
            })
            aggregate_rows.append(_aggregate_row(summary, preset))

        _write_csv(run_dir / "run_index.csv", run_rows, [
            "run_id",
            "preset_id",
            "preset_name",
            "ablation_stage",
            "enabled_metatrack_components",
            "run_status",
            "summary_path",
            "artifact_dir",
        ])
        _write_csv(run_dir / "aggregate_summary.csv", aggregate_rows, AGGREGATE_FIELDS)
        _write_report(run_dir / "ablation_report.md", run_id, aggregate_rows)
        _copy_representative_launcher_artifacts(run_dir, child_results)
        readiness = write_realism_readiness(run_dir)
        _write_json(run_dir / "controlled_run.json", {
            "run_id": run_id,
            "stage": CURRENT_STAGE,
            "current_stage": CURRENT_STAGE,
            "latest_runtime_stage": LATEST_RUNTIME_STAGE,
            "latest_completed_runtime_stage": LATEST_COMPLETED_RUNTIME_STAGE,
            "closure_stage": CLOSURE_STAGE,
            "current_capability": CURRENT_CAPABILITY,
            "runtime_truth": RUNTIME_TRUTH,
            "next_stage": NEXT_STAGE,
            "preset_order": CONTROLLED_PRESET_ORDER,
            "run_index": run_rows,
            "aggregate_summary": aggregate_rows,
            "realism_readiness": readiness,
        })
        completed = manager.mark_completed(run_id, data_truth_label="modular_runtime")
        return {
            "run_id": run_id,
            "status": "completed",
            "stage": CURRENT_STAGE,
            "current_stage": CURRENT_STAGE,
            "latest_runtime_stage": LATEST_RUNTIME_STAGE,
            "latest_completed_runtime_stage": LATEST_COMPLETED_RUNTIME_STAGE,
            "closure_stage": CLOSURE_STAGE,
            "current_capability": CURRENT_CAPABILITY,
            "runtime_truth": RUNTIME_TRUTH,
            "next_stage": NEXT_STAGE,
            "output_dir": str(run_dir),
            "data_truth_label": "modular_runtime",
            "backend_type": "modular_research_chain",
            "runtime_mode": "go_backed",
            "run_mode": "controlled_smoke",
            "preset_order": CONTROLLED_PRESET_ORDER,
            "run_index": run_rows,
            "aggregate_summary": aggregate_rows,
            "realism_readiness": readiness,
            "artifacts": list_controlled_artifacts(run_dir, run_id),
            "child_runs": child_results,
            "run": completed,
        }
    except Exception as exc:
        manager.mark_failed(run_id, str(exc))
        raise


def build_preset_draft_request(template: dict[str, Any], preset: dict[str, Any]) -> V3ComposerDraftRequest:
    preset_id = str(preset["preset_id"])
    selection = {
        **{str(key): str(value) for key, value in (preset.get("locked_modules") or {}).items()},
        **{str(key): str(value) for key, value in (preset.get("default_plugin_selection") or {}).items()},
    }
    controlled_modules = set(str(item) for item in (preset.get("controlled_modules") or template.get("controlled_modules") or []))
    modules: dict[str, V3ComposerDraftModule] = {}
    for module_id in STANDARD_MODULE_ORDER:
        plugin = selection.get(module_id)
        if not plugin:
            raise ControlledSmokeError(f"preset {preset_id} does not define plugin for {module_id}")
        if module_id == "MetricsReport":
            status = "output"
        elif module_id == "CommitteeEpoch" or plugin == "disabled":
            status = "disabled"
        elif module_id in controlled_modules:
            status = "variable"
        else:
            status = "fixed"
        modules[module_id] = V3ComposerDraftModule(module_id=module_id, status=status, plugin=plugin)
    topology = V3RuntimeTopology(
        metaverse_suite_enabled=True,
        metaverse_scenario="mixed_metaverse",
        tx_count=64,
        user_count=24,
        asset_count=64,
        item_count=32,
        avatar_count=24,
        scene_count=8,
        metaverse_count=2,
        hotspot_ratio=0.25,
        cross_scene_ratio=0.25,
        cross_shard_ratio=0.25,
        offchain_confirmation_enabled=True,
        offchain_failure_ratio=0.1,
        cross_metaverse_enabled=True,
        benchmark_suite_enabled=True,
        baseline_matrix_enabled=True,
        multi_seed_enabled=True,
        paper_export_enabled=True,
        sweep_seed_count=2,
        sweep_shard_counts=[1, 2],
        sweep_cross_shard_ratios=[0.0, 0.25],
        sweep_hotspot_ratios=[0.0, 0.25],
    )
    return V3ComposerDraftRequest(template_id=METATRACK_TEMPLATE_ID, preset_id=preset_id, modules=modules, topology=topology)


def list_controlled_artifacts(run_dir: Path, run_id: str) -> list[dict[str, Any]]:
    artifacts = []
    for filename in CONTROLLED_ARTIFACTS:
        path = run_dir / filename
        if path.is_file():
            artifacts.append({
                "name": filename,
                "download_url": f"/api/v3/composer/controlled-smoke/{run_id}/artifacts/{filename}",
                "size_bytes": path.stat().st_size,
            })
    return artifacts


def get_controlled_artifact_path(run_id: str, filename: str, root: Path = CONTROLLED_SMOKE_ROOT) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,120}", run_id) or run_id in {".", "..", "latest"}:
        raise ValueError("invalid controlled smoke run id")
    if filename not in CONTROLLED_ARTIFACTS:
        raise ValueError("controlled smoke artifact is not downloadable")
    return get_artifact_path(controlled_smoke_job_manager(root).run_dir(run_id), filename)


def _copy_representative_launcher_artifacts(run_dir: Path, child_results: list[dict[str, Any]]) -> None:
    representative = next((result for result in child_results if str((result.get("summary") or {}).get("preset_id", "")) == "metatrack_full_smoke"), child_results[-1] if child_results else None)
    if not representative:
        return
    child_dir = Path(str(representative.get("output_dir", "")))
    for filename in CONTROLLED_ARTIFACTS:
        if filename in {"run_index.csv", "aggregate_summary.csv", "ablation_report.md", "realism_readiness.json", "realism_readiness.md"}:
            continue
        source = child_dir / filename
        if source.is_file():
            shutil.copyfile(source, run_dir / filename)


def _aggregate_row(summary: dict[str, Any], preset: dict[str, Any]) -> dict[str, Any]:
    row = {
        "preset_id": str(preset.get("preset_id", "")),
        "ablation_stage": str(preset.get("ablation_stage", "")),
        "enabled_metatrack_components": _join(preset.get("enabled_metatrack_components", [])),
    }
    for field in AGGREGATE_FIELDS:
        row.setdefault(field, summary.get(field, ""))
    return row


def _write_report(path: Path, run_id: str, rows: list[dict[str, Any]]) -> None:
    lines = [
        "# V3.4.10 Controlled MetaTrack Smoke",
        "",
        "This report summarizes five preset-controlled local Draft Smoke runs.",
        "Repository closure stage: V3.4.11. Latest runtime capability: V3.4.10 controlled smoke runner.",
        "All presets keep workload, seed, TxPool, BlockProducer, Consensus, CommitteeEpoch, StateStorage, and MetricsReport fixed.",
        "It is not a paper-ready benchmark, Fabric live execution, BlockEmulator backend, or multi-node emulator.",
        "",
        f"- controlled_run_id: {run_id}",
        f"- preset_count: {len(rows)}",
        "",
        "| preset_id | ablation_stage | enabled_components | cross_shard_ratio | avg_execution_latency_ms | avg_state_access_latency_ms | avg_commit_latency_ms |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in rows:
        lines.append(
            "| {preset_id} | {ablation_stage} | {enabled} | {cross} | {exec_latency} | {state_latency} | {commit_latency} |".format(
                preset_id=row.get("preset_id", ""),
                ablation_stage=row.get("ablation_stage", ""),
                enabled=row.get("enabled_metatrack_components", ""),
                cross=row.get("cross_shard_ratio", ""),
                exec_latency=row.get("avg_execution_latency_ms", ""),
                state_latency=row.get("avg_state_access_latency_ms", ""),
                commit_latency=row.get("avg_commit_latency_ms", ""),
            )
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: _csv_value(row.get(field, "")) for field in fieldnames})


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")


def _csv_value(value: Any) -> str:
    if isinstance(value, (list, tuple, set)):
        return _join(value)
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return str(value)


def _join(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple, set)):
        return "|".join(str(item) for item in value)
    return str(value) if value is not None else ""
