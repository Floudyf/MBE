from __future__ import annotations

import shutil
from pathlib import Path

ARTIFACT_ALLOWLIST = frozenset({
    "config.yaml",
    "used_config.yaml",
    "used_config.json",
    "trace_meta.json",
    "summary.csv",
    "latency.csv",
    "runtime.log",
    "report.md",
    "dual_chain_summary.csv",
    "dual_chain_summary.json",
    "stage_metrics.csv",
    "protocol_summary.csv",
    "protocol_summary.json",
    "protocol_results.csv",
    "protocol_events.csv",
    "sweep_summary.csv",
    "sweep_summary.json",
    "sweep_report.md",
    "case_artifacts_index.json",
    "calibration_summary.csv",
    "calibration_summary.json",
    "replay_vs_observed.csv",
    "calibration_report.md",
    "summary.json",
    "block_log.csv",
    "tx_results.csv",
    "state_commit_log.csv",
    "txpool_log.csv",
    "consensus_log.csv",
    "routing_log.csv",
    "execution_log.csv",
    "state_access_log.csv",
    "node_topology.csv",
    "node_log.csv",
    "network_log.csv",
    "consensus_message_log.csv",
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
    "used_chain_profile.yaml",
    "used_chain_profile.json",
    "used_plugin_profile.yaml",
    "used_plugin_profile.json",
    "used_experiment_profile.yaml",
    "used_experiment_profile.json",
    "composer_draft.json",
    "normalized_draft.json",
    "draft_validation.json",
    "generated_experiment_profile.json",
    "generated_experiment_profile.yaml",
    "generated_plugin_profile.json",
    "generated_plugin_profile.yaml",
    "metatrack_summary.csv",
    "metatrack_summary.json",
    "metatrack_latency.csv",
    "metatrack_mechanism_metrics.csv",
    "metatrack_ablation_report.md",
    "run_index.csv",
    "aggregate_summary.csv",
    "ablation_report.md",
    "realism_readiness.json",
    "realism_readiness.md",
})


class ArtifactError(ValueError):
    """Raised when an artifact request is malformed."""


class ArtifactForbidden(PermissionError):
    """Raised when an artifact is not downloadable."""


class ArtifactMissing(FileNotFoundError):
    """Raised when a valid artifact does not exist."""


def validate_filename(filename: str) -> None:
    if "/" in filename or "\\" in filename or filename in {".", ".."}:
        raise ArtifactError("invalid artifact filename")
    if filename not in ARTIFACT_ALLOWLIST:
        raise ArtifactForbidden("artifact is not downloadable")


def list_artifacts(run_dir: Path, run_id: str) -> list[dict[str, object]]:
    artifacts = []
    for filename in sorted(ARTIFACT_ALLOWLIST):
        path = run_dir / filename
        if path.is_file():
            artifacts.append({
                "name": filename,
                "download_url": f"/api/v2/runs/{run_id}/artifacts/{filename}",
                "size_bytes": path.stat().st_size,
            })
    return artifacts


def get_artifact_path(run_dir: Path, filename: str) -> Path:
    validate_filename(filename)
    root = run_dir.resolve()
    path = (run_dir / filename).resolve()
    try:
        path.relative_to(root)
    except ValueError as exc:
        raise ArtifactError("artifact path must stay inside the run directory") from exc
    if not path.is_file():
        raise ArtifactMissing("artifact not found")
    return path


def mirror_run_to_latest(run_dir: Path, latest_dir: Path) -> None:
    if latest_dir.exists():
        shutil.rmtree(latest_dir)
    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    shutil.copytree(run_dir, latest_dir)
