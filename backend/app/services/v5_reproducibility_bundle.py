from __future__ import annotations

import json
import zipfile
from pathlib import Path, PurePosixPath

from backend.app.services import v5_artifact_storage, v5_real_cluster_runner
from backend.app.services.v5_artifact_contract import file_sha256
from backend.app.services.v5_formal_run_store import children

_DIAGNOSTIC_EXTENSIONS = {".json", ".jsonl", ".csv", ".log", ".txt", ".md", ".yaml", ".yml", ".toml", ".trace"}
_MAX_DIAGNOSTIC_FILE_BYTES = 128 * 1024 * 1024
_MAX_DIAGNOSTIC_TOTAL_BYTES = 512 * 1024 * 1024
# MBE_V38_0_4_STATEFUL_ORACLE_EVIDENCE_EXPORT: stream only the raw files the
# stateful-local correctness oracle actually consumes. They remain cold-archived
# on disk; the formal bundle gets a verified copy without restoring the run tree.
_STATEFUL_ORACLE_EVIDENCE_TOTAL_BYTES = 2 * 1024 * 1024 * 1024
_STATEFUL_ORACLE_FIXED_NODE_FILES = {
    "committed_chain.csv",
    "block_execution_summary.json",
    "transaction_execution_trace.csv",
    "observed_state_access.csv",
    "node_summary.json",
    "state_snapshot.json",
    "state_snapshot_metadata.json",
}


def _experiment_conditions(group: dict) -> dict:
    plan = group.get("plan") if isinstance(group.get("plan"), dict) else {}
    base_spec = plan.get("base_spec") if isinstance(plan.get("base_spec"), dict) else {}
    workload_source = base_spec.get("workload_source") if isinstance(base_spec.get("workload_source"), dict) else {}
    source_fields = (
        "source_type", "plugin_id", "dataset_id", "variant_mode", "variant_id", "requested_tx_count",
        "use_full_dataset", "seed", "selection_mode", "replay_mode", "target_submission_tps", "skew_axis",
        "target_alpha", "materialized_id", "source_sha256", "variant_parameters",
    )
    return {
        "execution_backend": group.get("execution_backend"),
        "worker_count": plan.get("worker_count"),
        "tx_count": base_spec.get("tx_count"),
        "topology": base_spec.get("topology"),
        "workload_source": {name: workload_source.get(name) for name in source_fields if workload_source.get(name) is not None},
    }


def build(group_dir: Path, group: dict, output_path: Path | None = None) -> Path:
    output = output_path or (group_dir / "artifacts.zip")
    output_resolved = output.resolve()
    group_files = [
        path for path in group_dir.rglob("*")
        if path.is_file()
        and path.name not in {"artifacts.zip", "reproducibility_manifest.json", "artifact_manifest.json"}
        and not path.name.startswith(".artifacts.")
        and path.resolve() != output_resolved
    ]
    archive_entries: list[tuple[Path, str, str]] = [
        (path, path.relative_to(group_dir).as_posix(), "run_group") for path in group_files
    ]
    archive_entries.extend(_failed_runtime_diagnostics(group_dir, group))
    streamed_entries = _stateful_oracle_archived_evidence(group_dir, group)
    manifest_files = [
        {
            "name": archive_name,
            "source": source,
            "size_bytes": path.stat().st_size,
            "sha256": file_sha256(path),
        }
        for path, archive_name, source in archive_entries
    ]
    manifest_files.extend(
        {
            "name": item["archive_name"],
            "source": item["source"],
            "size_bytes": item["size_bytes"],
            "sha256": item["sha256"],
        }
        for item in streamed_entries
    )
    manifest = {
        "run_group_id": group["run_group_id"],
        "experiment_conditions": _experiment_conditions(group),
        "file_count": len(manifest_files),
        "files": manifest_files,
    }
    reproducibility_manifest = group_dir / "reproducibility_manifest.json"
    artifact_manifest = group_dir / "artifact_manifest.json"
    reproducibility_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    artifact_manifest.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED, allowZip64=True) as archive:
        for path, archive_name, _ in archive_entries:
            if path.is_file():
                archive.write(path, archive_name)
        for item in streamed_entries:
            info = zipfile.ZipInfo(item["archive_name"])
            info.compress_type = zipfile.ZIP_DEFLATED
            with archive.open(info, "w", force_zip64=True) as target:
                for chunk in v5_artifact_storage.stream_archived_artifact(
                    item["runtime_root"], item["artifact_name"]
                ):
                    target.write(chunk)
        archive.write(reproducibility_manifest, reproducibility_manifest.name)
        archive.write(artifact_manifest, artifact_manifest.name)
    return output


def _is_stateful_oracle_evidence_name(name: str) -> bool:
    parts = PurePosixPath(str(name).replace("\\", "/")).parts
    if len(parts) != 3 or parts[0] != "nodes":
        return False
    leaf = parts[2]
    return leaf in _STATEFUL_ORACLE_FIXED_NODE_FILES or (
        leaf.startswith("state_delta.") and leaf.endswith(".wal")
    )


def _stateful_oracle_archived_evidence(group_dir: Path, group: dict) -> list[dict]:
    del group_dir  # API symmetry with other bundle collectors; evidence is run-scoped.
    entries: list[dict] = []
    total = 0
    group_id = str(group.get("run_group_id") or "")
    if not group_id:
        return entries
    for child in children(group_id):
        if child.get("status") != "completed":
            continue
        if str(child.get("comparison_semantics_class") or "") != "stateful_local_legacy_v1":
            continue
        result = child.get("result") if isinstance(child.get("result"), dict) else {}
        run_id = str(result.get("run_id") or "")
        child_id = str(child.get("child_run_id") or "unknown_child")
        if not run_id:
            continue
        try:
            runtime_root = v5_real_cluster_runner.run_dir(run_id)
        except ValueError:
            continue
        storage = v5_artifact_storage.read_storage_summary(runtime_root)
        manifest_relative = storage.get("archive_manifest_relative_path")
        if not isinstance(manifest_relative, str) or not manifest_relative:
            continue
        manifest_path = runtime_root / Path(*PurePosixPath(manifest_relative.replace("\\", "/")).parts)
        if not manifest_path.is_file():
            continue
        try:
            frozen = json.loads(manifest_path.read_text(encoding="utf-8-sig"))
        except (OSError, UnicodeError, json.JSONDecodeError):
            continue
        for item in frozen.get("files") if isinstance(frozen.get("files"), list) else []:
            if not isinstance(item, dict):
                continue
            name = str(item.get("name") or "")
            if not _is_stateful_oracle_evidence_name(name):
                continue
            try:
                size = int(item.get("size_bytes") or 0)
            except (TypeError, ValueError):
                continue
            sha256 = str(item.get("sha256") or "").strip().lower()
            if size < 0 or len(sha256) != 64:
                continue
            if total + size > _STATEFUL_ORACLE_EVIDENCE_TOTAL_BYTES:
                raise RuntimeError(
                    "stateful oracle evidence exceeds formal bundle safety cap: "
                    f"{total + size} > {_STATEFUL_ORACLE_EVIDENCE_TOTAL_BYTES}"
                )
            entries.append({
                "runtime_root": runtime_root,
                "artifact_name": name,
                "archive_name": (Path("stateful_oracle_evidence") / child_id / Path(*PurePosixPath(name).parts)).as_posix(),
                "source": "stateful_oracle_cold_archive_evidence",
                "size_bytes": size,
                "sha256": sha256,
            })
            total += size
    return entries


def _failed_runtime_diagnostics(group_dir: Path, group: dict) -> list[tuple[Path, str, str]]:
    entries: list[tuple[Path, str, str]] = []
    total = 0
    group_id = str(group.get("run_group_id") or group_dir.name)
    for child in children(group_id):
        completed_invalid = (
            child.get("individual_result_valid") is False
            or str(child.get("comparison_eligibility_status") or "") == "individual_result_invalid"
        )
        if child.get("status") == "completed" and not completed_invalid:
            continue
        result = child.get("result") if isinstance(child.get("result"), dict) else {}
        run_id = str(result.get("run_id") or "")
        child_id = str(child.get("child_run_id") or "unknown_child")
        if not run_id:
            continue
        try:
            runtime_root = v5_real_cluster_runner.run_dir(run_id)
        except ValueError:
            continue
        if not runtime_root.is_dir():
            continue
        for path in sorted(runtime_root.rglob("*")):
            if not path.is_file() or path.suffix.lower() not in _DIAGNOSTIC_EXTENSIONS:
                continue
            size = path.stat().st_size
            if size > _MAX_DIAGNOSTIC_FILE_BYTES or total + size > _MAX_DIAGNOSTIC_TOTAL_BYTES:
                continue
            archive_name = (Path("runtime_diagnostics") / child_id / path.relative_to(runtime_root)).as_posix()
            entries.append((path, archive_name, "non_paper_eligible_child_runtime"))
            total += size
    return entries

# MBE_FORMAL_RUNTIME_CLOSURE_20260820_V7
