from __future__ import annotations

import csv
import json
import shutil
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Iterable

from backend.app.core.paths import FORMAL_RUN_ROOT, ROOT, V5_REAL_CLUSTER_RUNS_ROOT
from backend.app.services.v3_saved_config_store import SAVED_CONFIG_ROOT, delete_saved_config, list_saved_configs


TERMINAL_STATUSES = {"completed", "completed_with_failures", "failed", "blocked", "cancelled"}
FOUR_METHOD_IDS = {"hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"}


@dataclass
class CleanupPlan:
    dry_run: bool = True
    deleted_run_group_ids: list[str] = field(default_factory=list)
    deleted_output_dirs: list[str] = field(default_factory=list)
    deleted_orphan_dirs: list[str] = field(default_factory=list)
    deleted_saved_config_ids: list[str] = field(default_factory=list)
    preserved_run_group_ids: list[str] = field(default_factory=list)
    preserved_saved_config_ids: list[str] = field(default_factory=list)
    skipped_active_runs: list[str] = field(default_factory=list)
    skipped_output_dirs: list[str] = field(default_factory=list)
    released_bytes: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict:
        return {
            "schema_version": "mbe_v5_cleanup_report_v1",
            "dry_run": self.dry_run,
            "deleted_run_group_ids": self.deleted_run_group_ids,
            "deleted_output_dirs": self.deleted_output_dirs,
            "deleted_orphan_dirs": self.deleted_orphan_dirs,
            "deleted_saved_config_ids": self.deleted_saved_config_ids,
            "preserved_run_group_ids": self.preserved_run_group_ids,
            "preserved_saved_config_ids": self.preserved_saved_config_ids,
            "skipped_active_runs": self.skipped_active_runs,
            "skipped_output_dirs": self.skipped_output_dirs,
            "released_bytes": self.released_bytes,
            "errors": self.errors,
        }


def delete_run_group(
    run_group_id: str,
    *,
    dry_run: bool = True,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    return delete_selected_run_groups(
        [run_group_id],
        dry_run=dry_run,
        formal_root=formal_root,
        real_cluster_root=real_cluster_root,
    )


def delete_selected_run_groups(
    run_group_ids: Iterable[str],
    *,
    dry_run: bool = True,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    plan = CleanupPlan(dry_run=dry_run)
    groups = _load_groups(formal_root)
    latest_successful = _latest_successful_four_method_group_id(groups)
    referenced = _referenced_output_dirs(groups, real_cluster_root)
    selected = list(dict.fromkeys(run_group_ids))

    for group_id in selected:
        try:
            group_path = _group_path(formal_root, group_id)
            group = groups.get(group_id) or _read_group(group_path)
            blockers = _run_group_delete_blockers(group, latest_successful)
            if blockers:
                if "status_not_terminal" in blockers:
                    plan.skipped_active_runs.append(group_id)
                else:
                    plan.preserved_run_group_ids.append(group_id)
                plan.errors.extend(f"{group_id}:{blocker}" for blocker in blockers)
                continue

            output_dirs = _exclusive_output_dirs(group, referenced, real_cluster_root)
            targets = [group_path, *output_dirs]
            plan.released_bytes += sum(_dir_size(path) for path in targets if path.exists())
            plan.deleted_run_group_ids.append(group_id)
            plan.deleted_output_dirs.extend(str(path) for path in output_dirs)
            if not dry_run:
                for path in output_dirs:
                    _remove_tree(path, real_cluster_root)
                _remove_tree(group_path, formal_root)
        except Exception as exc:  # keep cleanup audit evidence instead of failing half-silent
            plan.errors.append(f"{group_id}:{exc}")
    return plan.as_dict()


def delete_failed_run_groups(
    *,
    dry_run: bool = True,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    groups = _load_groups(formal_root)
    failed = [
        group_id
        for group_id, group in groups.items()
        if group.get("status") in {"failed", "blocked", "completed_with_failures"}
    ]
    return delete_selected_run_groups(failed, dry_run=dry_run, formal_root=formal_root, real_cluster_root=real_cluster_root)


def delete_old_unpinned_run_groups(
    before_time: datetime,
    *,
    dry_run: bool = True,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    groups = _load_groups(formal_root)
    selected = [
        group_id
        for group_id, group in groups.items()
        if _parse_time(group.get("finished_at") or group.get("updated_at") or group.get("created_at")) < before_time
    ]
    return delete_selected_run_groups(selected, dry_run=dry_run, formal_root=formal_root, real_cluster_root=real_cluster_root)


def scan_orphan_real_cluster_dirs(
    *,
    min_age_hours: int = 24,
    now_time: datetime | None = None,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    now_time = now_time or datetime.now(UTC)
    referenced = _referenced_output_dirs(_load_groups(formal_root), real_cluster_root)
    candidates: list[dict] = []
    if not real_cluster_root.is_dir():
        return {"schema_version": "mbe_v5_orphan_real_cluster_scan_v1", "orphan_dirs": candidates}
    for path in sorted(real_cluster_root.iterdir()):
        if not path.is_dir() or not _is_controlled_real_cluster_dir(path):
            continue
        if path.resolve() in referenced:
            continue
        age = now_time - _mtime(path)
        if age < timedelta(hours=min_age_hours):
            continue
        if _has_active_marker(path):
            continue
        candidates.append({"path": str(path), "size_bytes": _dir_size(path), "age_hours": age.total_seconds() / 3600})
    return {"schema_version": "mbe_v5_orphan_real_cluster_scan_v1", "orphan_dirs": candidates}


def cleanup_orphan_real_cluster_dirs(
    *,
    dry_run: bool = True,
    min_age_hours: int = 24,
    now_time: datetime | None = None,
    formal_root: Path = FORMAL_RUN_ROOT,
    real_cluster_root: Path = V5_REAL_CLUSTER_RUNS_ROOT,
) -> dict:
    plan = CleanupPlan(dry_run=dry_run)
    scan = scan_orphan_real_cluster_dirs(
        min_age_hours=min_age_hours,
        now_time=now_time,
        formal_root=formal_root,
        real_cluster_root=real_cluster_root,
    )
    for item in scan["orphan_dirs"]:
        path = Path(item["path"])
        plan.deleted_orphan_dirs.append(str(path))
        plan.released_bytes += int(item.get("size_bytes") or 0)
        if not dry_run:
            _remove_tree(path, real_cluster_root)
    return plan.as_dict()


def scan_legacy_saved_configs(*, saved_config_root: Path = SAVED_CONFIG_ROOT) -> dict:
    candidates: list[dict] = []
    preserved: list[dict] = []
    for config in list_saved_configs(root=saved_config_root):
        decision = _legacy_saved_config_decision(config)
        item = {
            "config_id": config.get("config_id"),
            "config_kind": config.get("config_kind"),
            "name": config.get("name", ""),
            "validation_status": config.get("validation_status", ""),
            "reason": decision,
        }
        if decision:
            candidates.append(item)
        else:
            preserved.append({**item, "reason": "current_runnable_v5_method_profile_or_non_formal_config"})
    return {
        "schema_version": "mbe_v5_legacy_saved_config_scan_v1",
        "candidate_configs": candidates,
        "preserved_configs": preserved,
        "candidate_count": len(candidates),
    }


def cleanup_legacy_saved_configs(
    *,
    dry_run: bool = True,
    saved_config_root: Path = SAVED_CONFIG_ROOT,
) -> dict:
    plan = CleanupPlan(dry_run=dry_run)
    scan = scan_legacy_saved_configs(saved_config_root=saved_config_root)
    for item in scan["candidate_configs"]:
        config_id = str(item.get("config_id") or "")
        try:
            path = (saved_config_root / f"{config_id}.json").resolve()
            _assert_within(path, saved_config_root)
            if path.is_file():
                plan.released_bytes += path.stat().st_size
            plan.deleted_saved_config_ids.append(config_id)
            if not dry_run:
                delete_saved_config(config_id, root=saved_config_root)
        except Exception as exc:
            plan.errors.append(f"{config_id}:{exc}")
    plan.preserved_saved_config_ids = [str(item.get("config_id") or "") for item in scan["preserved_configs"]]
    return plan.as_dict()


def write_cleanup_report(report: dict, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "cleanup_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    with (output_dir / "cleanup_report.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["category", "value"])
        writer.writeheader()
        for key, value in report.items():
            if isinstance(value, list):
                for item in value:
                    writer.writerow({"category": key, "value": item})
            else:
                writer.writerow({"category": key, "value": value})


def cleanup_report_output_dir(action: str, *, formal_root: Path = FORMAL_RUN_ROOT, now_time: datetime | None = None) -> Path:
    now_time = now_time or datetime.now(UTC)
    safe_action = "".join(char if char.isalnum() or char in {"-", "_"} else "_" for char in action).strip("_")
    if not safe_action:
        safe_action = "cleanup"
    report_id = now_time.strftime("%Y%m%d_%H%M%S_%f")
    return formal_root / "cleanup_reports" / safe_action / report_id


def _load_groups(formal_root: Path) -> dict[str, dict]:
    if not formal_root.is_dir():
        return {}
    groups: dict[str, dict] = {}
    for path in sorted(formal_root.glob("v5grp_*")):
        try:
            group = _read_group(path)
        except FileNotFoundError:
            continue
        groups[group["run_group_id"]] = group
    return groups


def _read_group(group_path: Path) -> dict:
    path = group_path / "run_group.json"
    if not path.is_file():
        raise FileNotFoundError(str(path))
    group = json.loads(path.read_text(encoding="utf-8"))
    group.setdefault("run_group_id", group_path.name)
    group["_group_path"] = str(group_path)
    return group


def _group_path(formal_root: Path, group_id: str) -> Path:
    if not group_id.startswith("v5grp_") or "/" in group_id or "\\" in group_id:
        raise ValueError("invalid V5 formal run group id")
    path = (formal_root / group_id).resolve()
    _assert_within(path, formal_root)
    return path


def _run_group_delete_blockers(group: dict, latest_successful: str | None) -> list[str]:
    blockers: list[str] = []
    status = group.get("status")
    group_id = group.get("run_group_id", "")
    if status not in TERMINAL_STATUSES:
        blockers.append("status_not_terminal")
    if group.get("pinned") is True:
        blockers.append("pinned")
    if _is_paper_candidate(group):
        blockers.append("paper_candidate")
    if latest_successful and group_id == latest_successful:
        blockers.append("latest_successful_four_method")
    return blockers


def _is_paper_candidate(group: dict) -> bool:
    if group.get("paper_candidate") is True:
        return True
    for child in _group_children(group):
        if child.get("paper_candidate") is True:
            return True
    aggregate = group.get("aggregate") or {}
    analysis = aggregate.get("paper_result_analysis") if isinstance(aggregate, dict) else None
    if _paper_analysis_complete(analysis):
        return True
    group_path = group.get("_group_path")
    if group_path:
        try:
            payload = json.loads((Path(group_path) / "aggregate" / "paper_result_analysis.json").read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = {}
        if _paper_analysis_complete(payload):
            return True
    return False


def _paper_analysis_complete(value: object) -> bool:
    return isinstance(value, dict) and str(value.get("analysis_status") or "").lower() in {"complete", "ready"}


def _group_children(group: dict) -> list[dict]:
    group_path = group.get("_group_path")
    if not group_path:
        return []
    child_dir = Path(group_path) / "children"
    if not child_dir.is_dir():
        return []
    children: list[dict] = []
    for path in sorted(child_dir.glob("v5child_*.json")):
        try:
            children.append(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return children


def _latest_successful_four_method_group_id(groups: dict[str, dict]) -> str | None:
    candidates = [group for group in groups.values() if _is_successful_four_method_group(group)]
    if not candidates:
        return None
    latest = max(candidates, key=lambda group: _parse_time(group.get("finished_at") or group.get("updated_at") or group.get("created_at")))
    return str(latest.get("run_group_id"))


def _is_successful_four_method_group(group: dict) -> bool:
    if group.get("status") != "completed":
        return False
    children = _group_children(group)
    completed_methods = {
        str(child.get("method_config_id") or child.get("method_id") or "")
        for child in children
        if child.get("status") == "completed"
    }
    return FOUR_METHOD_IDS.issubset(completed_methods)


def _referenced_output_dirs(groups: dict[str, dict], real_cluster_root: Path) -> dict[Path, set[str]]:
    references: dict[Path, set[str]] = {}
    for group_id, group in groups.items():
        for path in _output_dirs_for_group(group, real_cluster_root):
            references.setdefault(path.resolve(), set()).add(group_id)
    return references


def _exclusive_output_dirs(
    group: dict,
    referenced: dict[Path, set[str]],
    real_cluster_root: Path,
) -> list[Path]:
    group_id = str(group.get("run_group_id"))
    output_dirs: list[Path] = []
    for path in _output_dirs_for_group(group, real_cluster_root):
        resolved = path.resolve()
        if referenced.get(resolved, {group_id}) == {group_id} and _is_controlled_real_cluster_dir(path):
            output_dirs.append(path)
    return sorted(set(output_dirs))


def _output_dirs_for_group(group: dict, real_cluster_root: Path) -> list[Path]:
    output_dirs: list[Path] = []
    for child in _group_children(group):
        result = child.get("result") or {}
        for value in (result.get("output_dir"), (result.get("summary") or {}).get("output_dir")):
            path = _resolve_output_dir(value, real_cluster_root)
            if path is not None:
                output_dirs.append(path)
    return sorted(set(output_dirs))


def _resolve_output_dir(value: object, real_cluster_root: Path) -> Path | None:
    if not value:
        return None
    text = str(value)
    if text.startswith("$MBE_RUNTIME_ROOT"):
        suffix = text.replace("$MBE_RUNTIME_ROOT", "", 1).lstrip("/\\")
        path = real_cluster_root.parent / suffix
    else:
        path = Path(text)
        if not path.is_absolute():
            path = ROOT / path
    try:
        resolved = path.resolve()
        _assert_within(resolved, real_cluster_root)
    except ValueError:
        return None
    return resolved


def _is_controlled_real_cluster_dir(path: Path) -> bool:
    return path.name.startswith("v5_") and path.is_dir()


def _has_active_marker(path: Path) -> bool:
    return any((path / name).exists() for name in ("RUNNING", ".running", "supervisor.pid"))


def _remove_tree(path: Path, allowed_root: Path) -> None:
    resolved = path.resolve()
    _assert_within(resolved, allowed_root)
    shutil.rmtree(resolved)


def _assert_within(path: Path, root: Path) -> None:
    root = root.resolve()
    if path == root or root in path.parents:
        return
    raise ValueError(f"path outside allowed root: {path}")


def _dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _mtime(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, UTC)


def _parse_time(value: object) -> datetime:
    if not value:
        return datetime.min.replace(tzinfo=UTC)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.min.replace(tzinfo=UTC)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _legacy_saved_config_decision(config: dict) -> str:
    kind = config.get("config_kind")
    if kind == "formal_plan":
        return "legacy_v3_formal_plan_config"
    if kind != "method":
        return ""
    payload = config.get("payload") if isinstance(config.get("payload"), dict) else {}
    if payload.get("schema_version") != "v5_plugin_profile_v1":
        return "legacy_or_unknown_method_schema"
    if config.get("validation_status") != "runnable":
        return "non_runnable_v5_method_profile"
    snapshot = payload.get("compatibility_snapshot") if isinstance(payload.get("compatibility_snapshot"), dict) else {}
    selections = payload.get("plugin_selections")
    if snapshot.get("valid") is not True or not isinstance(selections, list) or not selections:
        return "invalid_v5_method_profile"
    return ""
