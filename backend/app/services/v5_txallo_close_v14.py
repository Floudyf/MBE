"""Finalize optional TxAllo evidence after extract/oracles and before cold archive.

No edits to formal eligibility, PBFT, TxAllo allocation or runtime decisions.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from backend.app.services import v5_real_cluster_artifacts
from backend.app.services.v5_artifact_contract import catalog_entry
from backend.app.services.v5_txallo_terminal_v14 import emit


def postprocess(run_dir: Path | str, result: dict[str, Any]) -> dict[str, Any]:
    root = Path(run_dir)
    diagnostics: dict[str, Any] = {"txallo_terminal_breakdown_status": "unavailable",
                                   "txallo_artifact_reindex_status": "unavailable"}
    try:
        audit = emit(root)
        diagnostics["txallo_terminal_breakdown_status"] = audit.get("status", "unavailable")
        diagnostics["txallo_terminal_breakdown_source"] = "aggregate/txallo_terminal_breakdown.json"
        diagnostics["txallo_terminal_closed_epoch_count"] = audit.get("closed_epoch_count")
    except (OSError, ValueError, TypeError) as exc:
        diagnostics["txallo_terminal_breakdown_status"] = "failed_write:" + type(exc).__name__

    # The runner's catalog and result.artifacts were captured BEFORE this
    # scheduler's metric extractor emitted txallo_evidence_summary.json.
    # Reindex without recomputing the execution/formal eligibility gates.
    try:
        run_id = str(result.get("run_id") or "")
        if not run_id or not run_id.startswith("v5_"):
            raise ValueError("run_id_missing_or_invalid")
        frozen_path = root / "artifact_catalog.json"
        # The runner already SHA256-hashed every raw artifact. Rehashing the
        # entire WAL/node log tree here would double finalization I/O; refresh
        # only post-extraction files, preserving all prior verified entries.
        if not frozen_path.is_file():
            raise ValueError("runner_artifact_catalog_missing")
        catalog = json.loads(frozen_path.read_text(encoding="utf-8-sig"))
        previous = catalog.get("files")
        if not isinstance(previous, list) or any(not isinstance(x, dict) for x in previous):
            raise ValueError("runner_artifact_catalog_invalid")
        by_name = {str(item.get("name") or ""): item for item in previous}
        if len(by_name) != len(previous) or "" in by_name:
            raise ValueError("runner_artifact_catalog_duplicate_or_blank")
        observed = []
        for rel in ("aggregate/txallo_evidence_summary.json", "aggregate/txallo_terminal_breakdown.json"):
            path = root / rel
            if not path.is_file():
                continue
            by_name[rel] = catalog_entry(
                root, rel, path.stat().st_size,
                download_url=f"/api/v5/real-cluster/runs/{run_id}/artifacts/{rel}",
            )
            observed.append(rel)
        catalog["files"] = [by_name[name] for name in sorted(by_name)]
        catalog["file_count"] = len(catalog["files"])
        tmp = frozen_path.with_name(".artifact_catalog.txobs14.tmp")
        try:
            tmp.write_text(json.dumps(catalog, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
            os.replace(tmp, frozen_path)
        finally:
            tmp.unlink(missing_ok=True)
        artifacts = v5_real_cluster_artifacts.list_artifacts(root, run_id)
        names = {str(row.get("name") or "") for row in artifacts}
        if any(name not in names for name in observed):
            raise ValueError("postprocess_artifact_absent_from_list")
        result["artifacts"] = artifacts
        diagnostics["txallo_catalog_incremental_reindex"] = True
        diagnostics["txallo_artifact_reindex_status"] = "passed"
        diagnostics["txallo_postprocess_artifact_count"] = len(artifacts)
    except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
        # Annotation only: finality and formal eligibility are owned elsewhere.
        diagnostics["txallo_artifact_reindex_status"] = "failed:" + type(exc).__name__
        diagnostics["txallo_artifact_reindex_error"] = str(exc)
    return diagnostics
