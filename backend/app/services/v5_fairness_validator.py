from __future__ import annotations

import csv
import json
from pathlib import Path


_FIXED = ("seed", "repeat_index", "execution_backend", "estimated_transactions")


def validate(rows: list[dict]) -> tuple[list[dict], dict]:
    checked: list[dict] = []
    groups: dict[str, list[dict]] = {}
    for row in rows:
        groups.setdefault(row["comparison_group_id"], []).append(row)
    failures: list[str] = []
    for group_id, items in groups.items():
        baseline = items[0]
        for row in items:
            blockers = list(row.get("blockers", []))
            for field in _FIXED:
                if row.get(field) != baseline.get(field): blockers.append(f"fairness mismatch: {field}")
            if blockers:
                row = {**row, "runnable": False, "blockers": blockers, "status": "blocked"}
                failures.append(f"{group_id}:{row.get('method_config_id')}")
            else:
                row = {**row, "status": "queued"}
            checked.append(row)
    return checked, {"passed": not failures, "failures": failures, "row_count": len(checked)}


def write_artifacts(root: Path, rows: list[dict], result: dict) -> None:
    root.mkdir(parents=True, exist_ok=True)
    (root / "fairness_validation.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    fields = ["child_run_id", "suite_type", "method_config_id", "fairness_key", "comparison_group_id", "seed", "repeat_index", "execution_backend", "estimated_transactions", "runnable", "status", "blockers"]
    with (root / "fairness_matrix.csv").open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in rows: writer.writerow({key: json.dumps(row.get(key)) if isinstance(row.get(key), (list, dict)) else row.get(key, "") for key in fields})
    with (root / "formal_matrix.csv").open("w", newline="", encoding="utf-8") as handle:
        fields = list(rows[0].keys()) if rows else ["child_run_id"]
        writer = csv.DictWriter(handle, fieldnames=fields); writer.writeheader()
        for row in rows: writer.writerow({key: json.dumps(value) if isinstance(value, (list, dict)) else value for key, value in row.items()})
