"""Read-only projection of Porygon v5.3.2 in-memory consensus phase trace.

Copies *observed* phase rows, never invents times or modifies runtime truth.
Written only when the source consensus logs contain Porygon phase events.
"""
from __future__ import annotations

import csv
import os
import re
from pathlib import Path

PREFIX = "PORYGON_PROPOSAL_PHASE_"
OUTPUT = "porygon_proposal_phase_trace.csv"
FIELDS = ("node_id", "time_ms", "height", "view", "phase", "context", "block_hash")


def _phase_view(context: str) -> str:
    found = re.search(r"(?:^|;)view=(\d+)(?:;|$)", context)
    return found.group(1) if found else ""


def export_porygon_phase_trace(run_dir: Path) -> dict[str, object]:
    run_dir = Path(run_dir)
    logs = sorted((run_dir / "nodes").glob("*/consensus_message_log.csv"))
    if not logs:
        return {"porygon_proposal_phase_trace_available": False,
                "porygon_proposal_phase_trace_event_count": 0}
    target = run_dir / OUTPUT
    temporary = run_dir / ("." + OUTPUT + ".tmp")
    count = 0
    node_counts: dict[str, int] = {}
    try:
        with temporary.open("w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=FIELDS)
            writer.writeheader()
            for path in logs:
                with path.open("r", encoding="utf-8-sig", newline="") as inp:
                    reader = csv.DictReader(inp)
                    if not reader.fieldnames or "message_type" not in reader.fieldnames:
                        continue
                    for row in reader:
                        kind = str(row.get("message_type") or "")
                        if not kind.startswith(PREFIX):
                            continue
                        writer.writerow({
                            "node_id": path.parent.name,
                            "time_ms": row.get("timestamp") or row.get("time_ms") or "",
                            "height": row.get("height") or row.get("block_height") or "",
                            "view": row.get("view") or _phase_view(str(row.get("from_node") or "")),
                            "phase": kind[len(PREFIX):],
                            "context": row.get("from_node") or "",
                            "block_hash": row.get("block_hash") or "",
                        })
                        count += 1
                        node_counts[path.parent.name] = node_counts.get(path.parent.name, 0) + 1
        if count == 0:
            temporary.unlink(missing_ok=True)
            target.unlink(missing_ok=True)
            return {"porygon_proposal_phase_trace_available": False,
                    "porygon_proposal_phase_trace_event_count": 0}
        os.replace(temporary, target)
    finally:
        temporary.unlink(missing_ok=True)
    return {
        "porygon_proposal_phase_trace_available": True,
        "porygon_proposal_phase_trace_event_count": count,
        "porygon_proposal_phase_trace_node_counts": node_counts,
        "porygon_proposal_phase_trace_artifact": OUTPUT,
    }
