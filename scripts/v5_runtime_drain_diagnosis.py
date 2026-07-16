"""Read-only diagnosis for V5 real-cluster drain closure failures.

The script inspects existing run directories and writes summarized reports under
.cache/workloads/reports. It never mutates the inspected run directories.
"""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REPORT_DIR = ROOT / ".cache" / "workloads" / "reports"
DEFAULT_RUNS = {
    "original": ROOT / ".cache" / "v5_real_cluster_runs" / "v5_20260715_074706_ce68e037",
    "derived": ROOT / ".cache" / "v5_real_cluster_runs" / "v5_20260715_081727_78f6e44d",
}

STAGE_ORDER = [
    "submitted_only",
    "received",
    "admitted",
    "proposed",
    "quorum_committed",
    "durable_committed",
    "SourceLock",
    "RelayCertificate",
    "TargetCommit",
    "SourceFinalize",
    "Refund",
    "failed",
    "unknown",
]

RAW_STAGE_ALIASES = {
    "submitted": "submitted_only",
    "received": "received",
    "admitted": "admitted",
    "proposed": "proposed",
    "quorum_committed": "quorum_committed",
    "durable_committed": "durable_committed",
    "SourceLock": "SourceLock",
    "RelayCertificate": "RelayCertificate",
    "TargetCommit": "TargetCommit",
    "SourceFinalize": "SourceFinalize",
    "Refund": "Refund",
    "failed": "failed",
}


@dataclass
class TxTrace:
    logical_tx_id: str
    is_cross_shard: bool | None = None
    events: list[dict[str, Any]] = field(default_factory=list)
    source_shard: str = ""
    target_shard: str = ""
    shard: str = ""
    sender: str = ""
    nonce: str = ""
    node_ids: set[str] = field(default_factory=set)
    in_tx_index: bool = False
    in_receipt: bool = False
    in_committed_block: bool = False
    receipt_success: bool | None = None
    errors: list[str] = field(default_factory=list)


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def rel(path: Path) -> str:
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def add_event(tx: TxTrace, event: dict[str, Any]) -> None:
    tx.events.append(event)
    node_id = str(event.get("node_id") or "")
    if node_id:
        tx.node_ids.add(node_id)
    if event.get("source_shard") and not tx.source_shard:
        tx.source_shard = str(event["source_shard"])
    if event.get("target_shard") and not tx.target_shard:
        tx.target_shard = str(event["target_shard"])
    if event.get("shard_id") and not tx.shard:
        tx.shard = str(event["shard_id"])
    if event.get("error"):
        tx.errors.append(str(event["error"]))


def event_timestamp(event: dict[str, Any]) -> int:
    for key in ("timestamp_ms", "timestamp"):
        value = event.get(key)
        if value not in (None, ""):
            try:
                return int(value)
            except ValueError:
                return 0
    return 0


def max_stage(tx: TxTrace) -> str:
    seen = {RAW_STAGE_ALIASES.get(str(e.get("stage") or ""), str(e.get("stage") or "")) for e in tx.events}
    if tx.errors:
        return "failed"
    for stage in reversed(STAGE_ORDER):
        if stage in seen:
            return stage
    return "unknown"


def load_submitted_metadata(run_dir: Path, traces: dict[str, TxTrace]) -> set[str]:
    submitted: set[str] = set()
    for row in read_csv(run_dir / "client" / "client_lifecycle.csv"):
        logical = row.get("logical_tx_id") or row.get("tx_id")
        if not logical:
            continue
        submitted.add(logical)
        tx = traces.setdefault(logical, TxTrace(logical_tx_id=logical))
        add_event(tx, {"source": "client_lifecycle.csv", **row})
    for row in read_csv(run_dir / "client" / "routing_decision_log.csv"):
        logical = row.get("tx_id")
        if not logical:
            continue
        tx = traces.setdefault(logical, TxTrace(logical_tx_id=logical))
        tx.shard = row.get("assigned_shard", tx.shard)
        keys = row.get("access_keys", "")
        if "sender:" in keys:
            tx.sender = keys
    for row in read_csv(run_dir / "client" / "client_submission_log.csv"):
        logical = row.get("logical_tx_id") or row.get("tx_id")
        if not logical:
            continue
        tx = traces.setdefault(logical, TxTrace(logical_tx_id=logical))
        tx.sender = row.get("sender", tx.sender)
        tx.nonce = row.get("nonce", tx.nonce)
        if row.get("is_cross_shard") not in (None, ""):
            tx.is_cross_shard = row.get("is_cross_shard", "").lower() == "true"
    return submitted


def load_node_lifecycle(run_dir: Path, traces: dict[str, TxTrace]) -> None:
    for path in sorted((run_dir / "nodes").glob("n*/transaction_lifecycle.csv")):
        node_id = path.parent.name
        for row in read_csv(path):
            logical = row.get("logical_tx_id") or row.get("tx_id")
            if not logical:
                continue
            tx = traces.setdefault(logical, TxTrace(logical_tx_id=logical))
            add_event(tx, {"source": rel(path), "node_id": node_id, **row})


def load_cross_shard(run_dir: Path, traces: dict[str, TxTrace]) -> Counter:
    counter: Counter = Counter()
    for path in sorted((run_dir / "nodes").glob("n*/cross_shard_log.csv")):
        node_id = path.parent.name
        for row in read_csv(path):
            logical = row.get("tx_id")
            if not logical:
                continue
            counter[row.get("stage", "")] += 1
            tx = traces.setdefault(logical, TxTrace(logical_tx_id=logical))
            add_event(tx, {"source": rel(path), "node_id": node_id, **row})
    return counter


def load_committed_evidence(run_dir: Path, traces: dict[str, TxTrace]) -> dict[str, int]:
    evidence_counts = Counter()
    for path in sorted((run_dir / "nodes").glob("n*/tx_index.jsonl")):
        for row in read_jsonl(path):
            tx_id = row.get("tx_id")
            if not tx_id:
                continue
            traces.setdefault(tx_id, TxTrace(logical_tx_id=tx_id)).in_tx_index = True
            evidence_counts["tx_index_rows"] += 1
    for path in sorted((run_dir / "nodes").glob("n*/receipts.jsonl")):
        for row in read_jsonl(path):
            tx_id = row.get("tx_id")
            if not tx_id:
                continue
            tx = traces.setdefault(tx_id, TxTrace(logical_tx_id=tx_id))
            tx.in_receipt = True
            tx.receipt_success = bool(row.get("success"))
            evidence_counts["receipt_rows"] += 1
    for path in sorted((run_dir / "nodes").glob("n*/blocks.jsonl")):
        for row in read_jsonl(path):
            for tx_id in row.get("tx_ids", []) or []:
                traces.setdefault(tx_id, TxTrace(logical_tx_id=tx_id)).in_committed_block = True
                evidence_counts["block_tx_ids"] += 1
    return dict(evidence_counts)


def load_statuses(run_dir: Path) -> dict[str, Any]:
    statuses = {}
    for path in sorted((run_dir / "nodes").glob("n*/node_runtime_status.json")):
        status = read_json(path) or {}
        statuses[path.parent.name] = {
            "node_id": status.get("node_id", path.parent.name),
            "shard_id": status.get("shard_id"),
            "role": status.get("role"),
            "committed_height": status.get("committed_height"),
            "terminal_count": status.get("terminal_count"),
            "mempool_depth": status.get("mempool_depth"),
            "reserved_tx_count": status.get("reserved_tx_count"),
            "pending_commit_count": status.get("pending_commit_count"),
            "pending_cross_shard_count": status.get("pending_cross_shard_count"),
            "proposal_in_flight": status.get("proposal_in_flight"),
            "relay_admission_failures": status.get("relay_admission_failures") or {},
            "failed_count": len(status.get("failed_logical_tx_ids") or []),
            "source_finalized_count": len(status.get("source_finalized_logical_tx_ids") or []),
            "last_progress_at": status.get("last_progress_at"),
        }
    return statuses


def summarize_progress(run_dir: Path) -> dict[str, Any]:
    rows = read_csv(run_dir / "drain_progress.csv")
    if not rows:
        return {}
    first = rows[0]
    last = rows[-1]
    last_ts = int(last["timestamp"])
    five_min_cutoff = last_ts - 300_000
    window = [r for r in rows if int(r["timestamp"]) >= five_min_cutoff]
    first_window = window[0] if window else last

    def delta(key: str) -> int:
        return int(last.get(key) or 0) - int(first_window.get(key) or 0)

    return {
        "first": first,
        "last": last,
        "sample_count": len(rows),
        "last_5_minute_delta": {
            "terminal": delta("terminal"),
            "min_validator_height": delta("min_validator_height"),
            "max_validator_height": delta("max_validator_height"),
            "mempool": delta("total_mempool_depth"),
            "reserved": delta("total_reserved_tx"),
            "pending": delta("pending_total"),
        },
        "last_terminal_progress_at": last.get("last_terminal_progress_at"),
        "last_mempool_progress_at": last.get("last_mempool_progress_at"),
    }


def classify_completion(tx: TxTrace) -> str:
    if max_stage(tx) == "SourceFinalize":
        return "B_completed_but_terminal_accounting_missing"
    if tx.is_cross_shard is False and (tx.in_tx_index or tx.in_receipt or tx.in_committed_block):
        return "B_completed_but_terminal_accounting_missing"
    if tx.is_cross_shard is True and (tx.in_tx_index or tx.in_receipt or tx.in_committed_block):
        return "C_only_missing_source_finalize"
    if max_stage(tx) in {"SourceLock", "RelayCertificate", "TargetCommit"}:
        return "C_only_missing_source_finalize"
    if max_stage(tx) == "failed":
        return "A_failed_or_rejected"
    return "A_actual_not_completed"


def summarize_run(label: str, run_dir: Path) -> dict[str, Any]:
    traces: dict[str, TxTrace] = {}
    submitted = load_submitted_metadata(run_dir, traces)
    load_node_lifecycle(run_dir, traces)
    cross_counts = load_cross_shard(run_dir, traces)
    evidence_counts = load_committed_evidence(run_dir, traces)
    statuses = load_statuses(run_dir)
    drain_status = read_json(run_dir / "drain_status.json") or {}
    stalled_report = read_json(run_dir / "stalled_runtime_report.json") or {}
    workload_summary = read_json(run_dir / "workload_replay_summary.json") or read_json(
        run_dir / "client" / "workload_replay_summary.json"
    ) or {}
    process_manifest = read_json(run_dir / "process_manifest.json") or {}
    compiled = read_json(run_dir / "compiled_run_plan.json") or {}
    compiled_workload = read_json(run_dir / "compiled_workload_plan.json") or {}

    terminal_ids = set()
    durable_ids = set()
    source_finalized_ids = set()
    refunded_ids = set()
    failed_ids = set()
    for status in (stalled_report.get("last_statuses") or []):
        durable_ids.update(status.get("durable_committed_logical_tx_ids") or [])
        source_finalized_ids.update(status.get("source_finalized_logical_tx_ids") or [])
        refunded_ids.update(status.get("refunded_logical_tx_ids") or [])
        failed_ids.update(status.get("failed_logical_tx_ids") or [])
    terminal_ids.update(source_finalized_ids)
    terminal_ids.update(refunded_ids)
    terminal_ids.update(failed_ids)
    for logical, tx in traces.items():
        if tx.is_cross_shard is False and logical in durable_ids:
            terminal_ids.add(logical)
        if tx.is_cross_shard is True and logical in source_finalized_ids:
            terminal_ids.add(logical)
        if tx.is_cross_shard is None and logical in durable_ids and logical not in submitted:
            terminal_ids.add(logical)
    for tx_id, tx in traces.items():
        if max_stage(tx) in {"SourceFinalize", "Refund", "durable_committed"}:
            if max_stage(tx) == "durable_committed" and tx.is_cross_shard is True:
                continue
            terminal_ids.add(tx_id)

    incomplete_ids = sorted(submitted - terminal_ids)
    stage_distribution: Counter = Counter()
    completion_classification: Counter = Counter()
    shard_distribution: Counter = Counter()
    node_distribution: Counter = Counter()
    sender_distribution: Counter = Counter()
    source_target_distribution: Counter = Counter()
    nonce_values: list[int] = []
    event_times: list[int] = []

    examples = []
    for logical in incomplete_ids:
        tx = traces.get(logical, TxTrace(logical_tx_id=logical))
        stage = max_stage(tx)
        stage_distribution[stage] += 1
        completion_classification[classify_completion(tx)] += 1
        if tx.shard:
            shard_distribution[tx.shard] += 1
        for node_id in tx.node_ids:
            node_distribution[node_id] += 1
        if tx.sender:
            sender_distribution[tx.sender] += 1
        if tx.source_shard or tx.target_shard:
            source_target_distribution[f"{tx.source_shard}->{tx.target_shard}"] += 1
        if tx.nonce:
            try:
                nonce_values.append(int(tx.nonce))
            except ValueError:
                pass
        event_times.extend([event_timestamp(e) for e in tx.events if event_timestamp(e)])
        if len(examples) < 30:
            examples.append(
                {
                    "logical_tx_id": logical,
                    "last_stage": stage,
                    "completion_classification": classify_completion(tx),
                    "in_tx_index": tx.in_tx_index,
                    "in_receipt": tx.in_receipt,
                    "in_committed_block": tx.in_committed_block,
                    "is_cross_shard": tx.is_cross_shard,
                    "source_shard": tx.source_shard,
                    "target_shard": tx.target_shard,
                    "shard": tx.shard,
                    "node_ids": sorted(tx.node_ids),
                    "sender": tx.sender[:120],
                    "nonce": tx.nonce,
                    "events": sorted(tx.events, key=event_timestamp),
                }
            )

    progress = summarize_progress(run_dir)
    last_delta = progress.get("last_5_minute_delta", {})
    stalled = (
        int(last_delta.get("terminal") or 0) == 0
        and int(last_delta.get("min_validator_height") or 0) == 0
        and int(last_delta.get("max_validator_height") or 0) == 0
    )
    if completion_classification.get("B_completed_but_terminal_accounting_missing", 0) == len(incomplete_ids):
        conclusion = "terminal_accounting_bug"
    elif stalled:
        conclusion = "stalled_correctness_bug"
    elif int(last_delta.get("terminal") or 0) > 0:
        conclusion = "slow_but_progressing"
    else:
        conclusion = "mixed"

    return {
        "label": label,
        "run_dir": rel(run_dir),
        "exists": run_dir.exists(),
        "compiled_child_id": compiled.get("child_id") or compiled.get("compiled_run_id"),
        "compiled_workload_identity": {
            key: compiled_workload.get(key)
            for key in (
                "plugin_id",
                "source_type",
                "dataset_id",
                "variant_id",
                "variant_mode",
                "materialized_id",
                "materialized_sha256",
                "target_alpha",
                "seed",
                "requested_tx_count",
                "actual_tx_count",
                "truth_label",
            )
        },
        "drain_status": drain_status,
        "stalled_report_classifiers": stalled_report.get("classifiers", []),
        "process_manifest": {
            "expected_process_count": process_manifest.get("expected_process_count"),
            "one_node_one_os_process": process_manifest.get("one_node_one_os_process"),
            "process_count": len(process_manifest.get("processes") or []),
        },
        "workload_summary": {
            key: workload_summary.get(key)
            for key in (
                "expected_count",
                "read_count",
                "submitted_count",
                "signature_pass_count",
                "nonce_continuity",
                "expected_cross_shard_count",
                "actual_cross_shard_count",
                "actual_cross_shard_ratio",
                "no_fallback",
            )
        },
        "submitted_count": len(submitted),
        "terminal_evidence_count": len(terminal_ids & submitted),
        "durable_evidence_count": len(durable_ids & submitted),
        "source_finalized_evidence_count": len(source_finalized_ids & submitted),
        "refunded_evidence_count": len(refunded_ids & submitted),
        "failed_evidence_count": len(failed_ids & submitted),
        "incomplete_count": len(incomplete_ids),
        "incomplete_stage_distribution": dict(stage_distribution),
        "incomplete_completion_classification": dict(completion_classification),
        "incomplete_shard_distribution": dict(shard_distribution),
        "incomplete_node_distribution": dict(node_distribution),
        "incomplete_sender_top10": sender_distribution.most_common(10),
        "incomplete_nonce_range": [min(nonce_values), max(nonce_values)] if nonce_values else None,
        "incomplete_source_target_distribution": dict(source_target_distribution),
        "incomplete_event_time_range": [min(event_times), max(event_times)] if event_times else None,
        "last_incomplete_progress_at": max(event_times) if event_times else None,
        "cross_shard_stage_counts": dict(cross_counts),
        "committed_evidence_counts": evidence_counts,
        "node_statuses": statuses,
        "progress": progress,
        "diagnosis_conclusion": conclusion,
        "representative_incomplete": examples,
        "hypothesis_evidence": build_hypothesis_evidence(statuses, cross_counts, progress, stalled_report, len(incomplete_ids)),
    }


def build_hypothesis_evidence(
    statuses: dict[str, Any],
    cross_counts: Counter,
    progress: dict[str, Any],
    stalled_report: dict[str, Any],
    incomplete_count: int,
) -> dict[str, Any]:
    relay_failures = {
        node: status.get("relay_admission_failures")
        for node, status in statuses.items()
        if status.get("relay_admission_failures")
    }
    pending_cross = sum(int(status.get("pending_cross_shard_count") or 0) for status in statuses.values())
    reserved = sum(int(status.get("reserved_tx_count") or 0) for status in statuses.values())
    pending_commit = sum(int(status.get("pending_commit_count") or 0) for status in statuses.values())
    mempool = sum(int(status.get("mempool_depth") or 0) for status in statuses.values())
    proposal_in_flight = any(bool(status.get("proposal_in_flight")) for status in statuses.values())
    last_delta = progress.get("last_5_minute_delta", {})
    return {
        "target_commit_finalize_send_or_retry_gap": {
            "evidence": {
                "TargetCommit": cross_counts.get("TargetCommit", 0),
                "SourceFinalize": cross_counts.get("SourceFinalize", 0),
                "incomplete_count": incomplete_count,
            },
            "supported": cross_counts.get("TargetCommit", 0) > cross_counts.get("SourceFinalize", 0),
        },
        "source_lock_or_relay_certificate_single_send_loss": {
            "evidence": dict(cross_counts),
            "supported": cross_counts.get("SourceLock", 0) > cross_counts.get("RelayCertificate", 0),
        },
        "retry_pending_relays_finalize_gap": {
            "evidence": {"pending_cross_total": pending_cross, "relay_failures": relay_failures},
            "supported": pending_cross == 0 and cross_counts.get("TargetCommit", 0) > cross_counts.get("SourceFinalize", 0),
        },
        "reserved_not_released": {"evidence": {"reserved_total": reserved}, "supported": reserved > 0},
        "proposal_or_commit_inflight_stuck": {
            "evidence": {"pending_commit_total": pending_commit, "proposal_in_flight": proposal_in_flight},
            "supported": pending_commit > 0 or proposal_in_flight,
        },
        "terminal_accounting_missing": {
            "evidence": {
                "classifiers": stalled_report.get("classifiers", []),
                "mempool_total": mempool,
                "reserved_total": reserved,
                "pending_cross_total": pending_cross,
                "pending_commit_total": pending_commit,
            },
            "supported": "terminal_accounting_missing" in (stalled_report.get("classifiers") or []),
        },
        "slow_but_progressing": {"evidence": last_delta, "supported": int(last_delta.get("terminal") or 0) > 0},
    }


def write_markdown(report: dict[str, Any], output_path: Path) -> None:
    lines = [
        "# V5 Runtime Drain Diagnosis",
        "",
        f"Generated report: `{rel(output_path)}`",
        "",
    ]
    for label, run in report["runs"].items():
        lines.extend(
            [
                f"## {label}",
                "",
                f"- run_dir: `{run['run_dir']}`",
                f"- conclusion: `{run['diagnosis_conclusion']}`",
                f"- submitted: {run['submitted_count']}",
                f"- terminal evidence: {run['terminal_evidence_count']}",
                f"- incomplete: {run['incomplete_count']}",
                f"- drain status: terminal={run['drain_status'].get('terminal')} incomplete={run['drain_status'].get('incomplete')} phase={run['drain_status'].get('phase')}",
                f"- stage distribution: `{run['incomplete_stage_distribution']}`",
                f"- completion classification: `{run['incomplete_completion_classification']}`",
                f"- source/target distribution: `{run['incomplete_source_target_distribution']}`",
                f"- last 5 minute progress delta: `{run['progress'].get('last_5_minute_delta')}`",
                f"- cross-shard stages: `{run['cross_shard_stage_counts']}`",
                "",
                "### Hypothesis Evidence",
                "",
            ]
        )
        for name, evidence in run["hypothesis_evidence"].items():
            lines.append(f"- {name}: supported={evidence['supported']} evidence=`{evidence['evidence']}`")
        lines.extend(["", "### Representative Incomplete IDs", ""])
        for item in run["representative_incomplete"][:30]:
            lines.append(
                f"- `{item['logical_tx_id']}` last={item['last_stage']} class={item['completion_classification']} "
                f"tx_index={item['in_tx_index']} receipt={item['in_receipt']} block={item['in_committed_block']} "
                f"route={item['source_shard']}->{item['target_shard']}"
            )
        lines.append("")
    output_path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--original-run-dir", type=Path, default=DEFAULT_RUNS["original"])
    parser.add_argument("--derived-run-dir", type=Path, default=DEFAULT_RUNS["derived"])
    args = parser.parse_args()

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    report = {
        "schema_version": "v5_runtime_drain_diagnosis_v1",
        "read_only": True,
        "runs": {
            "original": summarize_run("original", args.original_run_dir.resolve()),
            "derived": summarize_run("derived", args.derived_run_dir.resolve()),
        },
    }
    json_path = REPORT_DIR / "v5_runtime_drain_diagnosis.json"
    md_path = REPORT_DIR / "v5_runtime_drain_diagnosis.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")
    write_markdown(report, md_path)
    print(json.dumps({"json": rel(json_path), "markdown": rel(md_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
