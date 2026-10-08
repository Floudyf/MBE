from __future__ import annotations

import csv
import io
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mbe_metatrack_hol_externality_v1_1"
FULL_ID = "metatrack_latest"
NO_TRACK_ID = "metatrack_ab_track"


def _bool(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return default
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _quantile(values: list[float], q: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    pos = (len(ordered) - 1) * q
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return ordered[lo]
    return ordered[lo] * (hi - pos) + ordered[hi] * (pos - lo)


def _mean(values: list[float]) -> float | None:
    return statistics.fmean(values) if values else None


def _artifact_bytes(run_dir: Path, relative_name: str) -> bytes:
    direct = run_dir / Path(relative_name)
    if direct.is_file():
        return direct.read_bytes()
    # Cold archives are first-class evidence. This API verifies SHA-256/size
    # while streaming either legacy-path or exact-dedup tar.zst layouts.
    from backend.app.services.v5_artifact_storage import stream_archived_artifact
    return b"".join(stream_archived_artifact(run_dir, relative_name))


def _artifact_exists(run_dir: Path, relative_name: str) -> bool:
    direct = run_dir / Path(relative_name)
    if direct.is_file():
        return True
    try:
        from backend.app.services.v5_artifact_storage import archived_artifact_info
        archived_artifact_info(run_dir, relative_name)
        return True
    except (FileNotFoundError, RuntimeError, ValueError):
        return False


def _csv_rows(run_dir: Path, relative_name: str) -> list[dict[str, str]]:
    raw = _artifact_bytes(run_dir, relative_name).decode("utf-8-sig", errors="replace")
    return list(csv.DictReader(io.StringIO(raw)))


def _json(run_dir: Path, relative_name: str) -> dict[str, Any]:
    return json.loads(_artifact_bytes(run_dir, relative_name).decode("utf-8"))


def _representative_nodes(run_dir: Path) -> list[tuple[str, str]]:
    cluster = _json(run_dir, "real_cluster_summary.json")
    by_shard: dict[str, list[str]] = defaultdict(list)
    for item in cluster.get("node_summaries") or []:
        if not isinstance(item, dict):
            continue
        node = str(item.get("node_id") or "")
        shard = str(item.get("shard_id") or "")
        if node and shard:
            by_shard[shard].append(node)
    if not by_shard:
        # Fallback for old artifacts: inspect preserved online node shells.
        for node_dir in sorted((run_dir / "nodes").glob("*")):
            summary = node_dir / "node_summary.json"
            if not summary.is_file():
                continue
            payload = json.loads(summary.read_text(encoding="utf-8"))
            node = str(payload.get("node_id") or node_dir.name)
            shard = str(payload.get("shard_id") or "")
            if shard:
                by_shard[shard].append(node)
    if not by_shard:
        # Fully cold legacy runs may retain only archive metadata online. Probe
        # the bounded V5 node-id space through archive-aware artifact lookup.
        for index in range(64):
            node = f"n{index}"
            name = f"nodes/{node}/node_summary.json"
            if not _artifact_exists(run_dir, name):
                continue
            payload = _json(run_dir, name)
            shard = str(payload.get("shard_id") or "")
            if shard:
                by_shard[shard].append(node)
    return [(shard, sorted(nodes)[0]) for shard, nodes in sorted(by_shard.items()) if nodes]


def _structure_rows(run_dir: Path, reps: list[tuple[str, str]]) -> dict[tuple[str, int, str], dict[str, Any]]:
    out: dict[tuple[str, int, str], dict[str, Any]] = {}
    for shard, node in reps:
        name = f"nodes/{node}/metatrack_dependency_structure.csv"
        if not _artifact_exists(run_dir, name):
            continue
        for row in _csv_rows(run_dir, name):
            if str(row.get("shard_id") or shard) != shard:
                continue
            key = (shard, _int(row.get("height")), str(row.get("tx_id") or ""))
            if not key[2]:
                continue
            parsed = dict(row)
            parsed["_width"] = _int(row.get("matrix_frontier_width"), _int(row.get("legacy_frontier_width")))
            parsed["_ordinal"] = _int(row.get("routing_ordinal"))
            parsed["_depth"] = _int(row.get("dependency_depth_l"))
            parsed["_tail"] = _int(row.get("tail_depth_h"))
            parsed["_desc"] = _int(row.get("descendant_count_d"))
            parsed["_critical"] = _bool(row.get("critical_path"))
            parsed["_raw_preds"] = [v for v in str(row.get("raw_producer_ids") or "").split("|") if v]
            out[key] = parsed
    return out


def _attempt_rows(run_dir: Path, reps: list[tuple[str, str]]) -> dict[tuple[str, int, str], dict[str, Any]]:
    out: dict[tuple[str, int, str], dict[str, Any]] = {}
    for shard, node in reps:
        name = f"nodes/{node}/business_execute_invocation_count_by_node.csv"
        if not _artifact_exists(run_dir, name):
            continue
        for row in _csv_rows(run_dir, name):
            if not _bool(row.get("final_completion")):
                continue
            key = (shard, _int(row.get("block_height")), str(row.get("tx_id") or ""))
            if not key[2]:
                continue
            out[key] = dict(row)
    return out


def _exact_block_hol(run_dir: Path, reps: list[tuple[str, str]]) -> dict[tuple[str, int, str], float]:
    out: dict[tuple[str, int, str], float] = {}
    for shard, node in reps:
        name = f"nodes/{node}/block_execution_summary.json"
        if not _artifact_exists(run_dir, name):
            continue
        payload = _json(run_dir, name)
        for block in payload.get("blocks") or []:
            if not isinstance(block, dict):
                continue
            value = block.get("metatrack_single_fifo_ready_blocked_sum_ms")
            if value is None:
                continue
            key = (shard, _int(block.get("height")), str(block.get("block_hash") or ""))
            out[key] = _float(value)
    return out


def _block_graph(rows: list[dict[str, Any]]) -> dict[str, set[str]]:
    children: dict[str, set[str]] = defaultdict(set)
    known = {str(row.get("tx_id") or "") for row in rows}
    for row in rows:
        tx = str(row.get("tx_id") or "")
        for pred in row.get("_raw_preds") or []:
            if pred in known and tx:
                children[pred].add(tx)
    reach: dict[str, set[str]] = {}

    def visit(tx: str) -> set[str]:
        if tx in reach:
            return reach[tx]
        result: set[str] = set()
        for child in children.get(tx, set()):
            result.add(child)
            result.update(visit(child))
        reach[tx] = result
        return result

    for tx in known:
        visit(tx)
    return reach


def _structural_summary(structure: dict[tuple[str, int, str], dict[str, Any]]) -> dict[str, Any]:
    groups = {"single_or_zero": [], "multi": []}
    for row in structure.values():
        groups["multi" if row["_width"] >= 2 else "single_or_zero"].append(row)

    payload: dict[str, Any] = {}
    for name, rows in groups.items():
        desc = [float(r["_desc"]) for r in rows]
        tail = [float(r["_tail"]) for r in rows]
        depth = [float(r["_depth"]) for r in rows]
        payload[name] = {
            "transaction_count": len(rows),
            "mean_descendant_count": _mean(desc),
            "p95_descendant_count": _quantile(desc, 0.95),
            "mean_tail_depth": _mean(tail),
            "p95_tail_depth": _quantile(tail, 0.95),
            "mean_dependency_depth": _mean(depth),
            "p95_dependency_depth": _quantile(depth, 0.95),
            "critical_path_count": sum(1 for r in rows if r["_critical"]),
            "critical_path_rate": (
                sum(1 for r in rows if r["_critical"]) / len(rows) if rows else None
            ),
        }
    total = len(structure)
    multi = len(groups["multi"])
    payload["multi_transaction_share"] = multi / total if total else None
    return payload


def _full_timing_summary(
    structure: dict[tuple[str, int, str], dict[str, Any]],
    attempts: dict[tuple[str, int, str], dict[str, Any]],
) -> dict[str, Any]:
    grouped: dict[str, dict[str, list[float]]] = {
        "single_or_zero": defaultdict(list),
        "multi": defaultdict(list),
    }
    for key, srow in structure.items():
        arow = attempts.get(key)
        if not arow:
            continue
        group = "multi" if srow["_width"] >= 2 else "single_or_zero"
        for column in ("sojourn_ns", "state_wait_ns", "dependency_wait_ns", "queue_wait_ns"):
            grouped[group][column].append(_float(arow.get(column)) / 1_000_000.0)
    out: dict[str, Any] = {}
    for group, fields in grouped.items():
        out[group] = {}
        for column, values in fields.items():
            stem = column.removesuffix("_ns") + "_ms"
            out[group][f"mean_{stem}"] = _mean(values)
            out[group][f"p95_{stem}"] = _quantile(values, 0.95)
            out[group][f"p99_{stem}"] = _quantile(values, 0.99)
            out[group][f"sample_count_{stem}"] = len(values)
    return out


def _fifo_externality(
    structure: dict[tuple[str, int, str], dict[str, Any]],
    attempts: dict[tuple[str, int, str], dict[str, Any]],
    exact_by_block: dict[tuple[str, int, str], float],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    by_block: dict[tuple[str, int, str], list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for key, srow in structure.items():
        arow = attempts.get(key)
        if not arow:
            continue
        block_hash = str(arow.get("block_hash") or srow.get("block_hash") or "")
        by_block[(key[0], key[1], block_hash)].append((srow, arow))

    blocker_acc: dict[tuple[str, int, str], dict[str, Any]] = {}
    victims: list[dict[str, Any]] = []
    reconstructed_hol_ms = 0.0
    exact_runtime_mode = False
    matrix_pair_count: dict[str, int] = defaultdict(int)
    matrix_hol_ms: dict[str, float] = defaultdict(float)

    for block_key, pairs in sorted(by_block.items()):
        pairs.sort(key=lambda pair: pair[0]["_ordinal"])
        srows = [pair[0] for pair in pairs]
        reach = _block_graph(srows)

        ready: list[int] = []
        release: list[int] = []
        hol: list[int] = []
        exact_fields_present = all(
            "fifo_ready_offset_ns" in a and "fifo_release_offset_ns" in a and "fifo_ready_blocked_ns" in a
            for _, a in pairs
        )
        if exact_fields_present:
            exact_runtime_mode = True

        prefix_release = 0
        prefix_blocker_index = -1
        for idx, (srow, arow) in enumerate(pairs):
            if exact_fields_present:
                ready_ns = max(0, _int(arow.get("fifo_ready_offset_ns")))
                release_ns = max(ready_ns, _int(arow.get("fifo_release_offset_ns"), ready_ns))
                hol_ns = max(0, _int(arow.get("fifo_ready_blocked_ns")))
            else:
                start_ns = max(0, _int(arow.get("attempt_start_offset_ns")))
                queue_wait_ns = max(0, _int(arow.get("queue_wait_ns")))
                # Legacy reconstruction. QueueWaitNS starts when enqueueReady marks
                # the tx ready and ends at dispatch. Subtracting it from worker
                # start gives ready time plus only the small dispatch->worker lag.
                ready_ns = max(0, start_ns - queue_wait_ns)
                if ready_ns > prefix_release:
                    prefix_release = ready_ns
                    prefix_blocker_index = idx
                release_ns = prefix_release
                hol_ns = max(0, release_ns - ready_ns)

            ready.append(ready_ns)
            release.append(release_ns)
            hol.append(hol_ns)

            if exact_fields_present:
                # Keep prefix blocker reconstruction independent from the exact
                # victim duration so exact runtime evidence can audit attribution.
                if ready_ns > prefix_release:
                    prefix_release = ready_ns
                    prefix_blocker_index = idx
            if hol_ns <= 0:
                continue

            # Dominant FIFO head = preceding canonical tx whose readiness forms
            # the prefix maximum that determines this victim's release boundary.
            candidates = [
                j for j in range(idx)
                if ready[j] <= release_ns and release[j] <= release_ns
            ]
            dominant = None
            if candidates:
                dominant = max(candidates, key=lambda j: (ready[j], srows[j]["_ordinal"]))
                # Prefer an exact prefix-max match where possible.
                matching = [j for j in candidates if ready[j] == release_ns]
                if matching:
                    dominant = max(matching, key=lambda j: srows[j]["_ordinal"])
            if dominant is None:
                continue

            blocker = srows[dominant]
            victim = srow
            blocker_tx = str(blocker.get("tx_id") or "")
            victim_tx = str(victim.get("tx_id") or "")
            victim_fast_eligible = victim["_width"] < 2
            blocker_multi = blocker["_width"] >= 2
            descendant = victim_tx in reach.get(blocker_tx, set())
            hol_ms = hol_ns / 1_000_000.0
            reconstructed_hol_ms += hol_ms
            pair_label = (
                ("multi" if blocker_multi else "single_or_zero")
                + "->"
                + ("single_or_zero" if victim_fast_eligible else "multi")
            )
            matrix_pair_count[pair_label] += 1
            matrix_hol_ms[pair_label] += hol_ms

            bkey = (block_key[0], block_key[1], blocker_tx)
            acc = blocker_acc.setdefault(
                bkey,
                {
                    "shard_id": block_key[0],
                    "block_height": block_key[1],
                    "block_hash": block_key[2],
                    "blocker_tx_id": blocker_tx,
                    "routing_ordinal": blocker["_ordinal"],
                    "matrix_frontier_width": blocker["_width"],
                    "dependency_depth_l": blocker["_depth"],
                    "tail_depth_h": blocker["_tail"],
                    "descendant_count_d": blocker["_desc"],
                    "critical_path": blocker["_critical"],
                    "victim_count": 0,
                    "fast_eligible_victim_count": 0,
                    "non_descendant_fast_victim_count": 0,
                    "attributed_hol_ms": 0.0,
                    "fast_eligible_hol_ms": 0.0,
                    "non_descendant_fast_hol_ms": 0.0,
                },
            )
            acc["victim_count"] += 1
            acc["attributed_hol_ms"] += hol_ms
            if victim_fast_eligible:
                acc["fast_eligible_victim_count"] += 1
                acc["fast_eligible_hol_ms"] += hol_ms
                if not descendant:
                    acc["non_descendant_fast_victim_count"] += 1
                    acc["non_descendant_fast_hol_ms"] += hol_ms

            victims.append(
                {
                    "shard_id": block_key[0],
                    "block_height": block_key[1],
                    "block_hash": block_key[2],
                    "blocker_tx_id": blocker_tx,
                    "blocker_width": blocker["_width"],
                    "blocker_critical_path": blocker["_critical"],
                    "blocker_descendant_count": blocker["_desc"],
                    "victim_tx_id": victim_tx,
                    "victim_width": victim["_width"],
                    "victim_fast_eligible": victim_fast_eligible,
                    "victim_is_dag_descendant": descendant,
                    "fifo_hol_ms": hol_ms,
                    "evidence_mode": "exact_runtime" if exact_fields_present else "legacy_estimated",
                }
            )

    blockers = list(blocker_acc.values())
    blockers.sort(key=lambda row: (-float(row["attributed_hol_ms"]), -int(row["victim_count"])))

    exact_total_ms = sum(exact_by_block.values())
    calibration_error_pct = (
        abs(reconstructed_hol_ms - exact_total_ms) / exact_total_ms * 100.0
        if exact_total_ms > 0
        else None
    )

    total_hol = sum(float(row["attributed_hol_ms"]) for row in blockers)
    multi_blockers = [row for row in blockers if int(row["matrix_frontier_width"]) >= 2]
    multi_hol = sum(float(row["attributed_hol_ms"]) for row in multi_blockers)
    multi_fast_hol = sum(float(row["fast_eligible_hol_ms"]) for row in multi_blockers)
    multi_non_desc_fast_hol = sum(float(row["non_descendant_fast_hol_ms"]) for row in multi_blockers)

    all_struct = list(structure.values())
    multi_tx = sum(1 for row in all_struct if row["_width"] >= 2)
    tx_share = multi_tx / len(all_struct) if all_struct else None
    hol_share = multi_hol / total_hol if total_hol > 0 else None
    amplification = hol_share / tx_share if hol_share is not None and tx_share else None

    summary = {
        "evidence_mode": "exact_runtime" if exact_runtime_mode else "legacy_estimated",
        "transaction_count_with_structure": len(structure),
        "transaction_count_with_attempt": len(attempts),
        "fifo_hol_victim_count": len(victims),
        "fifo_hol_reconstructed_ms": reconstructed_hol_ms,
        "fifo_hol_exact_runtime_aggregate_ms": exact_total_ms if exact_total_ms > 0 else None,
        "fifo_hol_calibration_abs_error_pct": calibration_error_pct,
        "dominant_blocker_count": len(blockers),
        "multi_frontier_blocker_count": len(multi_blockers),
        "multi_frontier_transaction_share": tx_share,
        "multi_frontier_attributed_hol_ms": multi_hol,
        "multi_frontier_hol_share": hol_share,
        "multi_frontier_hol_amplification_vs_tx_share": amplification,
        "multi_frontier_fast_eligible_hol_ms": multi_fast_hol,
        "multi_frontier_non_descendant_fast_hol_ms": multi_non_desc_fast_hol,
        "multi_frontier_fast_eligible_victim_count": sum(int(row["fast_eligible_victim_count"]) for row in multi_blockers),
        "multi_frontier_non_descendant_fast_victim_count": sum(int(row["non_descendant_fast_victim_count"]) for row in multi_blockers),
        "multi_frontier_critical_path_blocker_count": sum(1 for row in multi_blockers if row["critical_path"]),
        "blocker_victim_matrix_pair_count": dict(sorted(matrix_pair_count.items())),
        "blocker_victim_matrix_hol_ms": dict(sorted(matrix_hol_ms.items())),
        "attribution_semantics": "dominant_prefix_ready_head",
        "legacy_estimation_semantics": (
            "ready ~= attempt_start_offset - queue_wait; calibrated against exact per-block FIFO blocked aggregate"
        ),
    }
    return summary, blockers, victims



def _execution_trace_identity(
    run_dir: Path,
    reps: list[tuple[str, str]],
) -> tuple[
    dict[tuple[str, int, str], str],
    dict[tuple[str, str], dict[str, Any]],
]:
    """Map physical tx identity to stable logical identity for paired-run joins."""
    by_physical: dict[tuple[str, int, str], str] = {}
    by_logical: dict[tuple[str, str], dict[str, Any]] = {}
    for shard, node in reps:
        name = f"nodes/{node}/transaction_execution_trace.csv"
        if not _artifact_exists(run_dir, name):
            continue
        for row in _csv_rows(run_dir, name):
            tx_id = str(row.get("tx_id") or "")
            logical_id = str(row.get("logical_tx_id") or "")
            height = _int(row.get("height"))
            if not tx_id or not logical_id:
                continue
            pkey = (shard, height, tx_id)
            previous = by_physical.get(pkey)
            if previous not in (None, logical_id):
                raise RuntimeError(f"physical tx maps to multiple logical ids: {pkey}")
            by_physical[pkey] = logical_id

            lkey = (shard, logical_id)
            value = {
                "tx_id": tx_id,
                "height": height,
                "block_hash": str(row.get("block_hash") or ""),
            }
            previous_logical = by_logical.get(lkey)
            if previous_logical is not None and (
                previous_logical["tx_id"] != tx_id
                or previous_logical["height"] != height
            ):
                raise RuntimeError(
                    f"logical tx executed more than once on representative shard: {lkey}"
                )
            by_logical[lkey] = value
    return by_physical, by_logical


def _paired_counterfactual_structure(
    full_dir: Path,
    no_track_dir: Path,
    full_reps: list[tuple[str, str]],
    no_track_reps: list[tuple[str, str]],
    no_track_attempts: dict[tuple[str, int, str], dict[str, Any]],
) -> tuple[dict[tuple[str, int, str], dict[str, Any]], dict[str, Any]]:
    """Project Full's structural frontier labels onto the paired no-track run.

    Historical `metatrack_single_execution` runs intentionally did not call the
    dual-track classifier and therefore wrote an empty dependency-structure CSV.
    Full and no-track are paired by seed/repeat and share routing + block producer.
    We join through transaction_execution_trace.logical_tx_id, never through a
    cross-run physical tx id guess.
    """
    full_structure = _structure_rows(full_dir, full_reps)
    if not full_structure:
        raise RuntimeError(
            f"paired Full dependency structure unavailable in {full_dir}"
        )

    full_phys_to_logical, full_logical_to_phys = _execution_trace_identity(
        full_dir, full_reps
    )
    no_phys_to_logical, no_logical_to_phys = _execution_trace_identity(
        no_track_dir, no_track_reps
    )
    if not full_phys_to_logical or not no_phys_to_logical:
        raise RuntimeError(
            "transaction_execution_trace logical identity mapping unavailable"
        )

    full_by_logical: dict[tuple[str, str], dict[str, Any]] = {}
    source_height_by_logical: dict[tuple[str, str], int] = {}
    for (shard, height, tx_id), row in full_structure.items():
        logical_id = full_phys_to_logical.get((shard, height, tx_id))
        if not logical_id:
            continue
        key = (shard, logical_id)
        if key in full_by_logical:
            raise RuntimeError(f"duplicate Full structure logical identity: {key}")
        full_by_logical[key] = row
        source_height_by_logical[key] = height

    projected: dict[tuple[str, int, str], dict[str, Any]] = {}
    missing_logical = 0
    missing_structure = 0
    height_match = 0
    predecessor_total = 0
    predecessor_mapped = 0

    for (shard, height, tx_id), attempt in no_track_attempts.items():
        logical_id = no_phys_to_logical.get((shard, height, tx_id))
        if not logical_id:
            missing_logical += 1
            continue
        logical_key = (shard, logical_id)
        full_row = full_by_logical.get(logical_key)
        if full_row is None:
            missing_structure += 1
            continue

        source_height = source_height_by_logical[logical_key]
        if source_height == height:
            height_match += 1

        clone = dict(full_row)
        clone["tx_id"] = tx_id
        clone["height"] = str(height)
        clone["block_hash"] = str(attempt.get("block_hash") or "")
        clone["_counterfactual_logical_id"] = logical_id
        clone["_counterfactual_source_height"] = source_height
        clone["_counterfactual_source"] = "paired_full_same_logical_tx"

        translated_preds: list[str] = []
        for full_pred in full_row.get("_raw_preds") or []:
            predecessor_total += 1
            pred_logical = full_phys_to_logical.get(
                (shard, source_height, str(full_pred))
            )
            if not pred_logical:
                continue
            no_pred = no_logical_to_phys.get((shard, pred_logical))
            if not no_pred or int(no_pred["height"]) != height:
                continue
            translated_preds.append(str(no_pred["tx_id"]))
            predecessor_mapped += 1
        clone["_raw_preds"] = translated_preds
        projected[(shard, height, tx_id)] = clone

    total = len(no_track_attempts)
    matched = len(projected)
    coverage = matched / total if total else 0.0
    height_alignment = height_match / matched if matched else 0.0
    predecessor_coverage = (
        predecessor_mapped / predecessor_total if predecessor_total else 1.0
    )
    diagnostics = {
        "structure_source": "paired_full_counterfactual",
        "no_track_attempt_count": total,
        "mapped_structure_count": matched,
        "logical_mapping_coverage": coverage,
        "same_shard_height_alignment_rate": height_alignment,
        "raw_predecessor_mapping_coverage": predecessor_coverage,
        "missing_logical_identity_count": missing_logical,
        "missing_full_structure_count": missing_structure,
        "full_structure_count": len(full_structure),
    }

    # Fail closed. A partial counterfactual join would make HOL attribution look
    # precise while silently changing the population under analysis.
    if coverage != 1.0:
        raise RuntimeError(
            "paired Full->NoTrack logical structure mapping incomplete: "
            f"{matched}/{total}, diagnostics={diagnostics}"
        )
    if height_alignment != 1.0:
        raise RuntimeError(
            "paired Full/NoTrack block-height alignment is not exact; "
            f"counterfactual structure is unsafe: {diagnostics}"
        )
    if predecessor_coverage != 1.0:
        raise RuntimeError(
            "paired predecessor identity mapping incomplete; "
            f"counterfactual descendant attribution is unsafe: {diagnostics}"
        )
    return projected, diagnostics



def analyze_run(
    run_dir: Path,
    method_id: str,
    *,
    structure_override: dict[tuple[str, int, str], dict[str, Any]] | None = None,
    structure_source: str | None = None,
    pairing_diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    reps = _representative_nodes(run_dir)
    if not reps:
        raise RuntimeError(f"no representative node/shard mapping in {run_dir}")
    own_structure = _structure_rows(run_dir, reps)
    structure = structure_override if structure_override is not None else own_structure
    attempts = _attempt_rows(run_dir, reps)
    if not structure:
        raise RuntimeError(f"metatrack_dependency_structure.csv unavailable in {run_dir}")
    if not attempts:
        raise RuntimeError(f"business execution evidence unavailable in {run_dir}")

    result: dict[str, Any] = {
        "run_dir": str(run_dir),
        "method_config_id": method_id,
        "representative_nodes": [{"shard_id": s, "node_id": n} for s, n in reps],
        "structure_source": structure_source or "own_runtime_artifact",
        "pairing_diagnostics": pairing_diagnostics,
        "structural": _structural_summary(structure),
        "timing_by_structure": _full_timing_summary(structure, attempts),
    }
    if method_id == NO_TRACK_ID:
        exact = _exact_block_hol(run_dir, reps)
        externality, blockers, victims = _fifo_externality(structure, attempts, exact)
        externality["structure_source"] = result["structure_source"]
        if pairing_diagnostics is not None:
            externality["pairing_diagnostics"] = pairing_diagnostics
        result["fifo_externality"] = externality
        result["_blockers"] = blockers
        result["_victims"] = victims
    return result

def _child_run_id(child: dict[str, Any]) -> str:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    return str(result.get("run_id") or child.get("run_id") or "")


def _child_run_dir(repo: Path, child: dict[str, Any]) -> Path:
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    output = result.get("output_dir")
    if output:
        path = Path(str(output))
        if path.exists():
            return path
    run_id = _child_run_id(child)
    if not run_id:
        raise RuntimeError(f"child {child.get('child_run_id')} has no run_id")
    return repo / ".cache" / "v5_real_cluster_runs" / run_id


def _pair_key(child: dict[str, Any]) -> tuple[int, int]:
    return (_int(child.get("seed")), _int(child.get("repeat_index")))


def _child_tps(child: dict[str, Any]) -> float | None:
    metrics = child.get("metrics") if isinstance(child.get("metrics"), dict) else {}
    for key in ("end_to_end_tps", "throughput_tps"):
        if metrics.get(key) is not None:
            return _float(metrics.get(key))
    result = child.get("result") if isinstance(child.get("result"), dict) else {}
    summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
    if summary.get("throughput_tps") is not None:
        return _float(summary.get("throughput_tps"))
    return None


def _route_digest(child: dict[str, Any]) -> str:
    metrics = child.get("metrics") if isinstance(child.get("metrics"), dict) else {}
    for key in (
        "metatrack_incremental_history_digest",
        "metatrack_routing_history_digest",
        "routing_history_digest",
    ):
        value = metrics.get(key)
        if value:
            return str(value)
    return ""


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _latest_eligible_group(repo: Path) -> str:
    root = repo / ".cache" / "v5_formal_runs"
    candidates = sorted(
        [p for p in root.glob("v5grp_*") if (p / "run_group.json").is_file()],
        key=lambda p: (p / "run_group.json").stat().st_mtime,
        reverse=True,
    )
    for directory in candidates:
        children_dir = directory / "children"
        children = []
        if children_dir.is_dir():
            for path in children_dir.glob("v5child_*.json"):
                try:
                    children.append(json.loads(path.read_text(encoding="utf-8")))
                except Exception:
                    pass
        methods = {str(c.get("method_config_id") or "") for c in children if c.get("status") == "completed"}
        if FULL_ID in methods and NO_TRACK_ID in methods:
            return directory.name
    raise RuntimeError("no completed formal group containing Metatrack + no-dual-track found")


def analyze_group(repo: Path, group_id: str | None = None) -> dict[str, Any]:
    repo = repo.resolve()
    group_id = group_id or _latest_eligible_group(repo)
    group_dir = repo / ".cache" / "v5_formal_runs" / group_id
    children_dir = group_dir / "children"
    if not children_dir.is_dir():
        raise RuntimeError(f"group children not found: {group_id}")

    children = []
    for path in sorted(children_dir.glob("v5child_*.json")):
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("status") == "completed" and payload.get("method_config_id") in {FULL_ID, NO_TRACK_ID}:
            children.append(payload)

    full_by_key = {_pair_key(c): c for c in children if c.get("method_config_id") == FULL_ID}
    no_by_key = {_pair_key(c): c for c in children if c.get("method_config_id") == NO_TRACK_ID}
    keys = sorted(set(full_by_key) & set(no_by_key))
    if not keys:
        raise RuntimeError("no paired Full/no-dual-track repeats")

    output_dir = group_dir / "diagnostics" / "metatrack_hol_externality_v1_1"
    output_dir.mkdir(parents=True, exist_ok=True)

    repeats: list[dict[str, Any]] = []
    all_blockers: list[dict[str, Any]] = []
    all_victims: list[dict[str, Any]] = []

    for seed, repeat in keys:
        full_child = full_by_key[(seed, repeat)]
        no_child = no_by_key[(seed, repeat)]
        full_dir = _child_run_dir(repo, full_child)
        no_dir = _child_run_dir(repo, no_child)

        full_tps = _child_tps(full_child)
        no_tps = _child_tps(no_child)
        delta_pct = (
            (no_tps - full_tps) / full_tps * 100.0
            if full_tps not in (None, 0) and no_tps is not None
            else None
        )
        route_full = _route_digest(full_child)
        route_no = _route_digest(no_child)
        route_match = (route_full == route_no) if route_full and route_no else None

        full = analyze_run(full_dir, FULL_ID)

        no_reps = _representative_nodes(no_dir)
        if not no_reps:
            raise RuntimeError(f"no representative node/shard mapping in {no_dir}")
        no_attempts = _attempt_rows(no_dir, no_reps)
        if not no_attempts:
            raise RuntimeError(f"business execution evidence unavailable in {no_dir}")
        no_own_structure = _structure_rows(no_dir, no_reps)

        if no_own_structure:
            no_track = analyze_run(
                no_dir,
                NO_TRACK_ID,
                structure_source="own_runtime_artifact",
            )
        else:
            # Historical no-track runs did not capture dependency structure.
            # Pairing is allowed only when the formal run proves routing identity.
            if route_match is not True:
                raise RuntimeError(
                    "historical NoTrack structure is empty and paired Full routing "
                    f"digest is not exactly equal: full={route_full!r} no={route_no!r}"
                )
            full_reps = _representative_nodes(full_dir)
            projected, pairing = _paired_counterfactual_structure(
                full_dir,
                no_dir,
                full_reps,
                no_reps,
                no_attempts,
            )
            no_track = analyze_run(
                no_dir,
                NO_TRACK_ID,
                structure_override=projected,
                structure_source="paired_full_counterfactual",
                pairing_diagnostics=pairing,
            )

        ext = no_track.get("fifo_externality") or {}
        repeat_row = {
            "seed": seed,
            "repeat_index": repeat,
            "full_run_id": _child_run_id(full_child),
            "no_track_run_id": _child_run_id(no_child),
            "full_tps": full_tps,
            "no_track_tps": no_tps,
            "no_track_minus_full_pct": delta_pct,
            "routing_digest_match": route_match,
            "structure_source": ext.get("structure_source"),
            "logical_mapping_coverage": (ext.get("pairing_diagnostics") or {}).get("logical_mapping_coverage"),
            "same_shard_height_alignment_rate": (ext.get("pairing_diagnostics") or {}).get("same_shard_height_alignment_rate"),
            "raw_predecessor_mapping_coverage": (ext.get("pairing_diagnostics") or {}).get("raw_predecessor_mapping_coverage"),
            "evidence_mode": ext.get("evidence_mode"),
            "fifo_hol_victim_count": ext.get("fifo_hol_victim_count"),
            "fifo_hol_reconstructed_ms": ext.get("fifo_hol_reconstructed_ms"),
            "fifo_hol_exact_runtime_aggregate_ms": ext.get("fifo_hol_exact_runtime_aggregate_ms"),
            "fifo_hol_calibration_abs_error_pct": ext.get("fifo_hol_calibration_abs_error_pct"),
            "multi_frontier_transaction_share": ext.get("multi_frontier_transaction_share"),
            "multi_frontier_hol_share": ext.get("multi_frontier_hol_share"),
            "multi_frontier_hol_amplification_vs_tx_share": ext.get("multi_frontier_hol_amplification_vs_tx_share"),
            "multi_frontier_fast_eligible_victim_count": ext.get("multi_frontier_fast_eligible_victim_count"),
            "multi_frontier_fast_eligible_hol_ms": ext.get("multi_frontier_fast_eligible_hol_ms"),
            "multi_frontier_non_descendant_fast_victim_count": ext.get("multi_frontier_non_descendant_fast_victim_count"),
            "multi_frontier_non_descendant_fast_hol_ms": ext.get("multi_frontier_non_descendant_fast_hol_ms"),
            "multi_frontier_critical_path_blocker_count": ext.get("multi_frontier_critical_path_blocker_count"),
        }
        repeats.append(repeat_row)

        for row in no_track.pop("_blockers", []):
            all_blockers.append({"seed": seed, "repeat_index": repeat, **row})
        for row in no_track.pop("_victims", []):
            all_victims.append({"seed": seed, "repeat_index": repeat, **row})

        pair_payload = {
            "seed": seed,
            "repeat_index": repeat,
            "routing_digest_match": route_match,
            "full_tps": full_tps,
            "no_track_tps": no_tps,
            "no_track_minus_full_pct": delta_pct,
            "full": full,
            "no_track": no_track,
        }
        (output_dir / f"repeat_{repeat}_seed_{seed}.json").write_text(
            json.dumps(pair_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    _write_csv(output_dir / "repeat_summary.csv", repeats)
    _write_csv(output_dir / "blocker_externality.csv", all_blockers)
    _write_csv(output_dir / "victim_attribution.csv", all_victims)

    multi_share = [float(r["multi_frontier_transaction_share"]) for r in repeats if r.get("multi_frontier_transaction_share") is not None]
    hol_share = [float(r["multi_frontier_hol_share"]) for r in repeats if r.get("multi_frontier_hol_share") is not None]
    amp = [float(r["multi_frontier_hol_amplification_vs_tx_share"]) for r in repeats if r.get("multi_frontier_hol_amplification_vs_tx_share") is not None]
    calibration = [float(r["fifo_hol_calibration_abs_error_pct"]) for r in repeats if r.get("fifo_hol_calibration_abs_error_pct") is not None]
    deltas = [float(r["no_track_minus_full_pct"]) for r in repeats if r.get("no_track_minus_full_pct") is not None]

    summary = {
        "schema_version": SCHEMA_VERSION,
        "group_id": group_id,
        "historical_no_track_structure_policy": "paired_full_same_seed_repeat_logical_tx_fail_closed",
        "paired_repeat_count": len(repeats),
        "evidence_modes": sorted({str(r.get("evidence_mode") or "") for r in repeats}),
        "routing_digest_match_all_available": all(r.get("routing_digest_match") is not False for r in repeats),
        "mean_no_track_minus_full_pct": _mean(deltas),
        "mean_multi_frontier_transaction_share": _mean(multi_share),
        "mean_multi_frontier_hol_share": _mean(hol_share),
        "mean_multi_frontier_hol_amplification_vs_tx_share": _mean(amp),
        "mean_legacy_calibration_abs_error_pct": _mean(calibration),
        "total_multi_frontier_fast_eligible_victim_count": sum(_int(r.get("multi_frontier_fast_eligible_victim_count")) for r in repeats),
        "total_multi_frontier_fast_eligible_hol_ms": sum(_float(r.get("multi_frontier_fast_eligible_hol_ms")) for r in repeats),
        "total_multi_frontier_non_descendant_fast_victim_count": sum(_int(r.get("multi_frontier_non_descendant_fast_victim_count")) for r in repeats),
        "total_multi_frontier_non_descendant_fast_hol_ms": sum(_float(r.get("multi_frontier_non_descendant_fast_hol_ms")) for r in repeats),
        "interpretation_guard": (
            "Self-sojourn is not blocker externality. This report tests whether multi-frontier heads "
            "disproportionately delay already-ready single/zero-frontier transactions under strict FIFO."
        ),
        "legacy_guard": (
            "legacy_estimated rows infer ready time from attempt_start_offset - queue_wait and must be "
            "judged together with calibration error against exact runtime aggregate."
        ),
        "output_dir": str(output_dir),
        "repeats": repeats,
    }
    (output_dir / "summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    report_lines = [
        "# MetaTrack 双轨 HOL 外部性诊断",
        "",
        f"- RunGroup: `{group_id}`",
        f"- 配对重复次数: **{len(repeats)}**",
        f"- 证据模式: `{', '.join(summary['evidence_modes'])}`",
        f"- 路由摘要配对一致: **{summary['routing_digest_match_all_available']}**",
        f"- 历史 NoTrack 结构策略: `{summary['historical_no_track_structure_policy']}`",
        "",
        "## 核心指标",
        "",
        f"- multi-frontier 交易平均占比: `{summary['mean_multi_frontier_transaction_share']}`",
        f"- multi-frontier 作为 dominant FIFO head 的 HOL 时间占比: `{summary['mean_multi_frontier_hol_share']}`",
        f"- HOL 放大倍数（HOL占比 / 交易占比）: `{summary['mean_multi_frontier_hol_amplification_vs_tx_share']}`",
        f"- 被 multi-frontier head 阻塞的 Fast-eligible victim 总数: `{summary['total_multi_frontier_fast_eligible_victim_count']}`",
        f"- 对 Fast-eligible victim 的归因 HOL: `{summary['total_multi_frontier_fast_eligible_hol_ms']}` ms",
        f"- 其中非该 head DAG 后继的 Fast victim 数: `{summary['total_multi_frontier_non_descendant_fast_victim_count']}`",
        "",
        "## 解释边界",
        "",
        "- 这里验证的是**阻挡外部性**，不是比较 Conservative 自身平均 sojourn。",
        "- `legacy_estimated` 使用旧归档重建 ready 时刻，并用 runtime 已有 exact FIFO blocked aggregate 校准。",
        "- 历史 NoTrack 若结构 CSV 为空，只允许从同 seed/repeat、路由摘要完全一致的 Full 通过 logical_tx_id 回填；逻辑映射、块高和前驱映射任一不完整即拒绝归因。",
        "- `exact_runtime` 使用本补丁新增的每交易 ready/release/blocked-ns 证据，不依赖 scheduler trace。",
        "- `metatrack_scheduler_trace.csv` 的 timestamp 是写 artifacts 时的顺序时间戳，且存在 20,000 行保留上限，因此本诊断不拿它做持续时间归因。",
        "",
    ]
    (output_dir / "REPORT.md").write_text("\n".join(report_lines), encoding="utf-8")
    return summary
