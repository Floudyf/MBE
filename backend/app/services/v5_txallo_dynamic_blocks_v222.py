from __future__ import annotations

# MBE_TXALLO_DYNAMIC_BLOCK_META_V226: metadata projection + MBE-adapted 15-source-block cadence.

import csv
import gzip
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from backend.app.core.paths import ROOT

ROW_SCHEMA = "mbe_txallo_dynamic_block_v1"
SUMMARY_SCHEMA = "mbe_txallo_dynamic_block_selection_v1"
DEFAULT_EPOCH_BLOCKS = 15
PAPER_REFERENCE_EPOCH_BLOCKS = 300
DEFAULT_G_EPOCH_MULTIPLE = 20


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write_gz(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name("." + path.name + f".{os.getpid()}.tmp")
    with tmp.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, compresslevel=9, mtime=0) as out:
            for row in rows:
                out.write((json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n").encode("utf-8"))
        raw.flush()
        os.fsync(raw.fileno())
    os.replace(tmp, path)


def _field_optional(fieldnames: list[str], *names: str) -> str | None:
    by_lower = {str(x).strip().lower(): str(x) for x in fieldnames if str(x).strip()}
    for name in names:
        hit = by_lower.get(name.lower())
        if hit:
            return hit
    return None


def _field(fieldnames: list[str], *names: str) -> str:
    hit = _field_optional(fieldnames, *names)
    if hit:
        return hit
    raise ValueError(f"TxAllo dynamic block source missing field; require one of {names}")


def _metadata(row: dict[str, Any], metadata_field: str | None, raw_index: int) -> dict[str, Any]:
    if not metadata_field:
        return {}
    raw = row.get(metadata_field)
    if raw in (None, ""):
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        value = json.loads(str(raw))
    except json.JSONDecodeError as exc:
        raise ValueError(f"TxAllo dynamic source row {raw_index} has invalid metadata JSON") from exc
    if not isinstance(value, dict):
        raise ValueError(f"TxAllo dynamic source row {raw_index} metadata is not an object")
    return value


def _row_value(row: dict[str, Any], field: str | None, metadata: dict[str, Any], *metadata_names: str) -> Any:
    if field:
        value = row.get(field)
        if value not in (None, ""):
            return value
    for name in metadata_names:
        value = metadata.get(name)
        if value not in (None, ""):
            return value
    return None


def _as_int(value: Any, label: str) -> int:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"TxAllo dynamic block source has empty {label}")
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"TxAllo dynamic block source has invalid {label}: {text!r}") from exc


def compile_txallo_dynamic_blocks(
    *,
    run_dir: Path,
    manifest: dict[str, Any],
    workload_plan: dict[str, Any],
    profile: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    sharding = profile.get("sharding") or {}
    config = sharding.get("config") or {}
    if sharding.get("plugin_id") != "txallo_account_sharding" or not bool(config.get("dynamic_a_txallo_runtime")):
        return None

    pool = manifest.get("txallo_history_pool")
    if not isinstance(pool, dict):
        raise ValueError("TxAllo dynamic mode requires manifest.txallo_history_pool")
    source_rel = str(pool.get("source_raw_relative_path") or "").strip()
    if not source_rel:
        raise ValueError("TxAllo dynamic mode requires txallo_history_pool.source_raw_relative_path")
    source = ROOT / Path(source_rel)
    if not source.is_file():
        raise ValueError(f"TxAllo dynamic source block file missing: {source}")

    evaluation_count = int(workload_plan.get("actual_tx_count") or workload_plan.get("tx_count") or 0)
    # This sidecar is an exact row-for-row projection of the raw source after
    # the reviewed anchor. Do not silently reuse it for sampled/reordered
    # workload variants where materialized_index no longer equals raw row order.
    selection_mode = str(workload_plan.get("selection_mode") or "").strip()
    variant_mode = str(workload_plan.get("variant_mode") or "").strip()
    if selection_mode != "validated_prefix" or variant_mode != "original_window":
        raise ValueError(
            "formal TxAllo dynamic block projection requires original_window + validated_prefix"
        )
    anchor = int(pool.get("evaluation_anchor_raw_row_index") or 0)
    expected_anchor_tx = str(pool.get("evaluation_anchor_transaction_id") or "").strip().lower()
    if evaluation_count <= 0 or anchor < 0:
        raise ValueError("TxAllo dynamic block selection requires positive evaluation count and valid anchor")

    epoch_blocks = int(config.get("a_epoch_blocks") or DEFAULT_EPOCH_BLOCKS)
    g_epoch_multiple = int(config.get("g_epoch_multiple") or DEFAULT_G_EPOCH_MULTIPLE)
    if epoch_blocks != DEFAULT_EPOCH_BLOCKS:
        raise ValueError("formal MBE-adapted TxAllo dynamic mode requires a_epoch_blocks=15")
    if g_epoch_multiple != DEFAULT_G_EPOCH_MULTIPLE:
        raise ValueError("formal TxAllo dynamic mode requires paper case-study g_epoch_multiple=20")

    try:
        csv.field_size_limit(sys.maxsize)
    except OverflowError:
        csv.field_size_limit(2**31 - 1)

    rows: list[dict[str, Any]] = []
    with source.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        tx_field = _field(fieldnames, "transaction_id", "source_transaction_id", "tx_id", "trx_id")
        block_field = _field_optional(fieldnames, "block_num", "block_number", "block")
        seq_field = _field_optional(fieldnames, "global_sequence", "global_seq", "sequence")
        metadata_field = _field_optional(fieldnames, "metadata")
        if block_field is None and metadata_field is None:
            raise ValueError(
                "TxAllo dynamic block source missing block identity; require top-level block_num/block_number/block "
                "or metadata.block_num"
            )
        if seq_field is None and metadata_field is None:
            raise ValueError(
                "TxAllo dynamic block source missing sequence identity; require top-level global_sequence/global_seq/sequence "
                "or metadata.global_sequence"
            )
        sender_field = _field(fieldnames, "sender_id", "miner", "sender")
        receiver_field = _field(fieldnames, "receiver_id", "receiver", "contract")

        last_block = -1
        last_sequence = -1
        for raw_index, row in enumerate(reader):
            if raw_index < anchor:
                continue
            if raw_index >= anchor + evaluation_count:
                break
            materialized_index = raw_index - anchor
            txid = str(row.get(tx_field) or "").strip().lower()
            metadata = _metadata(row, metadata_field, raw_index)
            block_num = _as_int(
                _row_value(row, block_field, metadata, "block_num", "block_number", "block"),
                "block_num",
            )
            global_sequence = _as_int(
                _row_value(row, seq_field, metadata, "global_sequence", "global_seq", "sequence"),
                "global_sequence",
            )
            sender = str(row.get(sender_field) or "").strip().lower()
            receiver = str(row.get(receiver_field) or "").strip().lower()
            if not txid or not sender or not receiver:
                raise ValueError(f"TxAllo dynamic source row {raw_index} missing tx/account identity")
            if materialized_index == 0 and expected_anchor_tx and txid != expected_anchor_tx:
                raise ValueError(
                    "TxAllo dynamic source anchor mismatch: "
                    f"expected {expected_anchor_tx}, got {txid}"
                )
            if block_num <= 0:
                raise ValueError(f"TxAllo dynamic source row {raw_index} has non-positive block_num")
            if last_block > block_num:
                raise ValueError("TxAllo dynamic source block_num regressed")
            if last_sequence >= 0 and global_sequence <= last_sequence:
                raise ValueError("TxAllo dynamic source global_sequence is not strictly increasing")
            last_block = block_num
            last_sequence = global_sequence
            rows.append({
                "schema_version": ROW_SCHEMA,
                "materialized_index": materialized_index,
                "raw_source_row_index": raw_index,
                "transaction_id": txid,
                "block_num": block_num,
                "global_sequence": global_sequence,
                "sender_id": sender,
                "receiver_id": receiver,
            })

    if len(rows) != evaluation_count:
        raise ValueError(
            f"TxAllo dynamic source ended early: selected {len(rows)} want {evaluation_count}"
        )

    out_rel = Path("workload") / "txallo_dynamic_blocks.jsonl.gz"
    out = run_dir / out_rel
    _write_gz(out, rows)
    sidecar_sha = _sha256(out)
    block_min = rows[0]["block_num"]
    block_max = rows[-1]["block_num"]
    source_epoch_count = ((block_max - block_min) // epoch_blocks) + 1
    summary = {
        "schema_version": SUMMARY_SCHEMA,
        "row_schema_version": ROW_SCHEMA,
        "relative_path": out_rel.as_posix(),
        "sha256": sidecar_sha,
        "selected_count": len(rows),
        "evaluation_anchor_raw_row_index": anchor,
        "evaluation_anchor_transaction_id": rows[0]["transaction_id"],
        "first_block_num": block_min,
        "last_block_num": block_max,
        "first_global_sequence": rows[0]["global_sequence"],
        "last_global_sequence": rows[-1]["global_sequence"],
        "a_epoch_blocks": epoch_blocks,
        "paper_reference_a_epoch_blocks": PAPER_REFERENCE_EPOCH_BLOCKS,
        "a_epoch_parameterization": "mbe_adapted_fixed_15_source_blocks",
        "g_epoch_multiple": g_epoch_multiple,
        "source_epoch_count_spanned": source_epoch_count,
        "selection_semantics": "validated_evaluation_prefix_raw_source_block_projection",
        "block_identity_source": "top_level_or_metadata_json",
        "sequence_identity_source": "top_level_or_metadata_json",
        "future_evaluation_transactions_used_for_prior_epoch": 0,
        "paper_note": "TxAllo paper evaluation used tau1=300 source blocks. This MBE-adapted profile intentionally uses tau1=15 source blocks while preserving the paper case-study tau2/tau1=20 ratio.",
        "source_raw_relative_path": source_rel,
    }
    summary_path = run_dir / "workload" / "txallo_dynamic_blocks_summary.json"
    summary_path.write_text(json.dumps(summary, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return summary
