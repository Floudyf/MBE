from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any

SCHEMA = "mbe_txallo_mapping_epoch_audit_v229"


def _digest(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _rows(path: Path) -> list[dict[str, str]]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return [
            {str(k).lower(): "" if v is None else str(v).strip() for k, v in row.items()}
            for row in csv.DictReader(handle)
        ]


def _placement_path(root: Path) -> Path | None:
    for rel in ("client/txallo_transaction_placement.csv", "txallo_transaction_placement.csv"):
        p = root / rel
        if p.is_file():
            return p
    return None


def _history_path(root: Path) -> Path | None:
    for rel in ("workload/txallo_mapping_epochs.jsonl", "client/txallo_mapping_epochs.jsonl", "txallo_mapping_epochs.jsonl"):
        p = root / rel
        if p.is_file():
            return p
    return None


def _load_epochs(root: Path) -> tuple[dict[int, dict[str, Any]], str | None, list[str]]:
    path = _history_path(root)
    if path is None:
        return {}, None, []
    epochs: dict[int, dict[str, Any]] = {}
    errors: list[str] = []
    try:
        for line_no, line in enumerate(path.read_text(encoding="utf-8-sig").splitlines(), start=1):
            if not line.strip():
                continue
            obj = json.loads(line)
            if not isinstance(obj, dict):
                errors.append(f"mapping_epoch_row_not_object:{line_no}")
                continue
            epoch = int(obj.get("epoch", -1))
            mapping = obj.get("mapping")
            aliases = obj.get("aliases")
            digest = str(obj.get("state_digest") or "").strip()
            mapping_digest = str(obj.get("mapping_digest") or "").strip()
            aliases_digest = str(obj.get("aliases_digest") or "").strip()
            if epoch < 0 or not isinstance(mapping, dict) or not isinstance(aliases, dict) or not digest:
                errors.append(f"mapping_epoch_row_invalid:{line_no}")
                continue
            normalized = {str(k).lower().strip(): str(v).strip() for k, v in mapping.items() if str(k).strip() and str(v).strip()}
            normalized_aliases = {str(k).lower().strip(): str(v).lower().strip() for k, v in aliases.items() if str(k).strip() and str(v).strip()}
            if mapping_digest != _digest(normalized):
                errors.append(f"mapping_epoch_mapping_digest_mismatch:{epoch}")
                continue
            if aliases_digest != _digest(normalized_aliases):
                errors.append(f"mapping_epoch_aliases_digest_mismatch:{epoch}")
                continue
            if digest != _digest({"mapping": normalized, "aliases": normalized_aliases}):
                errors.append(f"mapping_epoch_state_digest_mismatch:{epoch}")
                continue
            previous = epochs.get(epoch)
            if previous is not None and previous.get("state_digest") != digest:
                errors.append(f"mapping_epoch_conflict:{epoch}")
                continue
            epochs[epoch] = {"mapping": normalized, "aliases": normalized_aliases, "state_digest": digest}
    except (OSError, UnicodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
        errors.append(f"mapping_epoch_history_unreadable:{type(exc).__name__}")
    if epochs:
        expected = set(range(max(epochs) + 1))
        missing = sorted(expected.difference(epochs))
        if missing:
            errors.append("mapping_epoch_history_gap:" + ",".join(map(str, missing[:16])))
    return epochs, str(path.relative_to(root)).replace("\\", "/"), errors


def _split(value: str) -> list[str]:
    if not value:
        return []
    for sep in ("|", ";", ","):
        if sep in value:
            return [x.strip() for x in value.split(sep) if x.strip()]
    return [value.strip()] if value.strip() else []


def compute_routing_coherence(run_dir: Path | str) -> dict[str, Any] | None:
    root = Path(run_dir)
    placement = _placement_path(root)
    epochs, history_source, errors = _load_epochs(root)
    if placement is None or not epochs:
        return None
    rows = _rows(placement)
    if not rows or "routing_epoch" not in rows[0] or "mapping_state_digest" not in rows[0]:
        return None
    mismatch = 0
    checked = 0
    fallback: set[str] = set()
    history: set[str] = set()
    examples: list[dict[str, Any]] = []
    for row in rows:
        try:
            epoch = int(row.get("routing_epoch") or 0)
        except ValueError:
            epoch = -1
        snapshot = epochs.get(epoch)
        if snapshot is None:
            mismatch += 1
            if len(examples) < 8:
                examples.append({"logical_id": row.get("logical_id"), "reason": "mapping_epoch_missing", "routing_epoch": epoch})
            continue
        if row.get("mapping_state_digest") != snapshot["state_digest"]:
            mismatch += 1
            if len(examples) < 8:
                examples.append({"logical_id": row.get("logical_id"), "reason": "mapping_state_digest_mismatch", "routing_epoch": epoch})
            continue
        checked += 1
        mapping: dict[str, str] = snapshot["mapping"]
        expected: set[str] = set()
        for role in ("sender", "receiver"):
            account = (row.get(f"{role}_account") or "").lower()
            if not account:
                continue
            source = row.get(f"{role}_mapping_source") or ""
            shard = row.get(f"{role}_shard") or ""
            bad = False
            if source == "history_mapping":
                history.add(account)
                bad = mapping.get(account) != shard
            elif source == "fallback_hash":
                fallback.add(account)
                bad = account in mapping or not shard
            elif source == "none" and role == "receiver":
                continue
            else:
                bad = True
            if shard:
                expected.add(shard)
            if bad:
                mismatch += 1
                if len(examples) < 8:
                    examples.append({"logical_id": row.get("logical_id"), "role": role, "account": account, "source": source, "shard": shard, "epoch_mapping_shard": mapping.get(account), "routing_epoch": epoch})
        claimed = set(_split(row.get("involved_shards") or ""))
        if claimed != expected:
            mismatch += 1
            if len(examples) < 8:
                examples.append({"logical_id": row.get("logical_id"), "reason": "involved_shards_mismatch", "expected": sorted(expected), "claimed": sorted(claimed), "routing_epoch": epoch})
    mismatch += len(errors)
    return {
        "schema_version": SCHEMA,
        "status": "available_epoch_bound_mapping_evidence",
        "checked_transaction_count": checked,
        "mismatch_count": mismatch,
        "history_mapped_account_count": len(history),
        "fallback_account_count": len(fallback),
        "passed": mismatch == 0 and checked > 0,
        "examples": examples + [{"reason": e} for e in errors[: max(0, 8-len(examples))]],
        "mapping_epoch_history_source": history_source,
        "transaction_placement_source": str(placement.relative_to(root)).replace("\\", "/"),
        "paper_contract": "placement_is_audited_against_the_committed_history_mapping_epoch_active_when_the_transaction_was_routed",
    }


def compute_account_coverage(run_dir: Path | str) -> dict[str, Any] | None:
    result = compute_routing_coherence(run_dir)
    if result is None:
        return None
    return {
        "schema_version": SCHEMA,
        "status": result["status"],
        "passed": result["passed"],
        "reference_account_count": result["history_mapped_account_count"] + result["fallback_account_count"],
        "history_mapped_account_count": result["history_mapped_account_count"],
        "fallback_account_count": result["fallback_account_count"],
        "mismatch_count": result["mismatch_count"],
        "examples": result["examples"],
        "reference_source_files": [result["transaction_placement_source"], result["mapping_epoch_history_source"]],
        "runtime_mapped_count_not_used_as_reference": True,
        "truth_boundary": "canonical_logical_account_identity_checked_against_active_mapping_epoch_not_final_mapping",
    }
