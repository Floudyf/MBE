from __future__ import annotations

import csv
import gzip
import hashlib
import json
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "mbe_v5_optme_correctness_oracle_v23_3"
_SUPPORTED_MODES = {"read", "write", "read_write", "commutative_delta"}


def _canonical_digest(value: object) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _stable_direct_access_value(*, logical_tx_id: str, key: str, semantics: str, previous: str) -> str:
    payload: dict[str, str] = {"logical_tx_id": logical_tx_id, "key": key, "semantics": semantics}
    if previous:
        payload["previous"] = previous
    raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _business_state_digest(snapshot: dict[str, str]) -> str:
    rows: list[str] = []
    for key in sorted(snapshot):
        logical = key.split("::", 1)[1] if "::" in key else key
        if logical.startswith("relay_commit:") or logical.startswith("protocol:"):
            continue
        rows.append(f"{key}={snapshot[key]}")
    return hashlib.sha256("\n".join(rows).encode("utf-8")).hexdigest()


def _load_access_entries(path: Path) -> tuple[dict[str, dict[str, Any]], str, list[str]]:
    entries: dict[str, dict[str, Any]] = {}
    canonical: list[dict[str, Any]] = []
    blockers: list[str] = []
    try:
        opener = gzip.open if path.suffix == ".gz" else open
        with opener(path, "rt", encoding="utf-8") as handle:  # type: ignore[arg-type]
            for lineno, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                obj = json.loads(line)
                tx_id = str(obj.get("tx_id") or "").strip()
                logical_id = str(obj.get("logical_id") or tx_id).strip()
                accesses = obj.get("access_list")
                if not tx_id or not isinstance(accesses, list):
                    blockers.append(f"optme_v23_invalid_resolved_access:{lineno}")
                    continue
                if tx_id in entries:
                    blockers.append(f"optme_v23_duplicate_resolved_access_tx_id:{tx_id}")
                    continue
                normalized: list[dict[str, Any]] = []
                for access in accesses:
                    if not isinstance(access, dict):
                        blockers.append(f"optme_v23_invalid_access_item:{lineno}")
                        continue
                    key = str(access.get("key") or "").strip()
                    mode = str(access.get("mode") or "").strip()
                    semantics = str(access.get("update_semantics") or "")
                    if not key or "::" in key:
                        blockers.append(f"optme_v23_invalid_logical_key:{tx_id}:{key}")
                    if mode not in _SUPPORTED_MODES:
                        blockers.append(f"optme_v23_unsupported_mode:{tx_id}:{mode}")
                    try:
                        delta = int(access.get("delta") or 0)
                    except (TypeError, ValueError):
                        blockers.append(f"optme_v23_invalid_delta:{tx_id}:{key}")
                        delta = 0
                    normalized.append({"key": key, "mode": mode, "update_semantics": semantics, "delta": delta})
                entries[tx_id] = {"logical_id": logical_id, "access_list": normalized}
                canonical.append({"tx_id": tx_id, "logical_id": logical_id, "access_list": normalized})
    except (OSError, EOFError, UnicodeError, json.JSONDecodeError) as exc:
        return {}, "", [f"optme_v23_resolved_access_unreadable:{type(exc).__name__}"]
    canonical.sort(key=lambda x: x["tx_id"])
    return entries, _canonical_digest(canonical) if canonical else "", blockers


def _load_home_mapping(run_dir: Path) -> tuple[dict[str, str], list[str]]:
    path = run_dir / "client" / "placement_plan.csv"
    if not path.is_file():
        return {}, ["optme_v23_missing_placement_plan"]
    out: dict[str, str] = {}
    blockers: list[str] = []
    try:
        with path.open(newline="", encoding="utf-8-sig") as handle:
            for row in csv.DictReader(handle):
                key = str(row.get("state_key") or "").strip()
                home = str(row.get("home_shard") or "").strip()
                if not key or not home:
                    continue
                prior = out.get(key)
                if prior and prior != home:
                    blockers.append(f"optme_v23_home_mapping_conflict:{key}:{prior}:{home}")
                out[key] = home
    except (OSError, csv.Error, UnicodeError) as exc:
        return {}, [f"optme_v23_placement_plan_unreadable:{type(exc).__name__}"]
    if not out:
        blockers.append("optme_v23_empty_home_mapping")
    return out, blockers


def _read_json(path: Path) -> dict[str, Any]:
    try:
        obj = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return {}
    return obj if isinstance(obj, dict) else {}


def _state_partitions(run_dir: Path, home_map: dict[str, str]) -> list[str]:
    plan = _read_json(run_dir / "compiled_run_plan.json")
    partitions: set[str] = set()
    for node in plan.get("node_configs") if isinstance(plan.get("node_configs"), list) else []:
        if isinstance(node, dict):
            value = str(node.get("execution_shard_id") or "").strip()
            if value:
                partitions.add(value)
    partitions.update(home for home in home_map.values() if home)
    return sorted(partitions)


def _block_order_from_existing_evidence(block: dict[str, Any]) -> tuple[list[dict[str, Any]], list[str]]:
    evidence = block.get("optme_transaction_evidence")
    if not isinstance(evidence, list):
        return [], [f"optme_v23_missing_transaction_evidence:{block.get('height')}"]
    blockers: list[str] = []
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    main: list[tuple[int, int, dict[str, Any]]] = []
    rerun: list[tuple[int, int, dict[str, Any]]] = []
    terminal_only: list[dict[str, Any]] = []
    for item in evidence:
        if not isinstance(item, dict):
            blockers.append(f"optme_v23_invalid_transaction_evidence:{block.get('height')}")
            continue
        tx_id = str(item.get("tx_id") or "").strip()
        if not tx_id or tx_id in seen:
            blockers.append(f"optme_v23_invalid_or_duplicate_evidence_tx:{block.get('height')}:{tx_id}")
            continue
        seen.add(tx_id)
        idx = int(item.get("original_index") or 0)
        main_seq = int(item.get("main_sequence") or 0)
        epoch = int(item.get("reschedule_epoch") or 0)
        simulation_success = bool(item.get("simulation_success"))
        reexecuted = bool(item.get("reexecuted"))
        reexecution_success = bool(item.get("reexecution_success"))
        invalidated = bool(item.get("second_pass_invalidated"))
        terminal_success = bool(item.get("terminal_success"))
        row = {
            "tx_id": tx_id,
            "original_index": idx,
            "terminal_success": terminal_success,
            "applied": False,
            "phase": "terminal_only",
            "sequence": main_seq,
            "reschedule_epoch": epoch,
        }
        if simulation_success and main_seq > 0:
            row["applied"] = True
            row["phase"] = "main_sequence"
            main.append((main_seq, idx, row))
        elif reexecuted and reexecution_success and not invalidated and epoch > 0:
            row["applied"] = True
            row["phase"] = "reschedule_epoch"
            rerun.append((epoch, idx, row))
        else:
            if terminal_success:
                blockers.append(f"optme_v23_nonapplied_terminal_marked_success:{tx_id}")
            terminal_only.append(row)
    main.sort(key=lambda x: (x[0], x[1]))
    rerun.sort(key=lambda x: (x[0], x[1]))
    terminal_only.sort(key=lambda x: x["original_index"])
    ordinal = 0
    for _, _, row in main + rerun:
        ordinal += 1
        row["apply_ordinal"] = ordinal
        rows.append(row)
    for row in terminal_only:
        row["apply_ordinal"] = 0
        rows.append(row)
    return rows, blockers


def _collect_apply_evidence(run_dir: Path) -> tuple[list[dict[str, Any]], str, str, list[str]]:
    blockers: list[str] = []
    paths = sorted((run_dir / "nodes").glob("*/block_execution_summary.json"))
    if not paths:
        return [], "", "", ["optme_v23_missing_block_execution_summary"]
    reference_rows: list[dict[str, Any]] | None = None
    reference_digest = ""
    executor_id = ""
    for path in paths:
        summary = _read_json(path)
        current_executor = str(summary.get("block_executor_id") or "")
        if current_executor not in {"optme_block_executor", "stateless_optme_block_executor"}:
            blockers.append(f"optme_v23_wrong_executor:{path.parent.name}:{current_executor}")
            continue
        executor_id = executor_id or current_executor
        if executor_id != current_executor:
            blockers.append("optme_v23_mixed_executor_ids")
        rows: list[dict[str, Any]] = []
        blocks = summary.get("blocks") if isinstance(summary.get("blocks"), list) else []
        for block in sorted((b for b in blocks if isinstance(b, dict)), key=lambda b: int(b.get("height") or 0)):
            block_rows, more = _block_order_from_existing_evidence(block)
            blockers.extend(f"{path.parent.name}:{x}" for x in more)
            height = int(block.get("height") or 0)
            block_hash = str(block.get("block_hash") or "")
            for row in block_rows:
                rows.append({**row, "height": height, "block_hash": block_hash})
        digest = _canonical_digest(rows) if rows else ""
        if reference_rows is None:
            reference_rows, reference_digest = rows, digest
        elif digest != reference_digest:
            blockers.append(f"optme_v23_replica_apply_order_mismatch:{path.parent.name}")
    return reference_rows or [], reference_digest, executor_id, blockers


def _apply_replay(entries: dict[str, dict[str, Any]], order: list[dict[str, Any]], *, stateless: bool, home_map: dict[str, str]) -> tuple[dict[str, str], list[str]]:
    state: dict[str, str] = {}
    blockers: list[str] = []
    for item in order:
        if not bool(item.get("applied")):
            continue
        tx_id = str(item.get("tx_id") or "")
        entry = entries.get(tx_id)
        if entry is None:
            blockers.append(f"optme_v23_applied_tx_missing_access:{tx_id}")
            continue
        logical_id = str(entry.get("logical_id") or tx_id)
        for access in entry.get("access_list") or []:
            key = str(access.get("key") or "")
            mode = str(access.get("mode") or "")
            semantics = str(access.get("update_semantics") or "")
            if stateless:
                home = home_map.get(key, "")
                if not home:
                    blockers.append(f"optme_v23_missing_home_for_applied_key:{key}")
                    continue
                qualified = f"{home}::{key}"
            else:
                qualified = f"optme-global::{key}"
            if mode == "read":
                continue
            if mode == "read_write":
                state[qualified] = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics=semantics, previous=state.get(qualified, ""))
            elif mode == "write":
                state[qualified] = _stable_direct_access_value(logical_tx_id=logical_id, key=key, semantics=semantics, previous="")
            elif mode == "commutative_delta":
                try:
                    current = int(state.get(qualified, "") or "0")
                except ValueError:
                    blockers.append(f"optme_v23_non_integer_commutative_base:{key}")
                    continue
                state[qualified] = str(current + int(access.get("delta") or 0))
            else:
                blockers.append(f"optme_v23_unsupported_mode_during_replay:{mode}")
    return state, blockers


def _partition_business_digest(state: dict[str, str]) -> tuple[str, dict[str, str]]:
    by_partition: dict[str, dict[str, str]] = {}
    for key, value in state.items():
        partition = key.split("::", 1)[0] if "::" in key else ""
        by_partition.setdefault(partition, {})[key] = value
    digests = {p: _business_state_digest(snapshot) for p, snapshot in sorted(by_partition.items())}
    return (_canonical_digest(digests) if digests else ""), digests


def evaluate_optme(run_dir: Path, summary: dict[str, Any] | None = None) -> dict[str, Any]:
    run_dir = Path(run_dir)
    summary = summary if isinstance(summary, dict) else {}
    blockers: list[str] = []
    executor_id = str(summary.get("block_executor_id") or "")
    if executor_id not in {"optme_block_executor", "stateless_optme_block_executor"}:
        return {
            "serial_order_oracle_schema": SCHEMA_VERSION,
            "serial_order_oracle_status": "not_applicable",
            "serial_order_replay_applicable": False,
            "method_correctness_oracle_kind": "not_optme",
            "method_correctness_oracle_status": "not_applicable",
            "method_correctness_oracle_valid": None,
            "method_correctness_oracle_blockers": [],
        }
    stateless = executor_id == "stateless_optme_block_executor"

    access_path = run_dir / "client" / "resolved_access_lists.jsonl.gz"
    if not access_path.is_file():
        access_path = run_dir / "client" / "resolved_access_lists.jsonl"
    if not access_path.is_file():
        entries, input_digest = {}, ""
        blockers.append("optme_v23_missing_resolved_access_lists")
    else:
        entries, input_digest, more = _load_access_entries(access_path)
        blockers.extend(more)

    order, order_digest, observed_executor, more = _collect_apply_evidence(run_dir)
    blockers.extend(more)
    if observed_executor and observed_executor != executor_id:
        blockers.append(f"optme_v23_executor_summary_mismatch:{executor_id}:{observed_executor}")

    terminal_ids = [str(row.get("tx_id") or "") for row in order]
    if set(terminal_ids) != set(entries):
        blockers.append(f"optme_v23_terminal_transaction_set_mismatch:{len(set(terminal_ids))}:{len(entries)}")

    home_map: dict[str, str] = {}
    if stateless:
        home_map, more = _load_home_mapping(run_dir)
        blockers.extend(more)

    state, more = _apply_replay(entries, order, stateless=stateless, home_map=home_map)
    blockers.extend(more)
    if stateless:
        _, partition_digests = _partition_business_digest(state)
        for partition in _state_partitions(run_dir, home_map):
            partition_digests.setdefault(partition, _business_state_digest({}))
        replay_global_business = _canonical_digest(dict(sorted(partition_digests.items()))) if partition_digests else ""
    else:
        business = _business_state_digest(state)
        partition_digests = {"optme-global": business}
        replay_global_business = _canonical_digest(partition_digests)

    actual_global_business = str(summary.get("global_business_state_digest") or "").strip()
    if not actual_global_business:
        blockers.append("optme_v23_missing_actual_global_business_digest")
    equivalent = bool(not blockers and replay_global_business and replay_global_business == actual_global_business)
    if not blockers and not equivalent:
        blockers.append("optme_v23_business_digest_mismatch")

    status = "passed" if equivalent else "failed"
    kind = "optme_v23_stateless_partitioned_method_specific_replay" if stateless else "optme_v23_stateful_method_specific_replay"
    scope = "optme_v23_existing_tx_evidence_partitioned_replay" if stateless else "optme_v23_existing_tx_evidence_stateful_replay"
    return {
        "serial_order_oracle_schema": SCHEMA_VERSION,
        "serial_order_oracle_status": status,
        "serial_order_replay_applicable": True,
        "serial_order_replay_not_applicable_reason": "",
        "serial_order_replay_equivalent": equivalent,
        "serial_order_replay_blockers": blockers,
        "serial_order_replay_supported_scope": scope,
        "serial_order_replay_input_digest": input_digest,
        "serial_order_replay_order_digest": order_digest,
        "method_correctness_oracle_kind": kind,
        "method_correctness_oracle_status": status,
        "method_correctness_oracle_valid": equivalent,
        "method_correctness_oracle_scope": scope,
        "method_correctness_oracle_blockers": blockers,
        "optme_v23_replay_partition_business_digests": partition_digests,
        "optme_v23_replay_global_business_digest": replay_global_business,
        "optme_v23_apply_order_source": "existing_optme_transaction_evidence",
    }
