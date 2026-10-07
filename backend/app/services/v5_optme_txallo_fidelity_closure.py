from __future__ import annotations

import csv
import hashlib
import json
import math
from pathlib import Path
from typing import Any, Iterable

CLOSURE_VERSION = "optme_txallo_paper_fidelity_fairness_oracle_v16"
WRITEBACK_REPLICATION_CONTRACT = "home_all_replicas_failover_preserving_v1"
LEADER_ONLY_REJECTION = (
    "pending_state_deltas_are_home_replica_local_until_home_pbft_commit;"
    "leader_only_ingress_can_lose_an_uncommitted_exact_version_delta_on_view_change"
)

_TARGET_TOKENS = ("optme", "txallo")
_TXALLO = "txallo"
_OPTME = "optme"

_EMPTY = (None, "", [], {})


def _nonempty(value: Any) -> bool:
    return value not in _EMPTY


def _json_bytes(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")


def _sha(value: Any) -> str:
    return hashlib.sha256(_json_bytes(value)).hexdigest()


def _method_id(value: Any, fallback: dict[str, Any] | None = None) -> str:
    if isinstance(value, dict):
        for key in ("method_id", "id", "config_id", "method_config_id", "plugin_id"):
            if _nonempty(value.get(key)):
                return str(value[key]).strip()
        value = ""
    if _nonempty(value):
        return str(value).strip()
    source = fallback or {}
    for key in ("method_id", "method_config_id", "algorithm_id", "runtime_method_id", "child_method_id"):
        if _nonempty(source.get(key)):
            return str(source[key]).strip()
    method = source.get("method")
    if isinstance(method, dict):
        return _method_id(method)
    if _nonempty(method):
        return str(method).strip()
    return ""


def _is_target(method_id: str) -> bool:
    lower = method_id.lower()
    return any(token in lower for token in _TARGET_TOKENS)


def _is_txallo(method_id: str) -> bool:
    return _TXALLO in method_id.lower()


def _is_optme(method_id: str) -> bool:
    return _OPTME in method_id.lower()


def _as_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        number = float(value)
    except Exception:
        return None
    return number if math.isfinite(number) else None


def _first(source: dict[str, Any], keys: Iterable[str]) -> Any:
    for key in keys:
        value = source.get(key)
        if _nonempty(value):
            return value
    return None


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _read_csv(path: Path) -> list[dict[str, str]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    except Exception:
        return []


def _rel(root: Path, path: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except Exception:
        return str(path).replace("\\", "/")


def compute_logical_business_state_evidence(run_dir: Path | str) -> dict[str, Any]:
    """Build a placement-independent final business-state commitment.

    Runtime v16 writes one `business_state_commitments_v16.json` per replica.
    We require replica agreement inside every shard, take one agreed set per shard,
    union identical logical-key/value commitments across shards, and then hash the
    canonical global set.  A logical key with more than one committed value is
    preserved as a conflict instead of being silently overwritten.
    """
    root = Path(run_dir)
    node_dirs = sorted(path for path in (root / "nodes").glob("*") if path.is_dir()) if (root / "nodes").is_dir() else []
    paths = sorted(root.glob("nodes/*/business_state_commitments_v16.json"))
    missing_nodes = sorted(
        path.name for path in node_dirs
        if not (path / "business_state_commitments_v16.json").is_file()
    )
    if not paths:
        return {
            "status": "missing",
            "digest": None,
            "conflict_key_count": None,
            "replica_consistent": None,
            "shard_count": 0,
            "node_count": len(node_dirs),
            "evidence_node_count": 0,
            "missing_nodes": missing_nodes,
            "source_files": [],
            "truth_boundary": "placement_independent_logical_key_value_commitments_v16",
        }
    if missing_nodes:
        return {
            "status": "incomplete_node_evidence",
            "digest": None,
            "conflict_key_count": None,
            "replica_consistent": None,
            "shard_count": 0,
            "node_count": len(node_dirs),
            "evidence_node_count": len(paths),
            "missing_nodes": missing_nodes,
            "source_files": sorted(_rel(root, path) for path in paths),
            "truth_boundary": "placement_independent_logical_key_value_commitments_v16",
        }

    by_shard: dict[str, list[tuple[str, set[tuple[str, str]]]]] = {}
    files: list[str] = []
    malformed: list[str] = []
    for path in paths:
        data = _read_json(path)
        if not isinstance(data, dict):
            malformed.append(_rel(root, path))
            continue
        shard = str(data.get("shard_id") or "").strip()
        rows = data.get("commitments")
        if not shard or not isinstance(rows, list):
            malformed.append(_rel(root, path))
            continue
        pairs: set[tuple[str, str]] = set()
        valid = True
        for row in rows:
            if not isinstance(row, dict):
                valid = False
                break
            key_digest = str(row.get("key_digest") or "").strip()
            value_digest = str(row.get("value_digest") or "").strip()
            if not key_digest or not value_digest:
                valid = False
                break
            pairs.add((key_digest, value_digest))
        if not valid:
            malformed.append(_rel(root, path))
            continue
        files.append(_rel(root, path))
        by_shard.setdefault(shard, []).append((_rel(root, path), pairs))

    if malformed or not by_shard:
        return {
            "status": "malformed",
            "digest": None,
            "conflict_key_count": None,
            "replica_consistent": False if by_shard else None,
            "shard_count": len(by_shard),
            "node_count": len(node_dirs),
            "evidence_node_count": len(paths),
            "missing_nodes": [],
            "source_files": sorted(files),
            "malformed_files": sorted(malformed),
            "truth_boundary": "placement_independent_logical_key_value_commitments_v16",
        }

    agreed_by_shard: dict[str, set[tuple[str, str]]] = {}
    replica_mismatches: list[str] = []
    for shard, replicas in sorted(by_shard.items()):
        baseline = replicas[0][1]
        if any(pairs != baseline for _, pairs in replicas[1:]):
            replica_mismatches.append(shard)
            continue
        agreed_by_shard[shard] = baseline

    if replica_mismatches:
        return {
            "status": "replica_inconsistent",
            "digest": None,
            "conflict_key_count": None,
            "replica_consistent": False,
            "replica_mismatch_shards": replica_mismatches,
            "shard_count": len(by_shard),
            "node_count": len(node_dirs),
            "evidence_node_count": len(paths),
            "missing_nodes": [],
            "source_files": sorted(files),
            "truth_boundary": "placement_independent_logical_key_value_commitments_v16",
        }

    global_pairs: set[tuple[str, str]] = set()
    for pairs in agreed_by_shard.values():
        global_pairs.update(pairs)
    key_values: dict[str, set[str]] = {}
    for key_digest, value_digest in global_pairs:
        key_values.setdefault(key_digest, set()).add(value_digest)
    conflicts = sorted(key for key, values in key_values.items() if len(values) > 1)
    canonical = "\n".join(f"{key}={value}" for key, value in sorted(global_pairs))
    return {
        "status": "available",
        "digest": hashlib.sha256(canonical.encode("utf-8")).hexdigest(),
        "pair_count": len(global_pairs),
        "logical_key_count": len(key_values),
        "conflict_key_count": len(conflicts),
        "conflict_key_digests": conflicts[:32],
        "replica_consistent": True,
        "shard_count": len(agreed_by_shard),
        "node_count": len(node_dirs),
        "evidence_node_count": len(paths),
        "missing_nodes": [],
        "source_files": sorted(files),
        "truth_boundary": "placement_independent_logical_key_value_commitments_v16",
    }


def _candidate_files(root: Path, exact: Iterable[str], patterns: Iterable[str]) -> list[Path]:
    out: list[Path] = []
    seen: set[str] = set()
    for rel in exact:
        path = root / rel
        if path.is_file():
            key = str(path.resolve())
            if key not in seen:
                seen.add(key)
                out.append(path)
    for pattern in patterns:
        try:
            candidates = root.rglob(pattern)
        except Exception:
            continue
        for path in candidates:
            if not path.is_file():
                continue
            try:
                key = str(path.resolve())
            except Exception:
                key = str(path)
            if key in seen:
                continue
            seen.add(key)
            out.append(path)
            if len(out) >= 64:
                return out
    return out


def _canonical_rows_digest(root: Path, paths: list[Path], *, drop_fields: tuple[str, ...] = ()) -> dict[str, Any]:
    rows: list[dict[str, str]] = []
    files: list[str] = []
    drop = tuple(x.lower() for x in drop_fields)
    for path in paths:
        if path.suffix.lower() == ".csv":
            source_rows = _read_csv(path)
        elif path.suffix.lower() == ".json":
            data = _read_json(path)
            source_rows = []
            if isinstance(data, list):
                source_rows = [x for x in data if isinstance(x, dict)]
            elif isinstance(data, dict):
                if all(not isinstance(v, (dict, list)) for v in data.values()):
                    source_rows = [{"key": str(k), "value": "" if v is None else str(v)} for k, v in data.items()]
                else:
                    for key, value in data.items():
                        if isinstance(value, dict):
                            for subkey, subvalue in value.items():
                                if not isinstance(subvalue, (dict, list)):
                                    source_rows.append({"mapping": str(key), "key": str(subkey), "value": "" if subvalue is None else str(subvalue)})
        else:
            continue
        if not source_rows:
            continue
        files.append(_rel(root, path))
        for row in source_rows:
            normalized = {
                str(k): "" if v is None else str(v)
                for k, v in row.items()
                if not any(token in str(k).lower() for token in drop)
            }
            normalized["__source_file"] = _rel(root, path)
            rows.append(normalized)
    if not rows:
        return {"status": "missing", "digest": None, "row_count": 0, "files": []}
    encoded = sorted(_json_bytes(row).decode("utf-8") for row in rows)
    return {
        "status": "available",
        "digest": hashlib.sha256("\n".join(encoded).encode("utf-8")).hexdigest(),
        "row_count": len(rows),
        "files": sorted(set(files)),
    }


def compute_txallo_evidence(run_dir: Path | str, result: dict[str, Any] | None = None) -> dict[str, Any]:
    """Return four separate TxAllo evidence dimensions. Never aliases them."""
    root = Path(run_dir)
    data = result or {}

    account_mapping = {
        "status": "available_from_runtime_metric" if _nonempty(data.get("txallo_mapping_digest")) else "missing",
        "digest": data.get("txallo_mapping_digest") if _nonempty(data.get("txallo_mapping_digest")) else None,
        "row_count": _as_int(data.get("txallo_mapped_account_count")) or 0,
        "files": [],
        "source": "txallo_mapping_digest" if _nonempty(data.get("txallo_mapping_digest")) else None,
    }
    if account_mapping["digest"] is None:
        candidates = _candidate_files(
            root,
            (
                "client/txallo_account_mapping.csv",
                "txallo_account_mapping.csv",
                "aggregate/txallo_account_mapping.csv",
                "client/txallo_mapping.json",
                "txallo_mapping.json",
            ),
            ("*txallo*account*mapping*.csv", "*txallo*mapping*.json"),
        )
        account_mapping = _canonical_rows_digest(root, candidates)
        account_mapping["source"] = "txallo_account_mapping_artifact" if account_mapping["status"] == "available" else None

    transaction_placement = _canonical_rows_digest(
        root,
        _candidate_files(
            root,
            (
                "client/txallo_transaction_placement.csv",
                "client/transaction_placement.csv",
                "transaction_placement.csv",
                "aggregate/transaction_placement.csv",
            ),
            ("*txallo*transaction*placement*.csv", "*transaction*placement*.csv"),
        ),
        drop_fields=("target_node", "replica", "validator", "peer"),
    )

    routing = _canonical_rows_digest(
        root,
        _candidate_files(
            root,
            ("client/routing_decision_log.csv", "routing_decision_log.csv", "aggregate/routing_decision_log.csv"),
            ("*routing*decision*.csv",),
        ),
        drop_fields=("timestamp",),
    )

    state_home = _canonical_rows_digest(
        root,
        _candidate_files(
            root,
            ("client/state_home_mapping.csv", "state_home_mapping.csv", "aggregate/state_home_mapping.csv", "state_home_mapping.json"),
            ("*state*home*mapping*.csv", "*state*home*mapping*.json"),
        ),
        drop_fields=("execution", "route", "target_node", "replica", "validator", "peer"),
    )

    # When the concrete account mapping artifact is available, audit the paper's
    # uniqueness requirement and expose per-shard account counts.  The runtime digest
    # alone cannot prove these structural properties.
    mapping_candidates = _candidate_files(
        root,
        (
            "client/txallo_account_mapping.csv",
            "txallo_account_mapping.csv",
            "aggregate/txallo_account_mapping.csv",
        ),
        ("*txallo*account*mapping*.csv",),
    )
    if mapping_candidates:
        rows = _read_csv(mapping_candidates[0])
        account_to_shards: dict[str, set[str]] = {}
        for row in rows:
            lower = {str(k).lower(): "" if v is None else str(v) for k, v in row.items()}
            account = (
                lower.get("account") or lower.get("account_id") or lower.get("address")
                or lower.get("state_key") or lower.get("key") or lower.get("node")
            )
            shard = lower.get("shard") or lower.get("shard_id") or lower.get("home_shard") or lower.get("community")
            if account and shard:
                account_to_shards.setdefault(account, set()).add(shard)
        conflicts = {k: sorted(v) for k, v in account_to_shards.items() if len(v) > 1}
        shard_counts: dict[str, int] = {}
        for shards in account_to_shards.values():
            if len(shards) == 1:
                shard = next(iter(shards))
                shard_counts[shard] = shard_counts.get(shard, 0) + 1
        account_mapping["structural_evidence_status"] = "available"
        account_mapping["unique_account_count"] = len(account_to_shards)
        account_mapping["duplicate_account_conflict_count"] = len(conflicts)
        account_mapping["uniqueness_passed"] = not conflicts
        account_mapping["shard_account_counts"] = dict(sorted(shard_counts.items()))
        account_mapping["nonempty_shard_count"] = len([v for v in shard_counts.values() if v > 0])
        account_mapping["structural_source"] = _rel(root, mapping_candidates[0])
        runtime_count = _as_int(data.get("txallo_mapped_account_count"))
        account_mapping["completeness_against_runtime_count"] = (
            len(account_to_shards) == runtime_count if runtime_count is not None and account_to_shards else None
        )
    else:
        account_mapping["structural_evidence_status"] = "missing"
        account_mapping["unique_account_count"] = None
        account_mapping["duplicate_account_conflict_count"] = None
        account_mapping["uniqueness_passed"] = None
        account_mapping["shard_account_counts"] = None
        account_mapping["nonempty_shard_count"] = None
        account_mapping["structural_source"] = None
        account_mapping["completeness_against_runtime_count"] = None

    return {
        "account_allocation": account_mapping,
        "transaction_placement": transaction_placement,
        "routing_decision": routing,
        "state_home_mapping": state_home,
    }


def _is_writeback_row(row: dict[str, Any]) -> bool:
    text = " ".join(str(v) for v in row.values()).lower()
    return "state_delta_apply" in text or "write_apply" in text or "versioned_remote_home" in text


def _logical_delta_key(row: dict[str, Any]) -> tuple[tuple[str, str], ...]:
    lower = {str(k).lower(): "" if v is None else str(v) for k, v in row.items()}
    fields = (
        "logical_tx_id",
        "logical_id",
        "tx_id",
        "state_key",
        "key",
        "produced_version",
        "home_shard",
        "update_semantics",
        "semantics",
    )
    chosen: list[tuple[str, str]] = []
    # Prefer logical transaction identity over physical tx id if both exist.
    logical = lower.get("logical_tx_id") or lower.get("logical_id") or lower.get("tx_id")
    if logical:
        chosen.append(("logical_tx_id", logical))
    state_key = lower.get("state_key") or lower.get("key")
    if state_key:
        chosen.append(("state_key", state_key))
    for key in ("produced_version", "home_shard", "update_semantics", "semantics"):
        if lower.get(key):
            chosen.append((key, lower[key]))
    if len(chosen) >= 2:
        return tuple(chosen)
    # Fail closed: do not infer a logical delta by just dropping replica IDs.
    return tuple()


def compute_writeback_fanout(run_dir: Path | str, result: dict[str, Any] | None = None) -> dict[str, Any]:
    root = Path(run_dir)
    candidates = _candidate_files(
        root,
        ("physical_remote_state_operations.csv", "aggregate/physical_remote_state_operations.csv"),
        ("*physical_remote_state_operations*.csv",),
    )
    for path in candidates:
        rows = [row for row in _read_csv(path) if _is_writeback_row(row)]
        if not rows:
            continue
        keys = {_logical_delta_key(row) for row in rows}
        keys.discard(tuple())
        if keys:
            return {
                "status": "available_exact_logical_dedup",
                "physical_message_count": len(rows),
                "unique_logical_delta_count": len(keys),
                "replica_fanout_ratio": len(rows) / len(keys),
                "source": _rel(root, path),
            }
        return {
            "status": "physical_only_logical_identity_missing",
            "physical_message_count": len(rows),
            "unique_logical_delta_count": None,
            "replica_fanout_ratio": None,
            "source": _rel(root, path),
        }

    data = result or {}
    physical = None
    message_types = data.get("network_message_type_counts") or data.get("message_type_counts") or {}
    if isinstance(message_types, dict):
        for key, value in message_types.items():
            if "STATE_DELTA_APPLY" in str(key).upper():
                physical = (_as_int(value) or 0) + (physical or 0)
    if physical is None:
        physical = _as_int(_first(data, ("state_delta_apply_message_count", "physical_remote_writeback_count", "remote_state_write_apply_count")))
    # v16 deliberately does NOT use durable-publish counters as an exact logical-delta denominator.
    return {
        "status": "physical_summary_only" if physical is not None else "missing",
        "physical_message_count": physical,
        "unique_logical_delta_count": None,
        "replica_fanout_ratio": None,
        "source": "summary_metrics" if physical is not None else None,
    }


def _mechanism_remote_state(metrics: dict[str, Any]) -> dict[str, Any] | None:
    mechanism = metrics.get("mechanism_metrics")
    if not isinstance(mechanism, dict):
        return None
    for key in ("remote_state", "remote_state_access", "stateless_remote_state"):
        value = mechanism.get(key)
        if isinstance(value, dict):
            return value
    return None


def _remote_flat_count(metrics: dict[str, Any]) -> int:
    values = [
        _as_int(metrics.get("physical_remote_operation_count")),
        _as_int(metrics.get("remote_state_access_count")),
    ]
    return max((x for x in values if x is not None), default=0)


def _remote_nested_count(metrics: dict[str, Any]) -> int | None:
    nested = _mechanism_remote_state(metrics)
    if nested is None:
        return None
    values = []
    for key in ("physical_remote_operation_count", "remote_state_access_count", "operation_count", "total"):
        value = _as_int(nested.get(key))
        if value is not None:
            values.append(value)
    return max(values) if values else 0


def _metatrack_status(metrics: dict[str, Any]) -> str | None:
    mechanism = metrics.get("mechanism_metrics")
    if not isinstance(mechanism, dict):
        return None
    for key, value in mechanism.items():
        if "metatrack" not in str(key).lower() or not isinstance(value, dict):
            continue
        status = value.get("status") or value.get("availability")
        if _nonempty(status):
            return str(status)
    return None


def _txallo_diagnostics(metrics: dict[str, Any]) -> dict[str, Any]:
    """Collect paper-defined TxAllo quantities without guessing topology.

    Important paper semantics:
    - eta is the workload multiplier of a cross-shard transaction (eta > 1), not shard count.
    - lambda is per-shard processing capacity.
    - epsilon is the convergence threshold.
    - k (shard count) is supplied by the experiment topology and is therefore checked at group level.

    The paper's experimental calibration lambda=|T|/k and epsilon=1e-5*|T| is
    exposed only as a *calibration clue*.  It is not treated as a universal algorithm invariant.
    """
    history = _as_int(_first(metrics, ("txallo_history_transaction_count", "txallo_history_count")))
    accounts = _as_int(_first(metrics, ("txallo_mapped_account_count", "txallo_graph_account_count", "txallo_account_count")))
    edges = _as_int(_first(metrics, ("txallo_graph_edge_count", "txallo_edge_count")))
    lam = _as_float(_first(metrics, ("txallo_lambda", "paper_model_lambda_parameter")))
    eta = _as_float(_first(metrics, ("txallo_eta",)))
    eps = _as_float(_first(metrics, ("txallo_epsilon",)))
    chunk = _as_int(_first(metrics, ("txallo_adaptive_chunk_records", "txallo_adaptive_chunk_size")))
    adaptive_runs = _as_int(_first(metrics, ("txallo_a_txallo_run_count", "txallo_adaptive_run_count")))
    global_runs = _as_int(_first(metrics, ("txallo_g_txallo_run_count", "txallo_global_run_count")))
    modeled_cross = _as_float(_first(metrics, ("txallo_modeled_cross_shard_ratio", "txallo_model_cross_shard_ratio")))
    evaluation_cross = _as_float(_first(metrics, ("txallo_evaluation_cross_shard_ratio",)))
    evaluation_count = _as_int(_first(metrics, ("txallo_evaluation_transaction_count",)))
    stddev = _as_float(_first(metrics, ("txallo_modeled_workload_stddev", "txallo_workload_stddev")))
    modeled_throughput = _as_float(_first(metrics, ("txallo_modeled_throughput", "txallo_evaluation_throughput")))
    allocation_mode = _first(metrics, ("txallo_allocation_mode",))
    louvain_levels = _as_int(_first(metrics, ("txallo_louvain_level_count",)))
    mapping_complete = _first(metrics, ("txallo_mapping_complete",))
    lambda_source = _first(metrics, ("txallo_lambda_source",))
    epsilon_source = _first(metrics, ("txallo_epsilon_source",))
    dynamic_a_enabled = _first(metrics, ("txallo_dynamic_a_txallo_runtime_enabled",))
    a_epoch_blocks = _as_int(_first(metrics, ("txallo_a_epoch_blocks",)))
    g_epoch_multiple = _as_int(_first(metrics, ("txallo_g_epoch_multiple",)))
    committed_dynamic = _as_int(_first(metrics, ("txallo_committed_dynamic_transaction_count",)))
    a_tx_count = _as_int(_first(metrics, ("txallo_a_txallo_transaction_count",)))
    periodic_g_runs = _as_int(_first(metrics, ("txallo_periodic_g_txallo_run_count",)))
    total_allocator_history = _as_int(_first(metrics, ("txallo_total_allocator_history_transaction_count",)))
    mapping_epoch = _as_int(_first(metrics, ("txallo_mapping_epoch",)))
    closed_source_epochs = _as_int(_first(metrics, ("txallo_closed_source_epoch_count",)))
    pending_used = _as_int(_first(metrics, ("txallo_pending_or_uncommitted_transactions_used",)))
    initial_g_lambda = _as_float(_first(metrics, ("txallo_initial_g_lambda",)))
    initial_g_epsilon = _as_float(_first(metrics, ("txallo_initial_g_epsilon",)))

    epsilon_implied_reference_count = None
    if eps is not None and eps >= 0:
        guess = int(round(eps / 1e-5))
        if abs(eps - guess * 1e-5) <= max(1e-12, abs(eps) * 1e-9):
            epsilon_implied_reference_count = guess

    # A graph with |E| ~= |T| and |V| = |E|+1 means essentially every historical
    # transaction introduced a distinct pair-edge and the graph is tree-like.  This
    # can be a legitimate workload shape, so it is a warning/evidence-quality flag,
    # never by itself a correctness blocker.
    tree_like_graph = bool(
        history and accounts and edges is not None
        and edges == accounts - 1
        and abs(edges - history) <= 1
    )

    parameter_semantics_valid = bool(
        lam is not None and lam > 0
        and eta is not None and eta > 1
        and eps is not None and eps >= 0
    )

    return {
        "allocation_mode": allocation_mode,
        "louvain_level_count": louvain_levels,
        "mapping_complete": mapping_complete,
        "lambda_source": lambda_source,
        "epsilon_source": epsilon_source,
        "dynamic_a_txallo_runtime_enabled": dynamic_a_enabled,
        "a_epoch_blocks": a_epoch_blocks,
        "g_epoch_multiple": g_epoch_multiple,
        "committed_dynamic_transaction_count": committed_dynamic,
        "a_txallo_transaction_count": a_tx_count,
        "periodic_g_txallo_run_count": periodic_g_runs,
        "total_allocator_history_transaction_count": total_allocator_history,
        "mapping_epoch": mapping_epoch,
        "closed_source_epoch_count": closed_source_epochs,
        "pending_or_uncommitted_transactions_used": pending_used,
        "initial_g_lambda": initial_g_lambda,
        "initial_g_epsilon": initial_g_epsilon,
        "history_transaction_count": history,
        "mapped_account_count": accounts,
        "graph_edge_count": edges,
        "lambda_processing_capacity_per_shard": lam,
        "eta_cross_shard_workload_multiplier": eta,
        "epsilon_convergence_threshold": eps,
        "adaptive_chunk_records": chunk,
        "adaptive_run_count": adaptive_runs,
        "global_run_count": global_runs,
        "epsilon_paper_experiment_implied_reference_transaction_count": epsilon_implied_reference_count,
        "paper_parameter_semantics_valid": parameter_semantics_valid,
        "account_graph_tree_like_suspected": tree_like_graph,
        "modeled_cross_shard_ratio": modeled_cross,
        "evaluation_cross_shard_ratio": evaluation_cross,
        "evaluation_transaction_count": evaluation_count,
        "modeled_workload_stddev": stddev,
        "modeled_throughput_Lambda": modeled_throughput,
        "paper_model_Lambda_is_measured_end_to_end_tps": False,
    }

def _execution_stack_payload(metrics: dict[str, Any], method_id: str | None = None) -> dict[str, Any]:
    """Runtime execution stack only; method identity is deliberately excluded.

    A different method label must not by itself make two otherwise identical
    execution stacks unequal. Method identity is carried by its own digest/field.
    """
    payload: dict[str, Any] = {}
    for key in (
        "block_executor_id",
        "execution_plugin_id",
        "scheduler_plugin_id",
        "routing_plugin_id",
        "sharding_plugin_id",
        "state_access_plugin_id",
        "commit_plugin_id",
        "comparison_semantics_class",
        "state_access_semantics",
    ):
        if _nonempty(metrics.get(key)):
            payload[key] = metrics[key]
    worker = _as_int(_first(metrics, ("effective_worker_count", "worker_count", "configured_worker_count")))
    if worker is not None:
        payload["effective_worker_count"] = worker
    return payload


def _resolved_execution_stack_payload(item: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Merge declared plugin stack with runtime-resolved evidence, excluding method name."""
    method = item.get("method") if isinstance(item.get("method"), dict) else {}
    overrides = method.get("plugin_overrides") if isinstance(method, dict) else {}
    configs = method.get("plugin_config_overrides") if isinstance(method, dict) else {}
    payload: dict[str, Any] = {}
    if isinstance(overrides, dict):
        payload["plugins"] = {str(k): overrides[k] for k in sorted(overrides)}
    if isinstance(configs, dict):
        # Keep declared algorithm parameters, but worker truth is represented by
        # the resolved effective worker count below rather than a stale default.
        normalized_configs: dict[str, Any] = {}
        for category in sorted(configs):
            value = configs[category]
            if isinstance(value, dict):
                copied = {str(k): v for k, v in value.items() if str(k) != "worker_count"}
                if copied:
                    normalized_configs[str(category)] = copied
            elif value is not None:
                normalized_configs[str(category)] = value
        if normalized_configs:
            payload["plugin_configs_without_worker"] = normalized_configs
    runtime = _execution_stack_payload(metrics)
    if runtime:
        payload["runtime"] = runtime
    return payload


def enrich_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    out = dict(result or {})
    method = _method_id(method_id, out)
    if not _is_target(method):
        return out

    out["fidelity_closure_version"] = CLOSURE_VERSION
    out["method_identity"] = method
    out["state_delta_replication_contract"] = WRITEBACK_REPLICATION_CONTRACT
    out["leader_only_writeback_enabled"] = False
    out["leader_only_writeback_safe_under_current_pending_delta_contract"] = False
    out["leader_only_writeback_rejected_reason"] = LEADER_ONLY_REJECTION

    effective_worker = _as_int(_first(out, ("effective_worker_count", "worker_count", "configured_worker_count")))
    out["v16_effective_worker_count"] = effective_worker
    stack = _execution_stack_payload(out, method)
    out["v16_execution_stack_digest"] = _sha(stack)
    out["v16_execution_stack"] = stack

    business_truth = compute_logical_business_state_evidence(run_dir)
    out["v16_logical_business_state_evidence_status"] = business_truth["status"]
    out["v16_logical_business_state_digest"] = business_truth.get("digest")
    out["v16_logical_business_state_conflict_key_count"] = business_truth.get("conflict_key_count")
    out["v16_logical_business_state_replica_consistent"] = business_truth.get("replica_consistent")
    out["v16_logical_business_state_evidence_shard_count"] = business_truth.get("shard_count")
    out["v16_logical_business_state_evidence_node_count"] = business_truth.get("node_count")
    out["v16_logical_business_state_evidence_node_count_with_commitment"] = business_truth.get("evidence_node_count")
    out["v16_logical_business_state_evidence_missing_nodes"] = business_truth.get("missing_nodes") or []
    out["v16_logical_business_state_evidence"] = business_truth
    out["v16_legacy_global_business_state_digest"] = out.get("global_business_state_digest")
    out["v16_legacy_global_business_state_digest_is_placement_bound"] = True

    fanout = compute_writeback_fanout(run_dir, out)
    out["v16_writeback_observation_status"] = fanout["status"]
    out["v16_physical_writeback_message_count"] = fanout["physical_message_count"]
    out["v16_unique_logical_delta_count"] = fanout["unique_logical_delta_count"]
    out["v16_replica_fanout_ratio"] = fanout["replica_fanout_ratio"]
    out["v16_writeback_evidence_source"] = fanout["source"]

    coherence_failures: list[str] = []
    flat_remote = _remote_flat_count(out)
    nested_remote = _remote_nested_count(out)
    if flat_remote > 0 and nested_remote == 0:
        coherence_failures.append("flat_remote_state_nonzero_but_nested_mechanism_remote_state_zero")
    mt_status = _metatrack_status(out)
    if "metatrack" not in method.lower() and mt_status and mt_status.lower() in {"available", "ok", "present"}:
        coherence_failures.append("metatrack_metrics_marked_available_for_non_metatrack_method")
    out["v16_metric_coherence_failures"] = coherence_failures
    out["v16_metric_coherence_passed"] = not coherence_failures
    out["v16_metatrack_metrics_applicability"] = "not_applicable" if "metatrack" not in method.lower() else "applicable"

    finalized = _as_int(_first(out, ("finalized_tx_count", "terminal_unique_tx_count", "submitted_unique_tx_count"))) or 0
    tx_exec_ms = _as_float(_first(out, ("transaction_execution_ms", "business_execution_cpu_sum_ms")))
    timer_missing = bool(finalized > 0 and tx_exec_ms == 0 and _is_txallo(method))
    out["v16_business_execution_timer_complete"] = not timer_missing
    out["v16_business_execution_timer_blocker"] = "completed_transactions_but_zero_business_execution_time" if timer_missing else None

    if _is_txallo(method):
        evidence = compute_txallo_evidence(run_dir, out)
        out["v16_txallo_account_mapping_digest"] = evidence["account_allocation"]["digest"]
        out["v16_txallo_account_mapping_source"] = evidence["account_allocation"].get("source")
        out["v16_txallo_transaction_placement_digest"] = evidence["transaction_placement"]["digest"]
        out["v16_txallo_routing_decision_digest"] = evidence["routing_decision"]["digest"]
        out["v16_txallo_state_home_mapping_digest"] = evidence["state_home_mapping"]["digest"]
        out["v16_txallo_evidence"] = evidence
        allocation = evidence["account_allocation"]
        out["v16_txallo_account_mapping_uniqueness_passed"] = allocation.get("uniqueness_passed")
        out["v16_txallo_account_mapping_completeness_passed"] = allocation.get("completeness_against_runtime_count")
        out["v16_txallo_account_mapping_shard_account_counts"] = allocation.get("shard_account_counts")
        out["v16_txallo_account_mapping_nonempty_shard_count"] = allocation.get("nonempty_shard_count")
        diagnostics = _txallo_diagnostics(out)
        out["v16_txallo_diagnostics"] = diagnostics
        out["v16_txallo_account_graph_tree_like_suspected"] = diagnostics["account_graph_tree_like_suspected"]
        out["v16_txallo_parameter_semantics_valid"] = diagnostics["paper_parameter_semantics_valid"]
        # Topology-aware G/A cadence and workload-balance checks are performed at group level.
        out["v16_txallo_adaptive_history_accounting_consistent"] = None
        out["v16_txallo_extreme_workload_imbalance_suspected"] = None
        model_lambda = _first(out, ("txallo_modeled_throughput", "txallo_lambda_throughput", "txallo_modeled_lambda", "txallo_evaluation_throughput"))
        out["paper_model_Lambda"] = model_lambda
        out["paper_model_Lambda_is_measured_tps"] = False
    else:
        out["v16_txallo_account_mapping_digest"] = None

    if _is_optme(method):
        early = _as_int(_first(out, ("optme_early_abort_count", "early_abort_count")))
        reexec = _as_int(_first(out, ("optme_reexecution_count", "reexecution_count")))
        reordered = _as_int(_first(out, ("optme_reordered_transaction_count", "reordered_transaction_count")))
        out["v16_optme_replica_physical_early_abort_observation_count"] = early
        out["v16_optme_replica_physical_reexecution_observation_count"] = reexec
        out["v16_optme_replica_physical_reordered_observation_count"] = reordered
        # Never fabricate logical counts by dividing by validators. They require tx-level dedup evidence.
        out["v16_optme_logical_early_abort_count"] = _as_int(out.get("optme_logical_early_abort_count"))
        out["v16_optme_logical_reexecution_count"] = _as_int(out.get("optme_logical_reexecution_count"))
        out["v16_optme_logical_reordered_transaction_count"] = _as_int(out.get("optme_logical_reordered_transaction_count"))
        out["v16_optme_logical_count_status"] = (
            "available" if out["v16_optme_logical_early_abort_count"] is not None else "missing_tx_level_dedup_evidence"
        )
        out["v16_optme_reorder_path_warning"] = bool((early or 0) > 0 and (reexec or 0) > 0 and (reordered or 0) == 0)
        out["v16_optme_schedule_parallel_width"] = _as_int(_first(out, ("maximum_parallel_width", "optme_maximum_parallel_width")))
        out["v16_optme_effective_concurrent_workers"] = effective_worker

    # Per-run extractor does not own group-level initial/final state. Never synthesize state equivalence here.
    out["v16_physical_state_digest"] = None
    return out


def _metric(item: dict[str, Any], key: str) -> Any:
    metrics = item.get("metrics")
    if isinstance(metrics, dict) and _nonempty(metrics.get(key)):
        return metrics.get(key)
    return item.get(key)



def _method_profile_worker_count(item: dict[str, Any]) -> int | None:
    method = item.get("method")
    if not isinstance(method, dict):
        return None
    configs = method.get("plugin_config_overrides")
    if not isinstance(configs, dict):
        return None
    block = configs.get("block_executor")
    if not isinstance(block, dict):
        return None
    return _as_int(block.get("worker_count"))


def _txallo_topology_diagnostics(item: dict[str, Any], metrics: dict[str, Any]) -> dict[str, Any]:
    """Topology-aware paper checks for the TxAllo G/A chain.

    The TxAllo paper uses k as shard count, eta as cross-shard workload multiplier,
    lambda as per-shard capacity and epsilon as convergence threshold.  In its
    simulation setting it calibrates lambda=|T|/k and epsilon=1e-5*|T|.  MBE uses
    zero in the method profile to request the same auto-calibration for the initial
    G-TxAllo history window.  We therefore report whether the observed G/A history
    ledger is self-consistent, while keeping this calibration separate from the
    generic algorithm contract.
    """
    topology = item.get("topology_point") if isinstance(item.get("topology_point"), dict) else {}
    k = _as_int(topology.get("shards"))
    d = metrics.get("v16_txallo_diagnostics") if isinstance(metrics.get("v16_txallo_diagnostics"), dict) else _txallo_diagnostics(metrics)
    allocation_mode = str(d.get("allocation_mode") or "")
    history = _as_int(d.get("history_transaction_count"))
    lam = _as_float(d.get("lambda_processing_capacity_per_shard"))
    eps = _as_float(d.get("epsilon_convergence_threshold"))
    eta = _as_float(d.get("eta_cross_shard_workload_multiplier"))
    chunk = _as_int(d.get("adaptive_chunk_records"))
    aruns = _as_int(d.get("adaptive_run_count"))
    gruns = _as_int(d.get("global_run_count"))
    modeled_cross = _as_float(d.get("modeled_cross_shard_ratio"))
    stddev = _as_float(d.get("modeled_workload_stddev"))
    modeled_lambda = _as_float(d.get("modeled_throughput_Lambda"))
    dynamic_a_enabled = d.get("dynamic_a_txallo_runtime_enabled") is True
    a_epoch_blocks = _as_int(d.get("a_epoch_blocks"))
    g_epoch_multiple = _as_int(d.get("g_epoch_multiple"))
    committed_dynamic = _as_int(d.get("committed_dynamic_transaction_count"))
    a_tx_count = _as_int(d.get("a_txallo_transaction_count"))
    periodic_g_runs = _as_int(d.get("periodic_g_txallo_run_count"))
    total_allocator_history = _as_int(d.get("total_allocator_history_transaction_count"))
    mapping_epoch = _as_int(d.get("mapping_epoch"))
    closed_source_epochs = _as_int(d.get("closed_source_epoch_count"))
    pending_used = _as_int(d.get("pending_or_uncommitted_transactions_used"))
    initial_g_lambda = _as_float(d.get("initial_g_lambda"))
    initial_g_epsilon = _as_float(d.get("initial_g_epsilon"))

    method = item.get("method") if isinstance(item.get("method"), dict) else {}
    configs = method.get("plugin_config_overrides") if isinstance(method, dict) else {}
    sharding_cfg = configs.get("sharding") if isinstance(configs, dict) and isinstance(configs.get("sharding"), dict) else {}
    auto_lambda = _as_float(sharding_cfg.get("lambda")) == 0 if sharding_cfg else False
    auto_epsilon = _as_float(sharding_cfg.get("epsilon")) == 0 if sharding_cfg else False

    initial_window_from_lambda = None
    lambda_for_initial = initial_g_lambda if dynamic_a_enabled and initial_g_lambda is not None else lam
    epsilon_for_initial = initial_g_epsilon if dynamic_a_enabled and initial_g_epsilon is not None else eps
    if k and k > 0 and lambda_for_initial is not None and lambda_for_initial > 0:
        initial_window_from_lambda = int(round(lambda_for_initial * k))
    initial_window_from_epsilon = None
    if epsilon_for_initial is not None and epsilon_for_initial >= 0:
        guess = int(round(epsilon_for_initial / 1e-5))
        if abs(epsilon_for_initial - guess * 1e-5) <= max(1e-12, abs(epsilon_for_initial) * 1e-9):
            initial_window_from_epsilon = guess

    cadence_consistent = None
    reconstructed_history = None
    if allocation_mode in {"paper_g_snapshot", "paper_g_ratio_snapshot"}:
        epsilon_ok = initial_window_from_epsilon is None or history is None or int(initial_window_from_epsilon) == history
        lambda_ok = initial_window_from_lambda is None or history is None or initial_window_from_lambda == history
        if dynamic_a_enabled:
            committed_ok = committed_dynamic is not None and committed_dynamic >= 0
            total_ok = (
                history is not None and committed_dynamic is not None and total_allocator_history is not None
                and total_allocator_history == history + committed_dynamic
            )
            cadence_ok = (
                a_epoch_blocks == 15 and g_epoch_multiple == 20
                and closed_source_epochs is not None and closed_source_epochs >= 0
                and periodic_g_runs is not None and periodic_g_runs == closed_source_epochs // 20
                and gruns is not None and gruns == 1 + periodic_g_runs
                and aruns is not None and aruns >= 0
                and mapping_epoch is not None and mapping_epoch == aruns + periodic_g_runs
                and a_tx_count is not None and a_tx_count <= (committed_dynamic or 0)
                and pending_used in (None, 0)
            )
            cadence_consistent = bool(committed_ok and total_ok and cadence_ok and lambda_ok and epsilon_ok)
            reconstructed_history = total_allocator_history
        else:
            cadence_consistent = bool(
                history is not None
                and (history == 0 or gruns == 1)
                and (aruns in (None, 0))
                and lambda_ok
                and epsilon_ok
            )
            reconstructed_history = history

    elif history is not None and initial_window_from_lambda is not None and chunk is not None and aruns is not None:
        # Backward-compatible interpretation of archived pre-v20 runs.
        reconstructed_history = initial_window_from_lambda + chunk * aruns
        epsilon_ok = (
            initial_window_from_epsilon is None
            or int(initial_window_from_epsilon) == initial_window_from_lambda
        )
        cadence_consistent = bool(
            reconstructed_history == history
            and epsilon_ok
            and (gruns is None or gruns >= 1)
        )

    # Strong but scope-limited diagnostic for the observed 2-shard model.  When
    # gamma=0, total modeled workload equals the history edge-weight mass for the
    # common two-account projection.  rho=|T|/2 is the mathematical [|T|,0] extreme.
    # We label it "suspected" unless the concrete account mapping artifact proves
    # only one non-empty shard.
    extreme = False
    if k == 2 and history and modeled_cross == 0 and stddev is not None:
        extreme = abs(stddev - history / 2.0) <= max(1e-9, history * 1e-6)
        if extreme and lam is not None and modeled_lambda is not None:
            extreme = abs(modeled_lambda - lam) <= max(1e-9, abs(lam) * 1e-6)

    nonempty = _as_int(metrics.get("v16_txallo_account_mapping_nonempty_shard_count"))
    mapping_proves_single = bool(k and nonempty is not None and nonempty < k)
    uniqueness = metrics.get("v16_txallo_account_mapping_uniqueness_passed")
    completeness = metrics.get("v16_txallo_account_mapping_completeness_passed")

    return {
        "allocation_mode": allocation_mode,
        "shard_count_k": k,
        "eta_cross_shard_workload_multiplier": eta,
        "lambda_processing_capacity_per_shard": lam,
        "epsilon_convergence_threshold": eps,
        "paper_experiment_auto_lambda_requested": auto_lambda,
        "paper_experiment_auto_epsilon_requested": auto_epsilon,
        "initial_g_history_count_implied_by_lambda_k": initial_window_from_lambda,
        "initial_g_history_count_implied_by_epsilon": initial_window_from_epsilon,
        "reconstructed_history_from_g_plus_a_chunks": reconstructed_history,
        "adaptive_history_accounting_consistent": cadence_consistent,
        "extreme_workload_imbalance_suspected": extreme,
        "account_mapping_proves_fewer_nonempty_shards_than_k": mapping_proves_single,
        "account_mapping_uniqueness_passed": uniqueness,
        "account_mapping_completeness_against_runtime_count": completeness,
        "parameter_semantics_valid": bool(lam and lam > 0 and eta and eta > 1 and eps is not None and eps >= 0 and k and k > 0),
    }

def _logical_transaction_identity_from_child(item: dict[str, Any], metrics: dict[str, Any]) -> tuple[Any, str | None]:
    """Resolve logical workload identity without confusing physical replay instances with input transactions."""
    explicit = _first(metrics, ("logical_transaction_identity_digest", "materialized_workload_digest"))
    if not _nonempty(explicit):
        explicit = _first(item, ("logical_transaction_identity_digest", "materialized_workload_digest"))
    if _nonempty(explicit):
        return explicit, "explicit_logical_transaction_identity_digest"

    # The V5 dataset path records the immutable materialized workload before any
    # routing/sharding/execution method runs. This is the strongest cross-method
    # identity available in a child result and is intentionally independent of
    # TxAllo/OptME physical execution multiplicity or reorder.
    candidates: list[dict[str, Any]] = []
    for root in (item, metrics):
        if not isinstance(root, dict):
            continue
        replay = root.get("workload_replay_summary")
        if isinstance(replay, dict):
            candidates.append(replay)
    result = item.get("result")
    if isinstance(result, dict):
        summary = result.get("summary")
        if isinstance(summary, dict) and isinstance(summary.get("workload_replay_summary"), dict):
            candidates.append(summary["workload_replay_summary"])
    for replay in candidates:
        materialized = replay.get("materialized_sha256")
        if _nonempty(materialized):
            return materialized, "workload_replay_summary.materialized_sha256"

    # Serial replay input can be used only when it is explicitly logical-ID based
    # and has no physical-instance expansion. Stateful TxAllo in the audited run
    # had 1800 replay instances for 1000 unique logical transactions, so its replay
    # digest must NOT be treated as workload identity.
    basis = str(_first(metrics, ("serial_order_replay_identity_basis",)) or "").lower()
    replay_count = _as_int(_first(metrics, ("serial_order_replay_transaction_count",)))
    unique_count = _as_int(_first(metrics, ("serial_order_replay_unique_transaction_count", "executed_logical_transaction_count")))
    replay_digest = _first(metrics, ("serial_order_replay_input_digest",))
    if (
        _nonempty(replay_digest)
        and "logical_id" in basis
        and replay_count is not None
        and unique_count is not None
        and replay_count == unique_count
    ):
        return replay_digest, "serial_replay_logical_identity_without_instance_expansion"
    return None, None


def _canonical_child(item: dict[str, Any]) -> dict[str, Any]:
    out = dict(item)
    metrics = dict(item.get("metrics") or {}) if isinstance(item.get("metrics"), dict) else {}
    method = _method_id(item.get("method"), item)
    if not method:
        method = _method_id(metrics.get("method_identity"), metrics)
    out["v16_method_id"] = method

    initial = _metric(item, "initial_state_digest")
    # v16 logical business truth is placement-independent and is derived from
    # per-replica logical-key/value commitments. The legacy global business digest
    # is intentionally NOT used for equivalence because it hashes per-shard digests
    # and therefore changes when the same logical state moves between shards.
    business = metrics.get("v16_logical_business_state_digest") if _nonempty(metrics.get("v16_logical_business_state_digest")) else item.get("v16_logical_business_state_digest")
    legacy_business = metrics.get("global_business_state_digest") if _nonempty(metrics.get("global_business_state_digest")) else item.get("global_business_state_digest")
    final = item.get("global_final_state_digest") if _nonempty(item.get("global_final_state_digest")) else metrics.get("global_final_state_digest")
    state_home = metrics.get("v16_txallo_state_home_mapping_digest") if _nonempty(metrics.get("v16_txallo_state_home_mapping_digest")) else item.get("state_home_mapping_digest")

    out["v16_initial_state_digest"] = initial if _nonempty(initial) else None
    out["v16_global_business_state_digest"] = business if _nonempty(business) else None
    out["v16_legacy_global_business_state_digest"] = legacy_business if _nonempty(legacy_business) else None
    out["v16_legacy_global_business_state_digest_is_placement_bound"] = True
    out["v16_logical_business_state_evidence_status"] = metrics.get("v16_logical_business_state_evidence_status")
    out["v16_logical_business_state_conflict_key_count"] = metrics.get("v16_logical_business_state_conflict_key_count")
    out["v16_logical_business_state_evidence_shard_count"] = _as_int(metrics.get("v16_logical_business_state_evidence_shard_count"))
    out["v16_logical_business_state_evidence_missing_nodes"] = list(metrics.get("v16_logical_business_state_evidence_missing_nodes") or [])
    out["v16_global_final_state_digest"] = final if _nonempty(final) else None
    out["v16_state_home_mapping_digest"] = state_home if _nonempty(state_home) else None

    if _nonempty(initial) and _nonempty(business):
        out["v16_logical_business_state_digest"] = _sha({"initial": initial, "business": business})
    else:
        out["v16_logical_business_state_digest"] = None

    if _nonempty(initial) and _nonempty(final) and _nonempty(state_home):
        out["v16_physical_state_digest"] = _sha({"initial": initial, "final": final, "state_home": state_home})
    else:
        out["v16_physical_state_digest"] = None

    logical_identity, logical_identity_source = _logical_transaction_identity_from_child(item, metrics)
    out["v16_logical_transaction_identity_digest"] = logical_identity if _nonempty(logical_identity) else None
    out["v16_logical_transaction_identity_source"] = logical_identity_source

    effective_worker = _as_int(_first(metrics, ("v16_effective_worker_count", "effective_worker_count", "worker_count", "configured_worker_count")))
    requested_worker = None
    topology = item.get("topology_point")
    if isinstance(topology, dict):
        requested_worker = _as_int(topology.get("worker_count"))
    out["v16_requested_worker_count"] = requested_worker
    out["v16_method_profile_worker_count"] = _method_profile_worker_count(item)
    out["v16_effective_worker_count"] = effective_worker
    out["v16_effective_vs_requested_worker_match"] = (
        effective_worker == requested_worker if effective_worker is not None and requested_worker is not None else None
    )
    out["v16_block_executor_id"] = _first(metrics, ("block_executor_id",))
    resolved_stack = _resolved_execution_stack_payload(item, metrics)
    out["v16_execution_stack"] = resolved_stack
    out["v16_execution_stack_digest"] = _sha(resolved_stack) if resolved_stack else None

    out["v16_txallo_account_mapping_digest"] = metrics.get("v16_txallo_account_mapping_digest")
    out["v16_txallo_transaction_placement_digest"] = metrics.get("v16_txallo_transaction_placement_digest")
    out["v16_txallo_routing_decision_digest"] = metrics.get("v16_txallo_routing_decision_digest")
    out["v16_txallo_state_home_mapping_digest"] = metrics.get("v16_txallo_state_home_mapping_digest")
    out["v16_txallo_account_mapping_uniqueness_passed"] = metrics.get("v16_txallo_account_mapping_uniqueness_passed")
    out["v16_txallo_account_mapping_completeness_passed"] = metrics.get("v16_txallo_account_mapping_completeness_passed")
    out["v16_txallo_account_mapping_shard_account_counts"] = metrics.get("v16_txallo_account_mapping_shard_account_counts")
    out["v16_txallo_account_mapping_nonempty_shard_count"] = metrics.get("v16_txallo_account_mapping_nonempty_shard_count")
    if _is_txallo(method):
        topo_diag = _txallo_topology_diagnostics(item, metrics)
        out["v16_txallo_topology_diagnostics"] = topo_diag
        out["v16_txallo_adaptive_history_accounting_consistent"] = topo_diag.get("adaptive_history_accounting_consistent")
        out["v16_txallo_extreme_workload_imbalance_suspected"] = topo_diag.get("extreme_workload_imbalance_suspected")
        out["v16_txallo_account_mapping_proves_fewer_nonempty_shards_than_k"] = topo_diag.get("account_mapping_proves_fewer_nonempty_shards_than_k")

    blockers: list[str] = []
    warnings: list[str] = []
    if _is_target(method):
        if out["v16_logical_business_state_digest"] is None:
            blockers.append("placement_independent_business_state_digest_missing")
        if _as_int(out.get("v16_logical_business_state_conflict_key_count")) not in {None, 0}:
            blockers.append("logical_business_state_has_cross_shard_value_conflicts")
        if metrics.get("v16_logical_business_state_replica_consistent") is False:
            blockers.append("logical_business_state_commitment_replica_inconsistent")
        if out.get("v16_logical_business_state_evidence_missing_nodes"):
            blockers.append("logical_business_state_commitment_node_evidence_incomplete")
        expected_shards = _as_int(topology.get("shards")) if isinstance(topology, dict) else None
        observed_shards = out.get("v16_logical_business_state_evidence_shard_count")
        if expected_shards is not None and observed_shards is not None and observed_shards != expected_shards:
            blockers.append("logical_business_state_commitment_shard_coverage_mismatch")
        if out["v16_logical_transaction_identity_digest"] is None:
            blockers.append("logical_transaction_identity_digest_missing")
        if metrics.get("v16_metric_coherence_passed") is False:
            blockers.extend(str(x) for x in (metrics.get("v16_metric_coherence_failures") or []))
        if metrics.get("v16_business_execution_timer_complete") is False:
            blockers.append("business_execution_measurement_incomplete")
        profile_worker = out.get("v16_method_profile_worker_count")
        effective_worker = out.get("v16_effective_worker_count")
        requested_worker = out.get("v16_requested_worker_count")
        if effective_worker is None:
            blockers.append("effective_worker_count_missing")
            out["v16_resolved_worker_contract_source"] = "missing"
        elif profile_worker is not None and effective_worker == profile_worker:
            out["v16_resolved_worker_contract_source"] = "method_profile_explicit"
            if requested_worker is not None and requested_worker != effective_worker:
                warnings.append("method_specific_worker_override_differs_from_topology_request")
        elif requested_worker is not None and effective_worker == requested_worker:
            out["v16_resolved_worker_contract_source"] = "topology_request"
            if profile_worker is not None and profile_worker != effective_worker:
                warnings.append("stale_method_profile_worker_shadowed_by_topology_request")
        elif profile_worker is None and requested_worker is None:
            out["v16_resolved_worker_contract_source"] = "runtime_only"
            warnings.append("worker_count_has_runtime_evidence_but_no_declared_source")
        else:
            out["v16_resolved_worker_contract_source"] = "unresolved_mismatch"
            blockers.append("effective_worker_count_matches_neither_method_profile_nor_topology_request")
    if _is_txallo(method):
        if not _nonempty(out["v16_txallo_account_mapping_digest"]):
            blockers.append("txallo_account_allocation_mapping_digest_missing")
        topo_diag = out.get("v16_txallo_topology_diagnostics") or {}
        if topo_diag.get("parameter_semantics_valid") is False:
            blockers.append("txallo_paper_parameter_semantics_invalid")
        if out.get("v16_txallo_account_mapping_uniqueness_passed") is False:
            blockers.append("txallo_account_mapping_uniqueness_failed")
        if out.get("v16_txallo_account_mapping_completeness_passed") is False:
            blockers.append("txallo_account_mapping_completeness_failed")
        if topo_diag.get("adaptive_history_accounting_consistent") is False:
            blockers.append("txallo_g_a_history_accounting_inconsistent")
        # Poor/degenerate optimization output is not automatically a fidelity failure:
        # the paper algorithm may legitimately behave badly on an unusual graph.  It is
        # surfaced as a high-severity warning and excluded from claims of allocation quality.
        if topo_diag.get("extreme_workload_imbalance_suspected"):
            warnings.append("txallo_extreme_workload_imbalance_suspected")
        if topo_diag.get("account_mapping_proves_fewer_nonempty_shards_than_k"):
            # MBE_TXALLO_PAPER_V20_FIDELITY: Algorithm 1 explicitly allows l < k
            # after Louvain and initializes V_j=empty for j>l. Therefore fewer
            # non-empty output shards is an allocation-quality warning, not a
            # paper-fidelity blocker, for the paper G-snapshot family.
            if topo_diag.get("allocation_mode") in {"paper_g_snapshot", "paper_g_ratio_snapshot"}:
                warnings.append("txallo_paper_allows_empty_louvain_shards;fewer_nonempty_shards_observed")
            else:
                blockers.append("txallo_account_mapping_uses_fewer_nonempty_shards_than_configured")
                warnings.append("txallo_account_mapping_uses_fewer_nonempty_shards_than_configured")
        if metrics.get("v16_txallo_account_graph_tree_like_suspected"):
            warnings.append("txallo_account_graph_projection_suspicious_tree_like")
        if out.get("v16_txallo_account_mapping_uniqueness_passed") is None:
            warnings.append("txallo_account_mapping_structure_not_proven_from_artifact")
    if _is_optme(method) and metrics.get("v16_optme_reorder_path_warning"):
        warnings.append("optme_early_abort_and_reexecution_observed_but_reorder_count_zero")
    if _is_optme(method) and metrics.get("v16_optme_logical_count_status") == "missing_tx_level_dedup_evidence":
        warnings.append("optme_logical_abort_reexecution_counts_not_proven;replica_physical_counts_only")

    out["v16_fidelity_blockers"] = sorted(set(blockers))
    out["v16_fidelity_warnings"] = sorted(set(warnings))
    out["v16_paper_fidelity_candidate"] = not blockers
    return out


def _strict_equal(values: Iterable[Any]) -> bool | None:
    materialized = list(values)
    if len(materialized) < 2:
        return None
    if any(not _nonempty(v) for v in materialized):
        return None
    return len({str(v) for v in materialized}) == 1


def _same_runtime_stack(items: list[dict[str, Any]]) -> bool | None:
    if len(items) < 2:
        return None
    stacks = [item.get("v16_execution_stack_digest") for item in items]
    workers = [item.get("v16_effective_worker_count") for item in items]
    if any(not _nonempty(x) for x in stacks) or any(x is None for x in workers):
        return None
    return len(set(map(str, stacks))) == 1 and len(set(workers)) == 1


def apply_group_fidelity_gate(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    enriched = [_canonical_child(item) if _is_target(_method_id(item.get("method"), item)) or _is_target(_method_id("", item.get("metrics") or {})) else dict(item) for item in items]
    targets = [item for item in enriched if _is_target(item.get("v16_method_id", ""))]
    out = dict(report or {})
    if not targets:
        return enriched, out

    business_equal = _strict_equal(item.get("v16_logical_business_state_digest") for item in targets)
    physical_equal = _strict_equal(item.get("v16_physical_state_digest") for item in targets)
    logical_tx_equal = _strict_equal(item.get("v16_logical_transaction_identity_digest") for item in targets)
    runtime_stack_equal = _same_runtime_stack(targets)

    any_blockers = any(item.get("v16_fidelity_blockers") for item in targets)
    complete_system_observation_valid = bool(not any_blockers and logical_tx_equal is True and business_equal is True)
    direct_single_variable_valid = bool(complete_system_observation_valid and runtime_stack_equal is True)

    out.update({
        "fidelity_closure_version": CLOSURE_VERSION,
        "pairwise_logical_business_state_equivalent": business_equal,
        "pairwise_physical_state_equivalent": physical_equal,
        "pairwise_logical_transaction_identity_equivalent": logical_tx_equal,
        "runtime_execution_stack_equivalent": runtime_stack_equal,
        "complete_system_performance_observation_valid": complete_system_observation_valid,
        "direct_single_variable_performance_comparison_valid": direct_single_variable_valid,
        "state_delta_replication_contract": WRITEBACK_REPLICATION_CONTRACT,
        "leader_only_writeback_enabled": False,
        "leader_only_writeback_safety": "rejected_without_pending_delta_pbft_replication",
        "missing_truth_is_not_equivalent": True,
        "truth_dimensions_separated": [
            "logical_transaction_identity",
            "logical_business_state",
            "physical_state",
            "txallo_account_allocation",
            "transaction_placement",
            "routing_decision",
            "state_home_mapping",
            "execution_stack",
        ],
    })

    txallo_targets = [item for item in targets if _is_txallo(item.get("v16_method_id", ""))]
    txallo_mapping_equal = _strict_equal(item.get("v16_txallo_account_mapping_digest") for item in txallo_targets) if len(txallo_targets) >= 2 else None
    out["txallo_account_mapping_deterministic_across_variants"] = txallo_mapping_equal

    group_blockers: list[str] = []
    if logical_tx_equal is not True:
        group_blockers.append("logical_transaction_identity_not_proven_equal")
    if business_equal is not True:
        group_blockers.append("logical_business_state_not_proven_equal")
    if any_blockers:
        group_blockers.append("one_or_more_method_fidelity_blockers")
    if txallo_mapping_equal is False:
        group_blockers.append("txallo_account_mapping_not_deterministic_across_stateful_stateless_variants")
    if runtime_stack_equal is not True:
        out["v16_comparison_scope_warning"] = "runtime_execution_stack_or_effective_worker_differs;complete_system_comparison_only"
    out["v16_group_fidelity_blockers"] = group_blockers

    # Never turn a previously invalid result into valid. v16 may only fail closed.
    if group_blockers:
        out["performance_comparison_valid"] = False
        out["paper_candidate"] = False

    for item in enriched:
        if not _is_target(item.get("v16_method_id", "")):
            continue
        item["pairwise_logical_business_state_equivalent"] = business_equal
        item["pairwise_physical_state_equivalent"] = physical_equal
        item["pairwise_logical_transaction_identity_equivalent"] = logical_tx_equal
        item["runtime_execution_stack_equivalent"] = runtime_stack_equal
        item["complete_system_performance_observation_valid"] = complete_system_observation_valid
        item["direct_single_variable_performance_comparison_valid"] = direct_single_variable_valid
        if item.get("v16_fidelity_blockers") or logical_tx_equal is not True or business_equal is not True:
            item["paper_candidate"] = False
            reasons = list(item.get("paper_candidate_reasons") or [])
            reasons.extend(item.get("v16_fidelity_blockers") or [])
            if logical_tx_equal is not True:
                reasons.append("logical_transaction_identity_not_proven_equal")
            if business_equal is not True:
                reasons.append("logical_business_state_not_proven_equal")
            item["paper_candidate_reasons"] = sorted(set(map(str, reasons)))
    return enriched, out


def _row_stack(row: dict[str, Any]) -> dict[str, Any]:
    method = row.get("method") if isinstance(row.get("method"), dict) else {}
    overrides = method.get("plugin_overrides") if isinstance(method, dict) else None
    configs = method.get("plugin_config_overrides") if isinstance(method, dict) else None
    if not isinstance(overrides, dict):
        overrides = row.get("plugin_overrides") if isinstance(row.get("plugin_overrides"), dict) else {}
    if not isinstance(configs, dict):
        configs = row.get("plugin_config_overrides") if isinstance(row.get("plugin_config_overrides"), dict) else {}
    topology = row.get("topology_point") if isinstance(row.get("topology_point"), dict) else {}
    return {
        "plugins": overrides,
        "plugin_configs": configs,
        "requested_worker_count": _as_int(topology.get("worker_count")),
    }


def apply_fairness_fidelity_gate(rows: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    enriched: list[dict[str, Any]] = []
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        item = dict(row)
        stack = _row_stack(item)
        item["v16_declared_execution_stack_digest"] = _sha(stack)
        item["v16_requested_worker_count"] = stack["requested_worker_count"]
        group_id = str(item.get("comparison_group_id") or "")
        groups.setdefault(group_id, []).append(item)
        enriched.append(item)

    group_modes: dict[str, dict[str, Any]] = {}
    all_controlled = True
    for group_id, members in groups.items():
        digests = {str(x.get("v16_declared_execution_stack_digest")) for x in members}
        # Same declared stack means a controlled comparison at plan time. Different stacks are still runnable,
        # but they are complete-system observations rather than a single-variable uplift experiment.
        controlled = len(digests) <= 1
        mode = "controlled_single_variable" if controlled else "complete_system"
        group_modes[group_id] = {
            "mode": mode,
            "declared_execution_stack_equal": controlled,
            "row_count": len(members),
        }
        all_controlled = all_controlled and controlled
        for member in members:
            member["v16_comparison_mode"] = mode
            member["v16_direct_single_variable_comparison_planned"] = controlled

    out = dict(report or {})
    out["v16_fairness_contract_version"] = CLOSURE_VERSION
    out["v16_comparison_groups"] = group_modes
    out["v16_direct_single_variable_plan_fairness_valid"] = all_controlled
    out["v16_legacy_structural_fairness_passed"] = bool(out.get("passed"))
    out["v16_runtime_effective_worker_validation_stage"] = "post_run_group_truth_gate"
    if out.get("passed") and not all_controlled:
        warnings = list(out.get("warnings") or [])
        warnings.append("legacy_structural_fairness_passed_but_declared_execution_stacks_differ")
        out["warnings"] = warnings
    return enriched, out


# ---- v15 compatibility surface: semantics are upgraded, names remain importable. ----
def compute_txallo_placement_evidence(run_dir: Path | str) -> dict[str, Any]:
    evidence = compute_txallo_evidence(run_dir)
    placement = evidence["transaction_placement"]
    return {
        "status": placement["status"],
        "digest": placement["digest"],
        "row_count": placement["row_count"],
        "files": placement["files"],
        "source_version": "txallo_transaction_placement_evidence_v16",
    }


def enrich_extracted_metrics(run_dir: Path | str, method_id: str | None, result: dict[str, Any]) -> dict[str, Any]:
    return enrich_metrics(run_dir, method_id, result)


def apply_truth_closure_to_state_equivalence(items: list[dict[str, Any]], report: dict[str, Any]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    return apply_group_fidelity_gate(items, report)
