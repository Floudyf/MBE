from __future__ import annotations


REMOTE_FETCH_KIND = "fetch"
REMOTE_UNKNOWN_KIND = "unknown"
REMOTE_WRITEBACK_KIND = "writeback"


def is_remote_writeback_kind(value: object) -> bool:
    return str(value or "").strip().startswith("write_apply")


def normalize_remote_operation_kind(value: object) -> str:
    kind = str(value or "").strip()
    if is_remote_writeback_kind(kind):
        return REMOTE_WRITEBACK_KIND
    if kind in {"read", "read_write", "commutative_delta"}:
        return REMOTE_FETCH_KIND
    return REMOTE_UNKNOWN_KIND


def remote_operation_dedup_key(row: dict[str, object]) -> tuple[str, ...]:
    normalized = normalize_remote_operation_kind(row.get("access_kind"))
    execution_shard = str(row.get("execution_shard") or "")
    home_shard = str(row.get("home_shard") or "")
    state_key = str(row.get("state_key") or "")
    if normalized == REMOTE_WRITEBACK_KIND:
        source_block_hash = str(row.get("source_block_hash") or row.get("block_hash") or "")
        delta_id = str(row.get("delta_id") or "")
        if not delta_id:
            delta_id = "|".join(
                [
                    str(row.get("tx_id") or ""),
                    str(row.get("update_semantics") or ""),
                    str(row.get("witness_digest") or ""),
                ]
            )
        return (normalized, source_block_hash, execution_shard, home_shard, state_key, delta_id)
    return (normalized, str(row.get("block_hash") or ""), execution_shard, home_shard, state_key)


def summarize_remote_operations(rows: list[dict[str, object]], *, logical_tx_count: int = 0) -> dict[str, object]:
    successful = [row for row in rows if str(row.get("success") or "").strip().lower() in {"true", "1", "yes"}]
    fetch_count = sum(1 for row in successful if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_FETCH_KIND)
    writeback_count = sum(1 for row in successful if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_WRITEBACK_KIND)
    unknown_count = sum(1 for row in successful if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_UNKNOWN_KIND)
    dedup: dict[tuple[str, ...], dict[str, object]] = {}
    for row in successful:
        dedup.setdefault(remote_operation_dedup_key(row), row)
    dedup_fetch = sum(1 for row in dedup.values() if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_FETCH_KIND)
    dedup_writeback = sum(1 for row in dedup.values() if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_WRITEBACK_KIND)
    dedup_unknown = sum(1 for row in dedup.values() if normalize_remote_operation_kind(row.get("access_kind")) == REMOTE_UNKNOWN_KIND)
    dedup_total = len(dedup)
    return {
        "physical_remote_operation_count": len(successful),
        "physical_remote_fetch_count": fetch_count,
        "physical_remote_writeback_count": writeback_count,
        "physical_remote_failed_count": max(len(rows) - len(successful), 0),
        "remote_operation_unknown_kind_count": unknown_count,
        "replica_deduplicated_remote_operation_count": dedup_total,
        "replica_deduplicated_remote_fetch_count": dedup_fetch,
        "replica_deduplicated_remote_writeback_count": dedup_writeback,
        "replica_deduplicated_remote_unknown_kind_count": dedup_unknown,
        "remote_fetches_per_logical_tx": _ratio(dedup_fetch, logical_tx_count),
        "remote_writebacks_per_logical_tx": _ratio(dedup_writeback, logical_tx_count),
        "remote_operations_per_logical_tx": _ratio(dedup_total, logical_tx_count),
        "replica_amplification_factor": _ratio(len(successful), dedup_total),
        "remote_fetch_replica_amplification_factor": _ratio(fetch_count, dedup_fetch),
        "remote_writeback_replica_amplification_factor": _ratio(writeback_count, dedup_writeback),
    }


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator
