"""V4 semantic replay projections of unchanged Axie and DCL CSV events.

This adapter NEVER claims to reproduce EVM storage read/write truth. It provides
one deterministic declared scheduling projection and one coherent runtime model
for all MBE consumers. Actual sender/receiver, original tx hashes and row order
remain attached to every record. No future execution/consensus information is used.
"""
from __future__ import annotations

import csv
import hashlib
import json
import io
import itertools
from collections import Counter
from pathlib import Path
from typing import Any, Iterator

from backend.app.services.workload_adapters.base import SourceValidationSummary
from backend.app.services.workload_adapters.common import parse_timestamp_ms, sha256_file

ZERO = "0x" + "0" * 40
SCHEMA = "mbe_workload_record_v4"


def _entry(key: str, mode: str, semantics: str, delta: int = 0) -> dict[str, Any]:
    entry: dict[str, Any] = {"key": key, "mode": mode, "update_semantics": semantics}
    if delta:
        entry["delta"] = delta
    return entry


def _canonical_record(*, dataset_id: str, idx: int, event_id: str, tx_hash: str,
                      timestamp_ms: int, sender: str, receiver: str, operation: str,
                      access: list[dict[str, Any]], source_key: str, target_key: str,
                      skew_keys: dict[str, str], metadata: dict[str, Any], provenance: dict[str, Any]) -> dict[str, Any]:
    by_key: dict[str, dict[str, Any]] = {}
    for item in access:
        if item["key"] in by_key:
            raise ValueError(f"row {idx}: repeated semantic access key {item['key']}")
        by_key[item["key"]] = item
    if not by_key or source_key not in by_key or (target_key and target_key not in by_key):
        raise ValueError(f"row {idx}: routing keys must be in the static envelope")
    # V4 scheduling item digest requires no delta; runtime item can carry delta.
    runtime = [by_key[k] for k in sorted(by_key)]
    scheduling = [{"key": x["key"], "mode": x["mode"], "update_semantics": x["update_semantics"]} for x in runtime]
    def digest(items: list[dict[str, Any]]) -> str:
        return hashlib.sha256(json.dumps(items, ensure_ascii=False, separators=(",", ":")).encode("utf-8")).hexdigest()
    return {
        "schema_version": SCHEMA,
        "dataset_id": dataset_id,
        "source_row_index": idx,
        "source_event_id": event_id,
        "source_tx_hash": tx_hash or None,
        "timestamp_ms": timestamp_ms,
        "sender_id": sender,
        "receiver_id": receiver,
        "operation_type": operation,
        "runtime_value": 1,
        "state_keys": sorted(by_key),
        "static_access_envelope": {"keys": sorted(by_key), "complete": True,
                                  "policy_id": "mv_observed_role_semantic_projection_v1",
                                  "construction_method": "source_roles_only_not_chain_storage_truth"},
        "scheduling_access_schema": "mv_declared_scheduling_v1",
        "scheduling_access_source": "source_roles_semantic_projection_no_execution_oracle",
        "scheduling_access_list": scheduling,
        "scheduling_access_digest": digest(scheduling),
        "access_list_schema": "mv_runtime_semantic_v1",
        "access_list_source": "source_roles_semantic_projection_not_onchain_readwrite",
        "access_list": runtime,
        "access_list_digest": digest(runtime),
        "routing_source_key": source_key,
        "routing_target_key": target_key,
        "skew_keys": skew_keys,
        "metadata": {**metadata, "read_write_truth": "modeled_replay_semantics_not_observed_chain_storage"},
        "provenance": {**provenance, "projection_policy": "mv_layered_v4_v1", "native_access_trace_available": False},
    }


class _Base:
    """Source validation is bounded-memory, and never writes the input CSV."""
    required_columns: frozenset[str]
    adapter_id: str

    def validate_source(self, path: Path, manifest: dict[str, Any], *, expected_sha256: str | None = None) -> SourceValidationSummary:
        filehash = sha256_file(path)
        if expected_sha256 and filehash.lower() != expected_sha256.lower():
            raise ValueError("source SHA256 differs from registered original CSV")
        counts: Counter[str] = Counter()
        seen: set[str] = set()
        tx_hashes: set[str] = set()
        begin = end = None
        previous = -1
        for rec in self.iter_canonical_records(path, manifest):
            t = rec["timestamp_ms"]
            if t < previous:
                raise ValueError(f"row {rec['source_row_index']}: timestamp order decreased; refusing to reorder source rows")
            previous = t
            identifier = rec["source_event_id"]
            if identifier in seen:
                raise ValueError(f"duplicate source event ID {identifier}")
            seen.add(identifier)
            if rec["source_tx_hash"]:
                tx_hashes.add(rec["source_tx_hash"])
            counts[rec["operation_type"]] += 1
            begin = t if begin is None else min(begin, t)
            end = t if end is None else max(end, t)
        if not seen:
            raise ValueError("empty source CSV")
        expected = int(manifest.get("row_count") or 0)
        if expected and len(seen) != expected:
            raise ValueError(f"source count changed: {len(seen)} expected {expected}")
        return SourceValidationSummary(filehash, len(seen), len(tx_hashes), begin or 0, end or 0, dict(sorted(counts.items())))

    def _rows(self, path: Path, *, start: int = 0, stop: int | None = None,
              sparse_offsets: list[list[int]] | None = None) -> Iterator[tuple[int, dict[str, str]]]:
        # Raw records remain unchanged. The sparse index is consulted only when
        # every CSV event occupied exactly one physical line during the full
        # source gate; otherwise the same reader safely scans from row zero.
        with path.open("rb") as raw:
            header_line = raw.readline()
            header = next(csv.reader([header_line.decode("utf-8-sig")]))
            missing = self.required_columns - set(header)
            if missing:
                raise ValueError(f"missing source columns: {sorted(missing)}")
            origin = 0
            if sparse_offsets and start:
                candidates = [(int(row), int(offset)) for row, offset in sparse_offsets
                              if 0 <= int(row) <= start and int(offset) >= len(header_line)]
                if candidates:
                    origin, offset = max(candidates)
                    raw.seek(offset)
            stream = io.TextIOWrapper(raw, encoding="utf-8", newline="")
            reader = csv.DictReader(stream, fieldnames=header)
            for idx, row in enumerate(reader, origin):
                if idx < start:
                    continue
                if stop is not None and idx >= stop:
                    break
                yield idx, row

    def iter_canonical_window(self, path: Path, manifest: dict[str, Any], *,
                              start: int, stop: int,
                              sparse_offsets: list[list[int]] | None = None) -> Iterator[dict[str, Any]]:
        return self.iter_canonical_records(path, manifest, start=start, stop=stop,
                                           sparse_offsets=sparse_offsets)


class AxieLayeredV4Adapter(_Base):
    adapter_id = "mv_axie_layered_v4"
    required_columns = frozenset({
        "tx_seq", "block_time", "block_number", "tx_index", "tx_hash", "tx_from", "tx_to",
        "axie_from", "axie_to", "axie_token_id", "axie_transfer_count", "event_kind",
        "accesses_marketplace_hotspot", "operation_class",
    })

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any], *, start: int = 0,
                               stop: int | None = None, sparse_offsets: list[list[int]] | None = None) -> Iterator[dict[str, Any]]:
        for idx, row in self._rows(path, start=start, stop=stop, sparse_offsets=sparse_offsets):
            event = row["tx_seq"].strip()
            tx_hash = row["tx_hash"].strip().lower()
            raw_from = row["axie_from"].strip().lower()
            raw_to = row["axie_to"].strip().lower()
            sender = raw_from or row["tx_from"].strip().lower()
            receiver = raw_to or row["tx_to"].strip().lower()
            token = row["axie_token_id"].strip()
            contract = row["tx_to"].strip().lower()
            event_kind = row["event_kind"].strip().lower()
            if not event or not tx_hash or not sender or not receiver or not token or not contract:
                raise ValueError(f"row {idx}: missing Axie transfer relationship")
            if event_kind not in {"mint", "transfer"}:
                raise ValueError(f"row {idx}: unknown event_kind={event_kind!r}")
            mint = event_kind == "mint"
            self_transfer = sender == receiver and not mint
            sender_key = "axie_account:" + sender
            receiver_key = "axie_account:" + receiver
            # Axie token identity is independently observed. tx_to is a call
            # destination (marketplace or NFT contract) and must not be used to
            # fragment one token identity into distinct state keys.
            token_key = "axie_token:" + token
            access = []
            if not mint and not self_transfer:
                access.append(_entry(sender_key, "read_write", "axie_owner_debit"))
            if not self_transfer:
                access.append(_entry(receiver_key, "read_write", "axie_owner_credit"))
            elif sender != ZERO:
                access.append(_entry(sender_key, "read", "axie_self_transfer_identity"))
            access.append(_entry(token_key, "read" if self_transfer else "read_write", "axie_ownership_transfer"))
            # Marketplace hotspot is a source classification, not proof of a
            # global R/W storage slot. Preserve flag only in source metadata.
            market = row["accesses_marketplace_hotspot"].strip().lower() in {"true", "1", "yes"}
            return_op = "axie_mint" if mint else "axie_self_transfer" if self_transfer else "axie_transfer"
            yield _canonical_record(
                dataset_id=manifest["dataset_id"], idx=idx, event_id=event, tx_hash=tx_hash,
                timestamp_ms=parse_timestamp_ms(row["block_time"]), sender=sender, receiver=receiver,
                operation=return_op, access=access,
                source_key=(receiver_key if mint else sender_key), target_key=token_key,
                skew_keys={"axie_token": token_key, "receiver": receiver_key,
                           "contract": "axie_call_target:" + contract,
                           "marketplace": "axie_call_target:" + contract},
                metadata={"original_event_kind": row["event_kind"], "source_tx_hash": tx_hash,
                          "source_tx_seq": event, "raw_axie_from": raw_from, "raw_axie_to": raw_to,
                          "tx_from": row["tx_from"].strip().lower(), "tx_to": contract,
                          "block_number_raw": row["block_number"], "tx_index_raw": row["tx_index"],
                          "axie_transfer_count_raw": row["axie_transfer_count"],
                          "operation_class": row["operation_class"],
                          "marketplace_hotspot_observed_flag": market, "self_transfer": self_transfer,
                          "mint": mint, "zero_address_not_debited": mint,
                          "global_marketplace_write_inferred": False,
                          "asset_id_scope": "axie_dataset_token_id_no_contract_inference"},
                provenance={"source_chain": "ronin", "source_dataset": "axie_2021_10_01_full_day",
                            "source_event_unit": "one_original_csv_event_not_aggregated_tx"},
            )


class DCLLayeredV4Adapter(_Base):
    adapter_id = "mv_dcl_layered_v4"
    required_columns = frozenset({"id", "tx_hash", "buyer", "seller", "price", "timestamp", "category", "raw_contract_candidates"})

    def iter_canonical_records(self, path: Path, manifest: dict[str, Any], *, start: int = 0,
                               stop: int | None = None, sparse_offsets: list[list[int]] | None = None) -> Iterator[dict[str, Any]]:
        from decimal import Decimal, InvalidOperation
        import re
        for idx, row in self._rows(path, start=start, stop=stop, sparse_offsets=sparse_offsets):
            event = row["id"].strip()
            tx_hash = row["tx_hash"].strip().lower()
            buyer = row["buyer"].strip().lower()
            seller = row["seller"].strip().lower()
            category = row["category"].strip().lower()
            contracts = re.findall(r"0x[a-fA-F0-9]{40}", row["raw_contract_candidates"])
            if not event or not tx_hash or not buyer or not seller or category not in {"wearable", "emote"} or len(contracts) != 1:
                raise ValueError(f"row {idx}: incomplete or ambiguous DCL sale")
            if not all(re.fullmatch(r"0x[a-f0-9]{40}", a) for a in (buyer, seller)) or not re.fullmatch(r"0x[a-f0-9]{64}", tx_hash):
                raise ValueError(f"row {idx}: invalid buyer/seller/tx_hash")
            try:
                price = Decimal(row["price"].strip())
            except InvalidOperation as exc:
                raise ValueError(f"row {idx}: invalid decimal price") from exc
            if not price.is_finite() or price < 0:
                raise ValueError(f"row {idx}: negative or nonfinite price")
            contract = contracts[0].lower()
            # One logical account address => identical key in either sale role.
            # Old v2 template used different runtime sender/receiver namespaces.
            bb, sb = "dcl_balance:" + buyer, "dcl_balance:" + seller
            bn, sn = "dcl_nonce:" + buyer, "dcl_nonce:" + seller
            market = "market:" + contract
            cat = "category:" + category
            if buyer == seller:
                access = [_entry(bb, "read_write", "buyer_balance"),
                          _entry(bn, "read_write", "buyer_nonce")]
            else:
                access = [_entry(bb, "read_write", "buyer_balance"),
                          _entry(bn, "read_write", "buyer_nonce"),
                          _entry(sb, "read_write", "seller_balance"),
                          _entry(sn, "read", "seller_nonce_state")]
            # Market sale aggregate is the pre-existing MBE replay counter, NOT
            # a claim about an observed on-chain storage key or NFT token ID.
            access += [_entry(market, "commutative_delta", "market_sale_counter", 1),
                       _entry(cat, "read", "category_metadata")]
            yield _canonical_record(
                dataset_id=manifest["dataset_id"], idx=idx, event_id=event, tx_hash=tx_hash,
                timestamp_ms=int(row["timestamp"].strip()), sender=buyer, receiver=seller,
                operation=category, access=access, source_key=bb, target_key=market,
                skew_keys={"contract": market, "receiver": sb},
                metadata={"original_sale_id": event, "source_tx_hash": tx_hash,
                          "buyer": buyer, "seller": seller, "contract": contract,
                          "category": category, "price_raw": row["price"].strip(),
                          "buyer_equals_seller": buyer == seller,
                          "asset_token_id_available": False,
                          "sale_counter_semantics": "modeled_commutative_aggregate_not_observed_chain_slot"},
                provenance={"source_chain": "polygon_mainnet", "source_dataset": "dcl_sales_polygon_271868",
                            "source_event_unit": "one_original_sale_event_not_deduped_by_tx_hash"},
            )
