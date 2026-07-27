from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import shutil
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


METHODS = ("hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm")


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(encoding="utf-8", newline="") as handle:
        return list(csv.DictReader(handle))


def read_access_rows(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def stable_sha256(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def access_mode_counts(accesses: list[dict[str, Any]]) -> tuple[int, int]:
    reads = writes = 0
    for item in accesses:
        mode = item.get("mode")
        if mode in {"read", "read_write", "commutative_delta"}:
            reads += 1
        if mode in {"write", "read_write", "commutative_delta"}:
            writes += 1
    return reads, writes


def reconstruct(rows: list[dict[str, Any]], block_size: int) -> dict[str, Any]:
    matrix: set[tuple[int, str, int, str, str]] = set()
    frequency: dict[tuple[int, str], Counter[str]] = defaultdict(Counter)
    edges: Counter[tuple[int, str, str]] = Counter()
    for position, row in enumerate(rows):
        batch = position // block_size
        tx_index = int(row["index"])
        logical_id = row.get("logical_id") or row["tx_id"]
        accesses = row.get("access_list") or []
        keys: list[str] = []
        seen: set[str] = set()
        for access in accesses:
            key = access["key"]
            mode = access["mode"]
            if key in seen:
                continue
            seen.add(key)
            keys.append(key)
            matrix.add((batch, logical_id, tx_index, key, mode))
            frequency[(batch, key)]["frequency"] += 1
            if mode in {"read", "read_write", "commutative_delta"}:
                frequency[(batch, key)]["read_count"] += 1
            if mode in {"write", "read_write", "commutative_delta"}:
                frequency[(batch, key)]["write_count"] += 1
        keys.sort()
        for left in range(len(keys)):
            for right in range(left + 1, len(keys)):
                edges[(batch, keys[left], keys[right])] += 1
    return {
        "access_matrix": sorted(list(matrix)),
        "state_frequency": sorted(
            (batch, key, counts["frequency"], counts["read_count"], counts["write_count"])
            for (batch, key), counts in frequency.items()
        ),
        "coaccess_edges": sorted((batch, left, right, weight) for (batch, left, right), weight in edges.items()),
    }


def planner_reconstruction(method_dir: Path) -> dict[str, Any] | None:
    client = method_dir / "client"
    matrix_path = client / "access_matrix_summary.csv"
    frequency_path = client / "state_frequency.csv"
    edges_path = client / "coaccess_matrix_edges.csv"
    if not matrix_path.is_file() or not frequency_path.is_file() or not edges_path.is_file():
        return None
    matrix = sorted(
        (
            int(row["batch_index"]),
            row["logical_id"],
            int(row["tx_index"]),
            row["state_key"],
            row["mode"],
        )
        for row in read_csv(matrix_path)
    )
    frequency = sorted(
        (
            int(row["batch_index"]),
            row["state_key"],
            int(row["frequency"]),
            int(row["read_count"]),
            int(row["write_count"]),
        )
        for row in read_csv(frequency_path)
    )
    edges = sorted(
        (
            int(row["batch_index"]),
            row["left_key"],
            row["right_key"],
            int(row["weight"]),
        )
        for row in read_csv(edges_path)
    )
    return {"access_matrix": matrix, "state_frequency": frequency, "coaccess_edges": edges}


def block_size(method_dir: Path) -> int:
    plan = read_json(method_dir / "compiled_run_plan.json")
    profile = plan["node_configs"][0]["plugin_profile"]
    return int(profile["block_producer"]["config"].get("block_size") or 10)


def observed_market_and_category(run_root: Path) -> tuple[bool, bool]:
    market_write = False
    category_read = False
    for method in METHODS:
        nodes = run_root / method / "nodes"
        if not nodes.is_dir():
            continue
        for path in nodes.glob("*/observed_state_access.csv"):
            for row in read_csv(path):
                key = row.get("state_key", "")
                if row.get("access_type") == "write" and key.startswith("market:"):
                    market_write = True
                if row.get("access_type") == "read" and key.startswith("category:"):
                    category_read = True
    return market_write, category_read


def summary_blockers(summary: dict[str, Any]) -> list[str]:
    blockers: list[str] = []
    if summary.get("empty_access_list_count") != 0:
        blockers.append("empty access list observed")
    if summary.get("duplicate_key_count") != 0:
        blockers.append("duplicate access key observed")
    if summary.get("legacy_account_alias_count") != 0:
        blockers.append("legacy account/contract alias observed")
    if summary.get("commutative_delta_count", 0) <= 0:
        blockers.append("market commutative delta access missing")
    if summary.get("read_count", 0) <= 0:
        blockers.append("read access count missing")
    return blockers


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify Stage A real dataset resolved access-list evidence.")
    parser.add_argument("--run-root", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--tx-count", required=True, type=int)
    args = parser.parse_args()
    run_root = args.run_root.resolve()
    output = args.output.resolve()
    if output.exists() and any(output.iterdir()):
        raise SystemExit(f"stage A output must be empty: {output}")
    output.mkdir(parents=True, exist_ok=True)

    per_method: dict[str, dict[str, Any]] = {}
    blockers: list[str] = []
    for method in METHODS:
        method_dir = run_root / method
        access_path = method_dir / "client" / "resolved_access_lists.jsonl.gz"
        summary_path = method_dir / "client" / "access_list_summary.json"
        digest_path = method_dir / "client" / "access_list_digest.txt"
        if not access_path.is_file() or not summary_path.is_file() or not digest_path.is_file():
            blockers.append(f"{method}: resolved access artifacts missing")
            continue
        rows = read_access_rows(access_path)
        summary = read_json(summary_path)
        digest = digest_path.read_text(encoding="utf-8").strip()
        if len(rows) != args.tx_count or summary.get("transaction_count") != args.tx_count:
            blockers.append(f"{method}: transaction count mismatch")
        blockers.extend(f"{method}: {item}" for item in summary_blockers(summary))
        if method == "hash_serial":
            shutil.copyfile(access_path, output / "resolved_access_lists.jsonl.gz")
            shutil.copyfile(summary_path, output / "access_list_summary.json")
            shutil.copyfile(digest_path, output / "access_list_digest.txt")
        per_method[method] = {"digest": digest, "summary": summary, "rows": rows}

    digests = {method: item["digest"] for method, item in per_method.items()}
    cross_method_digest_equal = len(set(digests.values())) == 1 and len(digests) == len(METHODS)
    if not cross_method_digest_equal:
        blockers.append("cross-method resolved access digest mismatch")

    reconstruction_equal = True
    reconstruction_payload: dict[str, Any] = {}
    if "hash_serial" in per_method:
        expected = reconstruct(per_method["hash_serial"]["rows"], block_size(run_root / "hash_serial"))
        reconstruction_payload["independent"] = {
            "access_matrix_digest": stable_sha256(expected["access_matrix"]),
            "state_frequency_digest": stable_sha256(expected["state_frequency"]),
            "coaccess_edges_digest": stable_sha256(expected["coaccess_edges"]),
            "access_matrix_count": len(expected["access_matrix"]),
            "state_frequency_count": len(expected["state_frequency"]),
            "coaccess_edge_count": len(expected["coaccess_edges"]),
        }
        for method in ("metatrack_serial", "metatrack_block_stm"):
            planner = planner_reconstruction(run_root / method)
            if planner is None:
                blockers.append(f"{method}: planner X/F/W artifacts missing")
                reconstruction_equal = False
                continue
            equal = expected == planner
            reconstruction_payload[method] = {
                "equal": equal,
                "access_matrix_digest": stable_sha256(planner["access_matrix"]),
                "state_frequency_digest": stable_sha256(planner["state_frequency"]),
                "coaccess_edges_digest": stable_sha256(planner["coaccess_edges"]),
            }
            if not equal:
                reconstruction_equal = False
                blockers.append(f"{method}: independent X/F/W reconstruction mismatch")
    else:
        reconstruction_equal = False
        blockers.append("hash_serial resolved access rows unavailable for reconstruction")

    market_write, category_read = observed_market_and_category(run_root)
    if not market_write:
        blockers.append("market write not observed in real WriteSet")
    if not category_read:
        blockers.append("category read not observed in real ReadSet")

    (output / "coaccess_reconstruction.json").write_text(json.dumps(reconstruction_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    accepted = not blockers
    report = {
        "accepted": accepted,
        "formal_schema_v2": all(item["summary"].get("transaction_count") == args.tx_count for item in per_method.values()),
        "real_dataset_used": all((run_root / method / "workload_replay_summary.json").is_file() and read_json(run_root / method / "workload_replay_summary.json").get("dataset_id") == "dcl_sales_polygon_271868" for method in METHODS),
        "cross_method_digest_equal": cross_method_digest_equal,
        "coaccess_reconstruction_equal": reconstruction_equal,
        "market_write_observed": market_write,
        "category_read_observed": category_read,
        "empty_access_list_count": per_method.get("hash_serial", {}).get("summary", {}).get("empty_access_list_count"),
        "duplicate_key_count": per_method.get("hash_serial", {}).get("summary", {}).get("duplicate_key_count"),
        "legacy_account_alias_count": per_method.get("hash_serial", {}).get("summary", {}).get("legacy_account_alias_count"),
        "method_digests": digests,
        "blockers": blockers,
    }
    (output / "stage_a_access_list_acceptance.json").write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({"accepted": accepted, "blockers": blockers, "report": str(output / "stage_a_access_list_acceptance.json")}, sort_keys=True))
    return 0 if accepted else 1


if __name__ == "__main__":
    raise SystemExit(main())
