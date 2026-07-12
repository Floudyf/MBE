from __future__ import annotations

import json
from pathlib import Path


def extract(run_dir: Path) -> dict:
    summary_path = run_dir / "real_cluster_summary.json"
    finality_path = run_dir / "finality_summary.json"
    if not summary_path.is_file() or not finality_path.is_file():
        return {"missing": [name for name, path in {"real_cluster_summary.json": summary_path, "finality_summary.json": finality_path}.items() if not path.is_file()]}
    cluster = json.loads(summary_path.read_text(encoding="utf-8")); finality = json.loads(finality_path.read_text(encoding="utf-8"))
    required = ["transaction_lifecycle.jsonl", "transaction_finality.csv", "client_receipt_log.csv", "finality_summary.json", "real_cluster_summary.json"]
    missing = [name for name in required if not (run_dir / name).is_file()]
    return {"finalized_tx_count": finality.get("finalized_unique_logical_tx_count"), "throughput_tps": finality.get("throughput_tps"), "p50_latency_ms": finality.get("p50_finality_ms"), "p95_latency_ms": finality.get("p95_finality_ms"), "p99_latency_ms": finality.get("p99_finality_ms"), "state_root_consistent": cluster.get("state_root_consistent"), "orphan_process_count": cluster.get("orphan_process_count"), "no_fallback": cluster.get("no_fallback"), "lifecycle_complete": finality.get("logical_transaction_count") == finality.get("finalized_unique_logical_tx_count"), "source_artifacts": required, "missing": missing}
