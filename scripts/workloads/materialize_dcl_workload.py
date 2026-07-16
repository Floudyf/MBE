from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.app.services.v5_workload_data_plane import materialize

parser = argparse.ArgumentParser(description="Materialize deterministic original or key-Zipf DCL workload records.")
parser.add_argument("--canonical", type=Path, required=True)
parser.add_argument("--dataset-id", default="dcl_sales_polygon_271868")
parser.add_argument("--source-sha256", required=True)
parser.add_argument("--variant", choices=("original_window", "contract_zipf", "key_zipf"), default="original_window")
parser.add_argument("--skew-axis", default=None)
parser.add_argument("--tx-count", type=int, required=True)
parser.add_argument("--seed", type=int, required=True)
parser.add_argument("--alpha", type=float)
parser.add_argument("--cache-root", type=Path, default=Path(".cache/workloads"))
args = parser.parse_args()
try:
    print(json.dumps(materialize(args.canonical, args.cache_root, dataset_id=args.dataset_id, source_sha256=args.source_sha256, requested_tx_count=args.tx_count, seed=args.seed, variant_mode=args.variant, target_alpha=args.alpha, skew_axis=args.skew_axis), sort_keys=True))
except (OSError, ValueError) as exc:
    parser.exit(2, f"materialization failed: {str(exc).splitlines()[0]}\n")
