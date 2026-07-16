from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.app.services.v5_workload_data_plane import build_canonical

parser = argparse.ArgumentParser(description="Build deterministic canonical DCL JSONL.GZ.")
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--manifest", type=Path, default=Path("data/workloads/manifests/dcl_sales_polygon_271868.json"))
parser.add_argument("--cache-root", type=Path, default=Path(".cache/workloads"))
args = parser.parse_args()
try:
    print(json.dumps(build_canonical(args.input, args.cache_root, json.loads(args.manifest.read_text(encoding="utf-8"))), sort_keys=True))
except (OSError, ValueError, json.JSONDecodeError) as exc:
    parser.exit(2, f"canonical build failed: {str(exc).splitlines()[0]}\n")
