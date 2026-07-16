from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.app.services.v5_workload_data_plane import validate_csv, write_validation_report

parser = argparse.ArgumentParser(description="Validate the read-only Decentraland sales CSV.")
parser.add_argument("--input", type=Path, required=True)
parser.add_argument("--reports-root", type=Path, default=Path(".cache/workloads/reports"))
args = parser.parse_args()
try:
    summary = validate_csv(args.input)
    report = write_validation_report(summary, args.reports_root)
except (OSError, ValueError) as exc:
    parser.exit(2, f"validation failed: {str(exc).splitlines()[0]}\n")
print(json.dumps({"summary": summary.__dict__, "report": report.name}, sort_keys=True))
