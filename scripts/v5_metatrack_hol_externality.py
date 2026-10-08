from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.v5_metatrack_hol_externality import analyze_group

def main() -> int:
    ap = argparse.ArgumentParser(description="Analyze MetaTrack multi-frontier FIFO HOL externality.")
    ap.add_argument("--repo-root", default=str(ROOT))
    ap.add_argument("--group-id", default=None)
    args = ap.parse_args()
    summary = analyze_group(Path(args.repo_root), args.group_id)
    print("============================================================")
    print("MetaTrack HOL Externality v1.1")
    print("============================================================")
    print("group_id:", summary["group_id"])
    print("paired_repeat_count:", summary["paired_repeat_count"])
    print("evidence_modes:", ",".join(summary["evidence_modes"]))
    print("mean_multi_frontier_transaction_share:", summary["mean_multi_frontier_transaction_share"])
    print("mean_multi_frontier_hol_share:", summary["mean_multi_frontier_hol_share"])
    print("mean_multi_frontier_hol_amplification_vs_tx_share:", summary["mean_multi_frontier_hol_amplification_vs_tx_share"])
    print("total_multi_frontier_fast_eligible_victim_count:", summary["total_multi_frontier_fast_eligible_victim_count"])
    print("total_multi_frontier_fast_eligible_hol_ms:", summary["total_multi_frontier_fast_eligible_hol_ms"])
    print("mean_legacy_calibration_abs_error_pct:", summary["mean_legacy_calibration_abs_error_pct"])
    print("output_dir:", summary["output_dir"])
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
