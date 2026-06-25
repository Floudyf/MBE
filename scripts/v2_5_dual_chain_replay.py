from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backend.app.services.dual_chain_replay import run_dual_chain_replay


def main() -> int:
    parser = argparse.ArgumentParser(description="Run V2.5 local virtual-time dual-chain replay.")
    parser.add_argument("--config", default="configs/experiments/v2_dual_chain_sample.yaml")
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    result = run_dual_chain_replay(Path(args.config), Path(args.out))
    print(json.dumps({"status": result["status"], "output_dir": result["output_dir"], "summary": result["summary"]}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
