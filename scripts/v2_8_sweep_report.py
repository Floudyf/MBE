from __future__ import annotations

import argparse
import json
from pathlib import Path

from backend.app.services.sweep_runner_v2 import ROOT, run_sweep


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a V2.8 local sweep and write report artifacts.")
    parser.add_argument("--config", required=True, help="Sweep config path inside the workspace.")
    parser.add_argument("--out", required=True, help="Output directory for sweep artifacts.")
    args = parser.parse_args()

    result = run_sweep(Path(args.config), Path(args.out), root=ROOT)
    print(json.dumps({"status": result["status"], "summary": result["summary"], "output_dir": str(Path(args.out))}, indent=2))


if __name__ == "__main__":
    main()
