from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from chain.fabric.runner.fabric_smoke import SmokeConfig, SmokeEnvironmentError, run_smoke


def main() -> int:
    parser = argparse.ArgumentParser(description="V1.4-d local Fabric chain-backed smoke runner")
    parser.add_argument("--channel", default="mbechannel")
    parser.add_argument("--out", type=Path)
    parser.add_argument("--skip-network-up", action="store_true")
    parser.add_argument("--skip-deploy", action="store_true")
    parser.add_argument("--keep-network", action="store_true")
    parser.add_argument("--strict", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    output = args.out or ROOT / ".cache/fabric_smoke" / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    config = SmokeConfig(args.channel, output, args.skip_network_up, args.skip_deploy, args.keep_network, args.strict, args.dry_run)
    try:
        result = run_smoke(ROOT, config)
    except (SmokeEnvironmentError, RuntimeError) as error:
        print(f"Fabric smoke failed: {error}", file=sys.stderr)
        return 1
    if result.get("skipped"):
        print(f"Fabric smoke skipped: {result['reason']}")
    elif result.get("dry_run"):
        print("Fabric smoke dry-run plan:")
        print("\n".join(result["plan"]))
    else:
        print(f"wrote {result['raw_chain_log']}")
        print(f"wrote {result['trace']}")
        print(f"wrote {result['trace_meta']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
