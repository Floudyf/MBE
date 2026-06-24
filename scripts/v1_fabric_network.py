from __future__ import annotations
import argparse,sys
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT))
from chain.fabric.network.fabric_network import detect_fabric_environment,build_test_network_commands
def main():
 p=argparse.ArgumentParser();p.add_argument('--check',action='store_true');p.add_argument('--dry-run',action='store_true');p.add_argument('--strict',action='store_true');p.add_argument('--action',choices=['up','down','create-channel','deploy-asset','deploy-scene','deploy-reward']);a=p.parse_args();s=detect_fabric_environment(ROOT,a.strict)
 if s.missing: print('Fabric environment skipped: '+', '.join(s.missing)); return 1 if a.strict else 0
 print('Fabric environment ready for manual use')
 if a.dry_run: print(build_test_network_commands(ROOT,s.fabric_samples_dir)[a.action or 'up'])
 return 0
if __name__=='__main__': raise SystemExit(main())
