import json,subprocess,sys
from pathlib import Path
import yaml
ROOT=Path(__file__).resolve().parents[1]
def test_v18_sweep_dry_run_generates_baselines_and_report(tmp_path):
 spec=yaml.safe_load((ROOT/'configs/sweeps/v1_8_baselines.yaml').read_text());assert [x['name'] for x in spec['baselines']]==['baseline_hash_only','co_access_only','co_access_dual_track','full_v1']
 out=tmp_path/'out';subprocess.run([sys.executable,'scripts/v1_8_sweep.py','--dry-run','--out',str(out)],cwd=ROOT,check=True);rows=json.loads((out/'sweep_summary.json').read_text());assert len(rows)==4 and (out/'report.md').is_file()
