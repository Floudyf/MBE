from __future__ import annotations
import csv,json
from pathlib import Path
from backend.app.services.v5_porygon_truth_audit_v56 import export_porygon_truth_audit

def _write(path:Path,header,rows):
 path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('w',encoding='utf-8',newline='') as file:
  out=csv.writer(file);out.writerow(header);out.writerows(rows)

def test_porygon_v56_exact_identity_mismatch_not_masked(tmp_path):
 _write(tmp_path/'transaction_finality.csv',['logical_tx_id','success','terminal_stage'],[
    ['log1','true','durable_committed'],['log2','true','durable_committed']])
 _write(tmp_path/'transaction_lifecycle.csv',['logical_tx_id','tx_id'],[['log1','phys1'],['log2','phys2']])
 _write(tmp_path/'nodes/n0/porygon_paper_lifecycle_v56.csv',['tx_id','paper_status'],[
   ['phys1','committed'],['phys2','update_pending']])
 _write(tmp_path/'nodes/n1/porygon_paper_lifecycle_v56.csv',['tx_id','paper_status'],[
   ['phys1','committed'],['phys2','committed']])
 (tmp_path/'nodes/n0/block_execution_summary.json').write_text(json.dumps({'block_executor_id':'porygon_block_executor','blocks':[{'height':6,'block_hash':'abc','porygon_business_execution_us':150000,'porygon_result_exchange_wait_us':400000,'porygon_execution_critical_path_us':600000}]}))
 got=export_porygon_truth_audit(tmp_path)
 assert got['porygon_v56_identity_audit_available']
 assert got['porygon_v56_successful_finality_identity_exception_count']==1
 assert got['porygon_v56_wait_audit_row_count']==1
 with (tmp_path/'porygon_identity_audit_v56.csv').open() as f:
  rows=list(csv.DictReader(f))
 assert rows[1]['logical_tx_id']=='log2' and rows[1]['paper_any_noncommitted']=='true'
 with (tmp_path/'porygon_height_wait_v56.csv').open() as f:
  waits=list(csv.DictReader(f))
 assert waits[0]['other_critical_us']=='50000'

def test_porygon_v56_missing_evidence_remains_unknown(tmp_path):
 got=export_porygon_truth_audit(tmp_path)
 assert got['porygon_v56_identity_audit_available'] is False
 assert got['porygon_v56_successful_finality_identity_exception_count'] is None
 assert not (tmp_path/'porygon_identity_audit_v56.csv').exists()

def test_porygon_v56_ambiguous_mapping_never_claims_commit(tmp_path):
 _write(tmp_path/'transaction_finality.csv',['logical_tx_id','success'],[['log1','true']])
 _write(tmp_path/'transaction_lifecycle.csv',['logical_tx_id','tx_id'],[['log1','phys1'],['log1','phys2']])
 _write(tmp_path/'nodes/n0/porygon_paper_lifecycle_v56.csv',['tx_id','paper_status'],[['phys1','committed'],['phys2','committed']])
 got=export_porygon_truth_audit(tmp_path)
 assert got['porygon_v56_ambiguous_mapping_count']==1
 assert got['porygon_v56_successful_finality_identity_exception_count']==1
