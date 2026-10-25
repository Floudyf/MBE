"""Additive, auditable paper views for individually validated MBE child runs.

Never changes official performance_comparison_valid, paper_candidate, or original exports.
All comparison diagnostics describe observations, not causal explanations.
"""
from __future__ import annotations
import csv
import hashlib
import itertools
import json
import math
import os
import statistics
import tempfile
from collections import defaultdict
from pathlib import Path
from typing import Any

SCHEMA = 'mbe_paper_method_views_v1'
SAMPLES = 'paper_method_samples_v1.csv'
OBSERVED = 'paper_method_nonpaper_v1.csv'
SUMMARY = 'paper_method_summary_v1.csv'
PAIR = 'cross_method_state_audit_v1.csv'
META = 'paper_method_views_v1.json'
PLOT = 'paper_method_plot_v1.csv'

SAMPLE_FIELDS = ['group_id','dataset_id','variant_mode','selection_mode','source_sha256','tx_count','seed',
 'method_id','method_name','child_run_id','run_id','repeat_index','execution_status',
 'individual_result_valid','paper_candidate','method_correctness_oracle_status','v18_method_correctness_oracle_status',
 'tps','p99_ms','finalized_unique_logical_tx_count','effective_worker_count','performance_contract_class',
 'logical_transaction_identity_digest','logical_initial_state_digest','v17_logical_business_state_digest',
 'method_semantic_class','scope','paper_candidate_reasons']
SUM_FIELDS=['group_id','dataset_id','variant_mode','method_id','method_name','scope','count','tps_mean',
 'tps_sample_std','tps_min','tps_max','tps_ci95_low','tps_ci95_high','p99_mean','ci_note',
 'source_child_ids','performance_comparison_valid']
PAIR_FIELDS=['group_id','dataset_id','variant_mode','method_a','method_b','child_a','child_b',
 'both_individual_valid','both_paper_candidate','same_logical_transaction_identity','same_logical_initial_state',
 'same_physical_initial_state','same_state_home_mapping','same_final_v17_business_digest',
 'same_final_runtime_state_digest','same_serial_replay_input_digest','same_serial_commit_order_digest',
 'both_own_method_oracle_passed','same_execution_stack_digest','effective_worker_a','effective_worker_b',
 'cross_method_strict_performance_valid','evidence_status','cause_status',
 'requires_raw_state_delta_or_tx_order_for_causality','v17_digest_a','v17_digest_b']
PLOT_FIELDS=['group_id','dataset_id','variant_mode','method_id','method_name','metric','value',
 'sample_count','ci95_low','ci95_high','comparison_scope']

# Student t 0.975 criticals for 1..20 degrees of freedom (sample sizes 2..21).
T975=[None,12.706205,4.302653,3.182446,2.776445,2.570582,2.446912,2.364624,
 2.306004,2.262157,2.228139,2.200985,2.178813,2.160369,2.144787,2.131450,
 2.119905,2.109816,2.100922,2.093024,2.085963]

def _hex(value: Any) -> str:
    value=str(value or '')
    return value if len(value)==64 and all(c in '0123456789abcdef' for c in value.lower()) else ''

def _csv(path:Path,rows:list[dict],headers:list[str]):
    tmp=path.with_name('.'+path.name+'.writing')
    with tmp.open('w',encoding='utf-8-sig',newline='') as f:
        writer=csv.DictWriter(f,fieldnames=headers,extrasaction='ignore')
        writer.writeheader(); writer.writerows(rows)
    os.replace(tmp,path)

def _json(path:Path,obj:dict):
    tmp=path.with_name('.'+path.name+'.writing')
    tmp.write_text(json.dumps(obj,ensure_ascii=False,sort_keys=True,indent=2)+'\n',encoding='utf-8')
    os.replace(tmp,path)

def _tps(m:dict) -> float|None:
    # Do not fabricate a fallback between unrelated throughput metrics.
    for k in ('end_to_end_tps','throughput_tps'):
        v=m.get(k)
        if type(v) in (float,int) and math.isfinite(float(v)) and v>=0:return float(v)
    return None

def _runid(ch:dict)->str:
    v=str((ch.get('result') or {}).get('run_id') or '')
    return v if v.startswith('v5_') else ''

def _source(group:dict)->dict:
    return (((group.get('plan') or {}).get('base_spec') or {}).get('workload_source') or {})

def _method(ch:dict)->str:
    return str(ch.get('method_config_id') or (ch.get('method') or {}).get('method_id') or '')

def _reasons(ch:dict)->list[str]:
    raw=ch.get('paper_candidate_reasons') or ch.get('v17_fidelity_blockers') or []
    return [str(x) for x in raw] if isinstance(raw,list) else [str(raw)]

def _sample(group:dict,ch:dict)->dict:
    m=ch.get('metrics') or {}; src=_source(group); mid=_method(ch)
    method=ch.get('method') or {}
    valid=ch.get('individual_result_valid') is True and ch.get('status')=='completed'
    paper=ch.get('paper_candidate') is True
    oracle=ch.get('method_correctness_oracle_status') or m.get('method_correctness_oracle_status')
    v18=ch.get('v18_method_correctness_oracle_status') or m.get('v18_method_correctness_oracle_status')
    if valid and paper and oracle=='passed' and (not mid.endswith(('optme','txallo')) or v18=='passed'):
        scope='paper_individual_valid'
    elif valid and not paper and oracle=='passed' and (not mid.endswith(('optme','txallo')) or v18=='passed'):
        scope='individual_valid_not_paper_candidate'
    else:
        scope='excluded_or_insufficient_evidence'
    return {
        'group_id':group.get('run_group_id'),'dataset_id':src.get('dataset_id'),
        'variant_mode':src.get('variant_mode'),'selection_mode':src.get('selection_mode'),
        'source_sha256':src.get('source_sha256'),'tx_count':src.get('requested_tx_count'),
        'seed':ch.get('seed'),'method_id':mid,'method_name':method.get('display_name') or mid,
        'child_run_id':ch.get('child_run_id'),'run_id':_runid(ch),'repeat_index':ch.get('repeat_index'),
        'execution_status':ch.get('status'),'individual_result_valid':valid,'paper_candidate':paper,
        'method_correctness_oracle_status':oracle,'v18_method_correctness_oracle_status':v18,
        'tps':_tps(m),'p99_ms':m.get('p99_finality_ms'),
        'finalized_unique_logical_tx_count':m.get('finalized_unique_logical_tx_count'),
        'effective_worker_count':ch.get('effective_worker_count'),
        'performance_contract_class':ch.get('performance_contract_class'),
        'logical_transaction_identity_digest':ch.get('v16_logical_transaction_identity_digest') or m.get('v16_logical_transaction_identity_digest'),
        'logical_initial_state_digest':ch.get('logical_initial_state_digest') or m.get('logical_initial_state_digest'),
        'v17_logical_business_state_digest':ch.get('v17_logical_business_state_digest') or m.get('v17_logical_business_state_digest'),
        'method_semantic_class':ch.get('comparison_semantics_class'),
        'scope':scope,'paper_candidate_reasons':json.dumps(_reasons(ch),ensure_ascii=False),
    }

def _match_digest(a:dict,b:dict,key:str)->str:
    x=_hex(a.get(key)); y=_hex(b.get(key))
    return 'same' if x and y and x==y else 'different' if x and y else 'unproven'

def _state_pairs(group:dict, children:list[dict])->list[dict]:
    valid=[c for c in children if c.get('status')=='completed' and c.get('individual_result_valid') is True]
    out=[]; src=_source(group)
    for a,b in itertools.combinations(sorted(valid,key=lambda c:_method(c)),2):
        am=a.get('metrics') or {};bm=b.get('metrics') or {}
        da=a.get('v17_logical_business_state_digest') or am.get('v17_logical_business_state_digest')
        db=b.get('v17_logical_business_state_digest') or bm.get('v17_logical_business_state_digest')
        status='equal_v17_projection' if _hex(da) and _hex(db) and da==db else 'different_v17_projection' if _hex(da) and _hex(db) else 'missing_v17_projection'
        ai=a.get('v16_logical_transaction_identity_digest') or am.get('v16_logical_transaction_identity_digest')
        bi=b.get('v16_logical_transaction_identity_digest') or bm.get('v16_logical_transaction_identity_digest')
        x={
          'group_id':group.get('run_group_id'),'dataset_id':src.get('dataset_id'),
          'variant_mode':src.get('variant_mode'),'method_a':_method(a),'method_b':_method(b),
          'child_a':a.get('child_run_id'),'child_b':b.get('child_run_id'),
          'both_individual_valid':True,'both_paper_candidate':a.get('paper_candidate') is True and b.get('paper_candidate') is True,
          'same_logical_transaction_identity':'same' if _hex(ai) and ai==bi else 'different' if _hex(ai) and _hex(bi) else 'unproven',
          'same_logical_initial_state':_match_digest(a,b,'logical_initial_state_digest'),
          'same_physical_initial_state':_match_digest(a,b,'initial_state_digest'),
          'same_state_home_mapping':_match_digest(a,b,'state_home_mapping_digest'),
          'same_final_v17_business_digest':status,
          'same_final_runtime_state_digest':_match_digest(a,b,'global_final_state_digest'),
          'same_serial_replay_input_digest':_match_digest(a,b,'serial_order_replay_input_digest'),
          'same_serial_commit_order_digest':_match_digest(a,b,'serial_order_replay_commit_order_digest'),
          'both_own_method_oracle_passed':(a.get('method_correctness_oracle_status')=='passed' and b.get('method_correctness_oracle_status')=='passed'),
          'same_execution_stack_digest':_match_digest(a,b,'v16_execution_stack_digest'),
          'effective_worker_a':a.get('effective_worker_count'),'effective_worker_b':b.get('effective_worker_count'),
          'cross_method_strict_performance_valid':group.get('performance_comparison_valid') is True,
          'evidence_status':status,'cause_status':('no_v17_difference_observed' if status=='equal_v17_projection' else 'NOT_ATTRIBUTED_FROM_AGGREGATE_DIGESTS'),
          'requires_raw_state_delta_or_tx_order_for_causality':status!='equal_v17_projection',
          'v17_digest_a':da,'v17_digest_b':db,
        };out.append(x)
    return out

def _summarize(group:dict,samples:list[dict])->tuple[list[dict],list[dict]]:
    by=defaultdict(list)
    for row in samples:
        if row['scope']=='paper_individual_valid' and row['tps'] is not None:
            by[(row['dataset_id'],row['variant_mode'],row['method_id'],row['method_name'])].append(row)
    results=[];plots=[]
    for (dataset,variant,method,name),records in sorted(by.items()):
        values=[float(r['tps']) for r in records];n=len(values);avg=statistics.mean(values)
        std=statistics.stdev(values) if n>=2 else None
        if n>=2 and std is not None:
            degrees=n-1
            t=T975[degrees] if degrees<len(T975) else 1.96
            delta=t*std/math.sqrt(n)
            low=max(0.0,avg-delta);high=avg+delta
        else:low=high=None
        p99s=[float(r['p99_ms']) for r in records if type(r['p99_ms']) in (float,int)]
        row={'group_id':group.get('run_group_id'),'dataset_id':dataset,'variant_mode':variant,
             'method_id':method,'method_name':name,'scope':'independent_validated_method_measurement',
             'count':n,'tps_mean':avg,'tps_sample_std':std,'tps_min':min(values),'tps_max':max(values),
             'tps_ci95_low':low,'tps_ci95_high':high,
             'p99_mean':statistics.mean(p99s) if p99s else None,
             'ci_note':'single_sample_no_CI' if n==1 else 'student_t_95pct_approx_independent_samples',
             'source_child_ids':'|'.join(str(x['child_run_id']) for x in records),
             'performance_comparison_valid':group.get('performance_comparison_valid') is True}
        results.append(row)
        for metric,value,lo,hi in [('end_to_end_tps',avg,low,high),('p99_finality_ms',row['p99_mean'],None,None)]:
            if value is not None:plots.append({'group_id':row['group_id'],'dataset_id':dataset,'variant_mode':variant,
                        'method_id':method,'method_name':name,'metric':metric,'value':value,'sample_count':n,
                        'ci95_low':lo,'ci95_high':hi,'comparison_scope':'independent_method_observation_not_speedup'})
    return results,plots

def export_views(group_dir:Path|str,group:dict,children:list[dict])->dict:
    """Only writes additional evidence files. No mutation of source group/children."""
    root=Path(group_dir);root.mkdir(parents=True,exist_ok=True)
    rows=[_sample(group,ch) for ch in children]
    accepted=[r for r in rows if r['scope']=='paper_individual_valid']
    notpaper=[r for r in rows if r['scope']!='paper_individual_valid']
    pairs=_state_pairs(group,children)
    summary,plots=_summarize(group,accepted)
    _csv(root/SAMPLES,accepted,SAMPLE_FIELDS)
    _csv(root/OBSERVED,notpaper,SAMPLE_FIELDS)
    _csv(root/SUMMARY,summary,SUM_FIELDS)
    _csv(root/PAIR,pairs,PAIR_FIELDS)
    _csv(root/PLOT,plots,PLOT_FIELDS)
    report={'schema_version':SCHEMA,'run_group_id':group.get('run_group_id'),
      'strict_group_performance_comparison_valid':group.get('performance_comparison_valid') is True,
      'sample_count_all':len(rows),'individual_paper_samples':len(accepted),'non_paper_or_unproven_samples':len(notpaper),
      'methods_with_validated_samples':len(summary),'cross_method_digest_pair_count':len(pairs),
      'different_v17_digest_pair_count':sum(x['evidence_status']=='different_v17_projection' for x in pairs),
      'source_contract':'original persisted child truth flags, no retroactive recertification',
      'paper_scope':'independent_method_measurements_not_strict_single_variable_speedup',
      'causal_limit':'Aggregate digests cannot establish legal serial order or pinpoint differing keys; raw attested archive required',
      'old_exports_unchanged':True,'official_paper_candidate_unchanged':True,
      'output_files':[SAMPLES,OBSERVED,SUMMARY,PAIR,PLOT,META]}
    _json(root/META,report)
    return report
