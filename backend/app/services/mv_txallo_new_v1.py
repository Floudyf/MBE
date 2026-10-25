"""Evidence-bound TxAllo input bridge for FW V4/Role V2 and Henesys V4/Call V2.

The evaluation's exact *preceding* canonical records (never evaluation rows)
feed G; identical materialized rows and observed event/chain clocks feed A.
No account, transaction, ABI, block, or access semantics are fabricated.
"""
from __future__ import annotations

import gzip
import hashlib
import json
import math
from pathlib import Path
from typing import Any

FW = frozenset({'farmersworld_20260908_v4', 'fw_20260908_roles_v2'})
HENE = frozenset({'henesys_20250612_v4','henesys_20250824_v4',
                  'hene_20250612_calls_v2','hene_20250824_calls_v2'})
DATASETS = FW | HENE


def _sha(path: Path) -> str:
    d = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1<<20),b''):
            d.update(chunk)
    return d.hexdigest()


def _checked(cache_root: Path, relative: Any, expected: Any) -> Path:
    rel = Path(str(relative or ''))
    root = cache_root.resolve()
    if not str(relative or '') or rel.is_absolute() or '..' in rel.parts:
        raise ValueError('TxAllo new-source path is not a safe cache-relative path')
    p = (root / rel).resolve()
    if not p.is_relative_to(root) or not p.is_file():
        raise ValueError('TxAllo new-source canonical/materialized file missing')
    if len(str(expected or '')) != 64 or _sha(p) != str(expected).lower():
        raise ValueError('TxAllo new-source verified-cache SHA256 mismatch')
    return p


def _records(path: Path, start: int, count: int):
    found = 0
    with gzip.open(path,'rt',encoding='utf-8') as f:
        for i,line in enumerate(f):
            if i < start:continue
            if i >= start+count:break
            found += 1
            yield json.loads(line)
    if found != count:
        raise ValueError('TxAllo new-source selected canonical window truncated')


def _write(path: Path, records: list[dict[str,Any]]) -> str:
    # Sidecars live under the per-run directory, never the dataset or canonical cache.
    from backend.app.services.mv_txallo_input_v1 import _write_gzip
    return _write_gzip(path, records)


def _accounts(row: dict[str,Any]) -> tuple[str,str]:
    """Only actually observed accounts. A missing receiver is not fabricated.

    TxAllo's frozen graph supports a one-account transaction as a self-loop;
    a receiver-less WAX action / EVM creation is such a singleton when an
    observed sender exists. The receiver field remains empty in every sidecar.
    """
    sender = str(row.get('sender_id') or '').strip().lower()
    receiver = str(row.get('receiver_id') or '').strip().lower()
    if not sender:
        raise ValueError('TxAllo requires an observed sender account; cannot invent missing sender')
    return sender,receiver


def _block(row: dict[str,Any]) -> int:
    # Henesys source-projected metadata derives directly from actual source chain block number.
    meta = row.get('metadata') or {}
    value = meta.get('source_block_number')
    if isinstance(value,bool) or not isinstance(value,int) or value<=0 or value>2**63-1:
        raise ValueError('Henesys TxAllo requires an actual positive source_block_number, not a synthetic block')
    return value


def compile_new_txallo_inputs(*,run_dir:Path, manifest:dict[str,Any], workload_plan:dict[str,Any],
                               profile:dict[str,dict[str,Any]], cache_root:Path, project_root:Path):
    if (profile.get('sharding') or {}).get('plugin_id')!='txallo_account_sharding':return None
    dataset = str(workload_plan.get('dataset_id') or '')
    if dataset not in DATASETS:return None
    cfg = (profile.get('sharding') or {}).get('config') or {}
    if not bool(cfg.get('dynamic_a_txallo_runtime')):
        raise ValueError('FW/Henesys TxAllo adapter requires documented dynamic G/A enabled')
    mode = str(workload_plan.get('variant_mode') or '')
    selection = str(workload_plan.get('selection_mode') or '')
    if selection != 'contiguous_window' or mode not in {'original_window','historical_window'}:
        raise ValueError('TxAllo requires an ORIGINAL contiguous evaluation window preceded by observed history. Henesys validated_prefix starting at row 0 has no prehistory; select historical_window for EVERY compared method.')
    if dataset in HENE and mode != 'historical_window':
        raise ValueError('Henesys TxAllo requires explicitly selected historical_window for all methods; original validated prefix lacks preceding history')
    start = int(workload_plan.get('start_offset') or 0)
    count = int(workload_plan.get('actual_tx_count') or 0)
    ratio = float(cfg.get('history_ratio',0.1))
    if count <= 0 or start <= 0 or not 0 < ratio <= 1:
        raise ValueError('TxAllo needs positive evaluation count and genuine strictly preceding historical rows; never train on evaluation rows')
    if int(cfg.get('a_epoch_blocks') or 15)!=15 or int(cfg.get('g_epoch_multiple') or 20)!=20:
        raise ValueError('TxAllo preserved G/A cadence requires a_epoch_blocks=15 and g_epoch_multiple=20')
    croot = Path(cache_root)
    canonical = _checked(croot,workload_plan.get('canonical_relative_path'),workload_plan.get('canonical_sha256'))
    materialized = _checked(croot,workload_plan.get('materialized_relative_path'),workload_plan.get('materialized_sha256'))
    hist_count = min(start,max(1,math.ceil(count*ratio)))
    before = list(_records(canonical,start-hist_count,hist_count))
    current = list(_records(materialized,0,count))
    if not before or int(before[-1].get('source_row_index',-1)) != start-1:
        raise ValueError('TxAllo prehistory not immediately before evaluation')
    if not current or int(current[0].get('source_row_index',-1)) != start:
        raise ValueError('TxAllo evaluation start/source order mismatch')
    history=[]
    history_single_account_count=0
    evaluation_single_account_count=0
    for i,row in enumerate(before):
        order = int(row.get('source_row_index',-1))
        if order != start-hist_count+i:raise ValueError('TxAllo historical source order mismatch')
        sender,receiver = _accounts(row)
        history_single_account_count += int(not receiver)
        history.append({'schema_version':'mbe_txallo_history_record_v1','source_order':order,
             'transaction_id':str(row.get('source_event_id') or ''),'timestamp':str(row.get('timestamp_ms') or ''),
             'block_num':_block(row) if dataset in HENE else 0,
             'global_sequence':order+1, # Replay index, NOT native global sequence.
             'sender_id':sender,'receiver_id':receiver,'accounts':sorted({a for a in (sender,receiver) if a})})
    dynamic=[]
    prev_block=-1
    for index,row in enumerate(current):
        order = int(row.get('source_row_index',-1))
        if order != start+index or row.get('occurrence_index') not in (None,0):
            raise ValueError('TxAllo evaluation is not original contiguous unique source rows')
        sender,receiver=_accounts(row)
        evaluation_single_account_count += int(not receiver)
        txid = str(row.get('logical_event_id') or row.get('source_event_id') or '')
        if not txid:raise ValueError('TxAllo source logical transaction ID missing')
        if dataset in HENE:
            block = _block(row)
            if block < prev_block:raise ValueError('Henesys observed source block regressed')
            prev_block=block
            dyn={'schema_version':'mbe_txallo_dynamic_block_v1','block_num':block}
        else:
            dyn={'schema_version':'mbe_txallo_event_clock_v1','block_num':0,'epoch_coordinate':order+1}
        dynamic.append({**dyn,'materialized_index':index,'raw_source_row_index':order,
             'transaction_id':txid,'global_sequence':order+1,'sender_id':sender,'receiver_id':receiver})
    if dataset in HENE and history[-1]['block_num'] > dynamic[0]['block_num']:
        raise ValueError('Henesys history block exceeds evaluation first block')
    history_rel='workload/txallo_history.jsonl.gz'
    dynamic_rel='workload/txallo_dynamic_blocks.jsonl.gz'
    hs=_write(run_dir/history_rel,history)
    ds=_write(run_dir/dynamic_rel,dynamic)
    is_event=dataset in FW
    hsum={'schema_version':'mbe_txallo_history_selection_v1','selection_policy':'preceding_ratio_v1',
       'history_ratio':ratio,'evaluation_transaction_count':count,'requested_history_count':max(1,math.ceil(count*ratio)),
       'selected_history_count':hist_count,'history_pool_count':hist_count,'history_pool_sha256':hs,
       'selected_history_sha256':hs,'history_relative_path':history_rel,
       'history_window_start_source_order':start-hist_count,'history_window_end_source_order':start-1,
       'evaluation_anchor_raw_row_index':start,'accounts_semantics':'observed_sender_optional_observed_receiver_v2',
       'source_single_account_history_count':history_single_account_count,
       'future_evaluation_transactions_used':0,'g_cache_dir':str((project_root/'.cache'/'txallo_g').resolve()),
       'source_block_identity':'not_observed' if is_event else 'henesys_observed_source_block_number',
       'sequence_identity':'source_row_index_plus_one_not_native_sequence',
       'source_canonical_sha256':workload_plan.get('canonical_sha256'),
       'source_evaluation_sha256':workload_plan.get('materialized_sha256'),
       'paper_note':'10% is MBE pre-evaluation policy, not paper universal. No evaluation overlap.'}
    dsum={'schema_version':'mbe_txallo_dynamic_block_selection_v1',
       'row_schema_version':dynamic[0]['schema_version'],'relative_path':dynamic_rel,'sha256':ds,
       'selected_count':count,'evaluation_anchor_raw_row_index':start,
       'allow_sender_only_account_records':True,
       'accounts_semantics':'observed_sender_optional_observed_receiver_v2',
       'source_single_account_evaluation_count':evaluation_single_account_count,
       'evaluation_anchor_transaction_id':dynamic[0]['transaction_id'],
       'first_block_num':None if is_event else dynamic[0]['block_num'],
       'last_block_num':None if is_event else dynamic[-1]['block_num'],
       'first_global_sequence':dynamic[0]['global_sequence'],'last_global_sequence':dynamic[-1]['global_sequence'],
       'a_epoch_blocks':15,'paper_reference_a_epoch_blocks':300,'g_epoch_multiple':20,
       'source_epoch_count_spanned':((dynamic[-1].get('epoch_coordinate',dynamic[-1]['block_num'])-
                    dynamic[0].get('epoch_coordinate',dynamic[0]['block_num']))//15+1),
       'selection_semantics':'verified_preceding_history_plus_exact_contiguous_original_evaluation',
       'sequence_identity_source':'source_row_index_plus_one_replay_not_native_sequence',
       'future_evaluation_transactions_used_for_prior_epoch':0,
       'source_raw_relative_path':str(manifest.get('local_raw_relative_path') or ''),
       'paper_note':'Preserved TxAllo G/A core; MBE epoch cadence 15 and G multiple 20.'}
    if is_event:
        dsum.update({'first_epoch_coordinate':dynamic[0]['epoch_coordinate'],
            'last_epoch_coordinate':dynamic[-1]['epoch_coordinate'],
            'epoch_clock_source':'mbe_event_order_index','a_epoch_unit':'source_events_not_chain_blocks',
            'a_epoch_parameterization':'mbe_adapted_fixed_15_observed_events',
            'block_identity_source':'not_observed_no_chain_blocks_invented',
            'paper_fidelity_scope':'MBE_EVENT_ORDER_ADAPTATION_NOT_PAPER_SOURCE_BLOCK_REPRODUCTION'})
    else:
        dsum.update({'a_epoch_parameterization':'mbe_adapted_fixed_15_source_blocks',
            'block_identity_source':'henesys_observed_source_block_number',
            'paper_fidelity_scope':'MBE_HENESYS_SOURCE_BLOCK_ADAPTATION'})
    out=run_dir/'workload';out.mkdir(parents=True,exist_ok=True)
    (out/'txallo_history_summary.json').write_text(json.dumps(hsum,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    (out/'txallo_dynamic_blocks_summary.json').write_text(json.dumps(dsum,ensure_ascii=False,indent=2,sort_keys=True)+'\n',encoding='utf-8')
    return hsum,dsum
