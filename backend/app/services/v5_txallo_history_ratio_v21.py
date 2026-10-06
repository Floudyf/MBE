from __future__ import annotations
import gzip, hashlib, json, math, os
from collections import deque
from pathlib import Path
from typing import Any
from backend.app.core.paths import ROOT

SCHEMA="mbe_txallo_history_selection_v1"
POOL_SCHEMA="mbe_txallo_history_pool_v1"
POLICY="preceding_ratio_v1"
DEFAULT_RATIO=0.10

def _sha256(path: Path)->str:
    h=hashlib.sha256()
    with path.open("rb") as f:
        for b in iter(lambda:f.read(1024*1024),b""): h.update(b)
    return h.hexdigest()

def _write_gz(path:Path,rows:list[dict[str,Any]])->None:
    path.parent.mkdir(parents=True,exist_ok=True)
    tmp=path.with_name("."+path.name+f".{os.getpid()}.tmp")
    with tmp.open("wb") as raw:
        with gzip.GzipFile(filename="",mode="wb",fileobj=raw,compresslevel=9,mtime=0) as out:
            for row in rows:
                out.write((json.dumps(row,ensure_ascii=False,separators=(",",":"),sort_keys=True)+"\n").encode())
        raw.flush(); os.fsync(raw.fileno())
    os.replace(tmp,path)

def compile_txallo_history(*,run_dir:Path,manifest:dict[str,Any],workload_plan:dict[str,Any],profile:dict[str,dict[str,Any]])->dict[str,Any]|None:
    sharding=profile.get("sharding") or {}
    if sharding.get("plugin_id")!="txallo_account_sharding": return None
    pool=manifest.get("txallo_history_pool")
    if not isinstance(pool,dict): raise ValueError("TxAllo requires manifest.txallo_history_pool")
    if pool.get("schema_version")!=POOL_SCHEMA or pool.get("selection_policy")!=POLICY:
        raise ValueError("unsupported TxAllo history pool contract")
    ratio=float((sharding.get("config") or {}).get("history_ratio",pool.get("history_ratio",DEFAULT_RATIO)))
    if not (0.0<ratio<=1.0): raise ValueError("TxAllo history_ratio must be in (0,1]")
    evaluation_count=int(workload_plan.get("actual_tx_count") or workload_plan.get("tx_count") or 0)
    pool_count=int(pool.get("row_count") or 0)
    if evaluation_count<=0 or pool_count<=0: raise ValueError("TxAllo requires positive evaluation/history counts")
    requested=max(1,int(math.ceil(evaluation_count*ratio)))
    selected_count=min(requested,pool_count)
    pool_path=ROOT/Path(str(pool.get("local_relative_path") or ""))
    if not pool_path.is_file(): raise ValueError("TxAllo history pool missing")
    expected=str(pool.get("sha256") or "").lower()
    actual=_sha256(pool_path)
    if len(expected)!=64 or actual!=expected: raise ValueError("TxAllo history pool SHA-256 mismatch")
    selected:deque[dict[str,Any]]=deque(maxlen=selected_count)
    seen=0
    with gzip.open(pool_path,"rt",encoding="utf-8",newline="") as h:
        for line_no,line in enumerate(h,1):
            line=line.strip()
            if not line: continue
            row=json.loads(line)
            if row.get("schema_version")!="mbe_txallo_history_record_v1": raise ValueError(f"history row {line_no}: bad schema")
            accounts=row.get("accounts")
            if not isinstance(accounts,list) or not any(str(x).strip() for x in accounts): raise ValueError(f"history row {line_no}: no accounts")
            selected.append(row); seen+=1
    if seen!=pool_count or len(selected)!=selected_count: raise ValueError("TxAllo history pool count mismatch")
    rows=list(selected)
    start_order=int(rows[0]["source_order"]); end_order=int(rows[-1]["source_order"])
    anchor=int(pool.get("evaluation_anchor_raw_row_index") or pool_count)
    if end_order!=anchor-1: raise ValueError("selected history does not end immediately before evaluation anchor")
    out_rel=Path("workload")/"txallo_history.jsonl.gz"
    out=run_dir/out_rel
    _write_gz(out,rows)
    sel_sha=_sha256(out)
    summary={
        "schema_version":SCHEMA,"selection_policy":POLICY,"history_ratio":ratio,
        "evaluation_transaction_count":evaluation_count,"requested_history_count":requested,
        "selected_history_count":selected_count,"history_pool_count":pool_count,
        "history_pool_sha256":actual,"selected_history_sha256":sel_sha,
        "history_relative_path":out_rel.as_posix(),"history_window_start_source_order":start_order,
        "history_window_end_source_order":end_order,"evaluation_anchor_raw_row_index":anchor,
        "accounts_semantics":"sender_receiver_accounts_only","future_evaluation_transactions_used":0,
        "paper_note":"history_ratio=0.10 is an MBE experiment policy, not a TxAllo paper invariant",
        "g_cache_dir":str((ROOT/".cache"/"txallo_g").resolve()),
    }
    sp=run_dir/"workload"/"txallo_history_summary.json"
    sp.write_text(json.dumps(summary,ensure_ascii=False,sort_keys=True,indent=2)+"\n",encoding="utf-8")
    return summary
