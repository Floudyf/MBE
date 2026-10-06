from __future__ import annotations
import gzip, hashlib, json
from pathlib import Path
from backend.app.services import v5_txallo_history_ratio_v21 as mod

def _gz(path:Path,rows:list[dict])->str:
    with path.open("wb") as raw:
        with gzip.GzipFile(filename="",mode="wb",fileobj=raw,compresslevel=9,mtime=0) as out:
            for row in rows: out.write((json.dumps(row,sort_keys=True,separators=(",",":"))+"\n").encode())
    return hashlib.sha256(path.read_bytes()).hexdigest()

def test_latest_preanchor_ratio(monkeypatch,tmp_path:Path):
    rows=[{"schema_version":"mbe_txallo_history_record_v1","source_order":i,"transaction_id":f"tx{i}",
           "timestamp":"","block_num":i,"sender_id":f"a{i}","receiver_id":"m.federation",
           "accounts":[f"a{i}","m.federation"]} for i in range(10)]
    pool=tmp_path/"pool.gz"; sha=_gz(pool,rows); monkeypatch.setattr(mod,"ROOT",tmp_path)
    manifest={"txallo_history_pool":{"schema_version":"mbe_txallo_history_pool_v1","selection_policy":"preceding_ratio_v1",
        "history_ratio":0.10,"row_count":10,"sha256":sha,"local_relative_path":"pool.gz","evaluation_anchor_raw_row_index":10}}
    profile={"sharding":{"plugin_id":"txallo_account_sharding","config":{"history_ratio":0.10}}}
    result=mod.compile_txallo_history(run_dir=tmp_path/"run",manifest=manifest,workload_plan={"actual_tx_count":20},profile=profile)
    assert result["selected_history_count"]==2
    assert result["history_window_start_source_order"]==8
    assert result["history_window_end_source_order"]==9
    assert result["future_evaluation_transactions_used"]==0
