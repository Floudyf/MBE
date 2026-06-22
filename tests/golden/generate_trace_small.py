"""Generate the small streaming trace fixture without a workload generator."""
from pathlib import Path
import sys
ROOT = Path(__file__).resolve().parents[2]; sys.path.insert(0, str(ROOT))
from trace.writer import write_trace, write_trace_meta

records = (
    {"tx_id":"tx-001","tx_type":"asset_transfer","timestamp":0,"chain_id":"mockchain-0","contract":"asset","function":"transfer","args":{"asset_id":"asset-1"},"read_set":["asset:asset-1"],"write_set":["asset:asset-1","owner:user-2"],"access_list":["asset:asset-1","owner:user-2"],"commutative":False,"update_type":"replace","status":"success","chain_latency_ms":1.0},
    {"tx_id":"tx-002","tx_type":"reward_claim","timestamp":1,"chain_id":"mockchain-0","contract":"reward","function":"claim","args":{"pool_id":"pool-1"},"read_set":["reward_pool:pool-1"],"write_set":["balance:user-2"],"access_list":["reward_pool:pool-1","balance:user-2"],"commutative":True,"update_type":"delta","status":"success","chain_latency_ms":1.5},
)
OUT = Path(__file__).parent
write_trace(records, OUT / "trace_small.jsonl.gz")
write_trace_meta({"tx_count":2,"actual_tx_mix":{"asset_transfer":0.5,"reward_claim":0.5},"actual_hot_key_ratio":0.0,"actual_cross_shard_ratio":0.0,"avg_read_set_size":1.0,"avg_write_set_size":1.5,"seed":42,"trace_format":"jsonl","compression":"gzip","schema_version":"v0"}, OUT / "trace_meta.json")
