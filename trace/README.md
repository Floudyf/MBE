# Trace

Formal traces are `trace.jsonl.gz` and must never be fully loaded into memory. `writer/gzip_jsonl_writer.py` consumes records one by one; `writer/meta_writer.py` writes required metadata; `schema/tx_trace.schema.json` defines each single-chain record. V1.4-a adds the offline `converter/fabric_to_unified_trace.py` raw-log converter; it does not use a Fabric SDK or start a network. Plain `.jsonl` remains available only for small readable debugging fixtures.
