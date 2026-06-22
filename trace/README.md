# Trace

Formal V0 traces are `trace.jsonl.gz` and must never be fully loaded into memory. `writer/gzip_jsonl_writer.py` consumes records one by one; `writer/meta_writer.py` writes required metadata; `schema/tx_trace.schema.json` defines each single-chain record. Plain `.jsonl` remains available only for small readable debugging fixtures.
