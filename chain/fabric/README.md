# Fabric chain-backed trace skeleton (V1.4-a)

V1.4 will use the official `fabric-samples/test-network` for future local validation. This repository neither copies `fabric-samples` nor maintains a Fabric network.

V1.4-a is offline only: `raw_chain_log.jsonl`, `access_schema.yaml`, and the converter produce the MBE trace format. Future V1.4-b/c/d work may perform network-up, channel creation, chaincode deployment, and invoke steps. Fabric smoke is not part of default CI, and any Fabric failure must not affect V0/V1 synthetic replay. A real chain only supplies chain-backed traces; mechanism comparisons remain in the MBE executor.

`raw_chain_log.jsonl` has one JSON object per transaction with `tx_id`, `tx_type`, `submit_time`, `commit_time`, `status`, `contract`, `function`, `args`, `block_number`, `event`, and `chain_latency_ms`. Numeric times are milliseconds; ISO times are converted to epoch milliseconds by the converter.
