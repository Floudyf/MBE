# Fabric chain-backed trace skeleton (V1.4-a)

V1.4 will use the official `fabric-samples/test-network` for future local validation. This repository neither copies `fabric-samples` nor maintains a Fabric network.

V1.4-a is offline only: `raw_chain_log.jsonl`, `access_schema.yaml`, and the converter produce the MBE trace format. V1.4-b adds source-only Asset, Scene, and Reward Go chaincode aligned with the schema. This repository does not start Fabric, deploy chaincode, or invoke peers. V1.4-c will add a test-network wrapper, V1.4-d a small real runner, and V1.4-e the replay smoke. Fabric smoke is not default CI; a real chain only supplies chain-backed traces while mechanism comparisons remain in the MBE executor.

`raw_chain_log.jsonl` has one JSON object per transaction with `tx_id`, `tx_type`, `submit_time`, `commit_time`, `status`, `contract`, `function`, `args`, `block_number`, `event`, and `chain_latency_ms`. Numeric times are milliseconds; ISO times are converted to epoch milliseconds by the converter.
