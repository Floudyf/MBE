# Fabric chain-backed trace skeleton (V1.4-a)

V1.4 will use the official `fabric-samples/test-network` for future local validation. This repository neither copies `fabric-samples` nor maintains a Fabric network.

V1.4-a is offline only: `raw_chain_log.jsonl`, `access_schema.yaml`, and the converter produce the MBE trace format. V1.4-b adds Asset, Scene, and Reward Go chaincode aligned with the schema. V1.4-c provides a test-network wrapper. V1.4-d adds the opt-in local runner: `python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest`. It uses an external official `fabric-samples/test-network`, produces a small real chain-backed trace, and cleans the network by default. It is not a production Fabric network, formal multi-chain/cross-chain system, or replacement for the MBE executor; it only supplies real-chain input traces.

`raw_chain_log.jsonl` has one JSON object per transaction with `tx_id`, `tx_type`, `submit_time`, `commit_time`, `status`, `contract`, `function`, `args`, `block_number`, `event`, and `chain_latency_ms`. Numeric times are milliseconds; ISO times are converted to epoch milliseconds by the converter.
