# V1.4-d local Fabric smoke runner

`python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest` runs a small, local chain-backed trace smoke against an external official `fabric-samples/test-network`. Set `FABRIC_SAMPLES_DIR` to that checkout first. The runner starts the local network, deploys the repository Asset, Scene, and Reward chaincodes, invokes a deterministic path, writes `raw_chain_log.jsonl`, then converts it to `trace.jsonl.gz` and `trace_meta.json`.

The output directory also contains `runtime.log` and `summary.json`. Bootstrap create/balance calls appear in `runtime.log`; the raw log contains only the five schema-backed trace operations. If peer CLI output lacks a stable transaction id or block number, the runner uses deterministic sequence fallbacks and records that limitation in `runtime.log`.

This is a small local trace source, not a production Fabric network, multi-chain/cross-chain experiment, or replacement for the MBE executor. It cleans the network with `cd $FABRIC_SAMPLES_DIR/test-network && ./network.sh down` unless `--keep-network` is supplied. Use `--dry-run` to print the network, deployment, and invocation plan without starting Docker or invoking Fabric.

The V1.4 chaincode modules declare `go 1.18` because this Fabric 2.5 test-network's vendoring helper supports module directives through that version. This is a chaincode packaging compatibility declaration; the V0 executor's Go 1.26.1 constraint is unchanged.
