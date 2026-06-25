# V2.6 Cross-chain Protocol Baselines

V2.6 implements local cross-chain protocol baseline replay. It is a V3-ready replay layer, not a production bridge.

## Goal

V2.6 adds baseline protocol models above the V2.5 `ChainBackend` interface:

- `lock_mint_serial`
- `lock_mint_pipeline`
- `fixed_window_baseline`
- `committee_bridge_basic`

The baselines compare local protocol behavior, end-to-end latency, finality wait, timeout/refund counts, and pending counts.

## Baselines

`lock_mint_serial` advances one cross-chain transaction through source lock, source finality, certificate generation, target mint, target finality, and completion before starting the next transaction.

`lock_mint_pipeline` lets multiple cross-chain transactions advance through different stages at the same time. This is not MetaFlow and does not implement AFS, FDA, or Pending Pool.

`fixed_window_baseline` limits the number of concurrently unfinished cross-chain transactions with a fixed `window_size`. The window is not adaptive and is not FDA.

`committee_bridge_basic` adds a local `committee_delay_ms` after source finality before target mint. It does not implement real signatures, MintCert, RefundCert, FinalityProof, or a committee security model.

## Artifacts

A V2.6 run writes:

- `metadata.json`
- `used_config.yaml`
- `used_config.json`
- `protocol_summary.csv`
- `protocol_summary.json`
- `protocol_results.csv`
- `protocol_events.csv`
- `runtime.log`
- `report.md`

## API / CLI

API:

- `GET /api/v2/cross-chain/protocols`
- `GET /api/v2/cross-chain/sample-config`
- `POST /api/v2/cross-chain/protocol-replay`

CLI:

```powershell
python scripts/v2_6_cross_chain_protocol_replay.py --config configs/experiments/v2_cross_chain_protocol_sample.yaml --out .cache/v2_6_protocol/latest
```

The API stores results through the V2.2 job/artifact manager. The CLI writes the same artifact set to the requested output directory.

## Non-goals

V2.6 is local protocol baseline replay. V2.6 is not real chain execution, not Fabric execution, not public-chain replay, and not a production bridge.

V2.6 does not start Docker, Fabric, or network.sh. It does not connect to public-chain live nodes or archive nodes.

V2.6 does not implement MetaFlow, a real committee bridge, real signatures, real certificates, Pending Pool, V2.7 UI, V2.8 sweep/report, V2.9 realism bridge, or V3 live backends.

V2.7 may later expose these baselines in UI. V2.8 may add sweeps and reports. V2.9 may be a future realism bridge stage. V3 is reserved for production-like deployment and live backends.
