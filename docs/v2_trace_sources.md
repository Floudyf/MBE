# V2.3 Trace Sources

## Goal

V2.3 adds a unified trace source layer for local experiment preview and validation. The layer describes trace source capabilities, limitations, data truth labels, and validation rules before a run is attempted.

V2.3 does not run new experiment engines. It does not implement a multi-chain trace schema, dual-chain replay, cross-chain protocol replay, MetaFlow, committee bridge, Pending Pool, public-chain live ingestion, archive node access, Docker/Fabric startup, or `network.sh` automation.

## Trace Source Types

```text
synthetic
existing_trace
fabric_chain_backed_trace
public_chain_imported_trace
```

`synthetic` represents local workload generation followed by Go executor replay. Synthetic replay is not real on-chain execution.

`existing_trace` reuses a trace file already present inside the workspace. It does not start a chain and does not automatically guarantee access-set, delta, or commutative-update semantics.

`fabric_chain_backed_trace` reuses `.cache/fabric_smoke/latest/trace.jsonl.gz`. The trace source is a real Fabric smoke run, but the web backend only checks and replays files. It does not start Docker, Fabric, or `network.sh`.

`public_chain_imported_trace` is a V2.3 skeleton for imported public-chain data. It is externally realistic only when a file is provided, but the default semantic label is `semantic_unknown`.

## Data Truth Labels

```text
synthetic_replay
existing_trace_replay
fabric_chain_backed_trace_replay
public_chain_imported_trace_semantic_unknown
```

These labels must be shown separately from runnable/planned status. They describe where data came from and what the platform can honestly claim about it.

## Capabilities And Limitations

Capabilities are declared in `configs/trace_sources/v2_trace_sources.yaml`. Public-chain imported traces intentionally provide only:

```text
tx_id
timestamp
chain_id
status
raw_event
```

They do not default to:

```text
access_list
read_set
write_set
commutative
update_type
delta_semantics
```

## Existing Trace Validation

Existing trace validation resolves the requested path inside the repository workspace, rejects path escape, checks file existence, checks whether `trace_meta.json` is adjacent, and records file size. It does not load the whole trace into memory.

## Fabric Chain-backed Trace Validation

Fabric chain-backed validation only checks:

```text
.cache/fabric_smoke/latest/trace.jsonl.gz
.cache/fabric_smoke/latest/trace_meta.json
```

When missing, it returns the CLI command that can be run outside the web API. The web backend never starts Docker, Fabric, `network.sh`, deployCC, or peer invoke.

## Public-chain Imported Trace Rule

Public-chain imported trace validation never connects to a public-chain live node and never requires an archive node. It returns `public_chain_imported_trace_semantic_unknown` and warnings that access-list, read/write set, commutative update, update type, and delta semantics are not guaranteed.

## Relationship To Later Stages

V2.4 may define a formal multi-chain trace schema. V2.5 may implement dual-chain replay. V2.6 may implement cross-chain protocol baselines. V2.3 only creates the trace source description and validation layer those stages can build on later.
