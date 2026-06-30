# V3.5.1 Logical Node Topology Runtime

## 1. Scope

This stage only implements logical node topology runtime. It does not implement real TCP, multi-process launch, or real PBFT.

## 2. Implemented Capabilities

- frontend topology config
- backend topology validation
- logical node generation
- in-memory message bus / logical message logs
- node_topology.csv
- node_log.csv
- network_log.csv
- consensus_message_log.csv
- summary node metrics

## 3. Topology Fields

- shard_count
- validators_per_shard
- executors_per_shard
- storage_nodes_per_shard
- supervisor_enabled
- node_runtime_mode
- network_mode

## 4. Default Topology

```text
shard_count = 4
validators_per_shard = 4
executors_per_shard = 1
storage_nodes_per_shard = 1
supervisor_enabled = true
node_runtime_mode = logical_single_process
network_mode = in_memory_message_bus
```

## 5. Runtime Truth

This is a single-process logical node topology runtime. It is not real TCP, not multi-process, not real PBFT, and not a BlockEmulator backend.

## 6. Artifacts

- node_topology.csv
- node_log.csv
- network_log.csv
- consensus_message_log.csv

## 7. Summary Metrics

- shard_count
- validators_per_shard
- logical_node_count
- validator_node_count
- executor_node_count
- storage_node_count
- supervisor_node_count
- message_count
- network_message_count
- consensus_message_count
- node_event_count

## 8. Validation

V3.5.1 validation commands:

```powershell
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest backend/tests -q
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe -m pytest tests -q
cd executor
go test ./...
cd ..
cd frontend
npm.cmd run build
cd ..
C:\Users\飛\AppData\Local\Programs\Python\Python312\python.exe scripts/v0_sanity.py
git diff --check
git status --short
```

Recorded V3.5.1 validation results:

- `pytest backend/tests -q`: passed, 275 passed, 1 Starlette/httpx2 deprecation warning.
- `pytest tests -q`: passed, 24 passed.
- `go test ./...`: passed after permissioned rerun because the sandbox could not write the Go build cache.
- `npm.cmd run build`: passed after permissioned rerun because the sandbox could not create `frontend/dist/assets`; Vite reported the existing CJS Node API deprecation warning.
- `scripts/v0_sanity.py`: passed.
- `git diff --check`: passed with only line-ending conversion warnings.
- `git status --short`: expected V3.5.1 modified files before commit.

## 9. Next Step

V3.5.2 Local Multi-process Launcher Preview.
