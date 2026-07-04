# V3.12 Runtime Realism Closure

## 1. Goal

V3.12 closes the runtime realism stage by hardening the V3.5 logical topology, launcher preview, and node process preview into a small local multi-process runtime MVP.

It adds:

- `local_multi_process` node runtime mode.
- managed local process plan and short smoke lifecycle.
- process lifecycle and status artifacts.
- localhost TCP / NetworkAdapter process path records.
- shard assignment, committee assignment, epoch log, and light reconfiguration artifacts.

## 2. Relationship To V3.5

V3.5 introduced the logical topology, launcher preview, and node process preview. V3.12 does not replace that topology. It uses the same logical topology as the source of deterministic local process plans and hardens the preview into runnable/observable/cleanable local-machine behavior.

`logical_single_process` remains the default compatibility path.

## 3. local_multi_process Runtime Mode

`node_runtime_mode` now supports:

- `logical_single_process`: default single-process logical node mode.
- `local_multi_process`: local-machine multi-process MVP for small controlled emulator runs.

This mode is local-machine only. It is not multi-server deployment and not production networking.

## 4. dry_run / smoke

`process_runtime_mode` supports:

- `dry_run`: writes address table, process manifest, launch/status/lifecycle plans, and deterministic network message records without starting OS processes.
- `smoke`: starts short-lived local node smoke processes, captures stdout/stderr/status, and stops safely.

`dry_run` is the stable CI/default mode.

## 5. max_local_processes Guard

`max_local_processes` defaults to 8 and is validated in the range 1..32. If topology-derived process count exceeds the limit, V3.12 enters capped mode and records `capped_process_count` in summary and manifest.

## 6. NetworkAdapter Process Path

V3.12 writes `network_message_log.csv` for local process message path observability. The path truth is:

```text
localhost_tcp_preview_not_production_network
```

The records are deterministic preview/MVP records and do not claim production networking.

## 7. Shard / Committee / Epoch MVP

When `enable_committee_epoch=true`, V3.12 writes deterministic:

- shard assignment.
- committee assignment.
- epoch log.
- light reconfiguration plan.

`epoch_count=1` writes a no-op reconfiguration summary. `epoch_count>1` writes a simple deterministic round-robin reconfiguration plan. This is not secure random resharding and not production committee lifecycle.

## 8. Artifacts

Local multi-process runtime artifacts:

- `address_table.json`
- `multi_process_manifest.json`
- `node_process_log.csv`
- `node_lifecycle_log.csv`
- `network_message_log.csv`
- `node_process_status.json`
- `local_multi_process_summary.json`
- `node_stdout.log` and `node_stderr.log` in smoke mode

Committee / epoch artifacts:

- `shard_assignment_log.csv`
- `committee_assignment_log.csv`
- `committee_summary.json`
- `epoch_log.csv`
- `reconfiguration_plan.json`
- `reshard_plan_log.csv`
- `reconfiguration_summary.json`

## 9. Summary Metrics

V3.12 adds summary metrics for node/process mode, planned/started/stopped/failed/capped process counts, max local processes, network message count, network path truth, shard/committee/epoch counts, reconfiguration event count, committee epoch enablement/truth, and runtime realism truth.

Truth labels:

```text
runtime_realism_truth = local_multi_process_runtime_mvp_not_production_cluster
network_path_truth = localhost_tcp_preview_not_production_network
committee_epoch_truth = committee_epoch_mvp_not_secure_reconfiguration
```

## 10. Frontend Changes

The V3 Composer topology panel adds controls for:

- node runtime mode.
- process runtime mode.
- max local processes.
- committee/epoch enablement.
- epoch count.

Result panels show process, network, shard, committee, epoch, and reconfiguration metrics. ArtifactGroups includes Local Multi-process Runtime artifacts and Committee / Epoch artifacts.

## 11. Truth Boundary

V3.12 implements a local multi-process runtime MVP for controlled local emulator experiments.

It is not multi-server deployment.
It is not a production cluster.
It is not production PBFT / HotStuff / Raft.
It is not BlockEmulator backend.
It is not Fabric/EVM live backend.
It does not prove paper-grade performance.

中文口径：

V3.12 实现的是本地小规模多进程 runtime MVP，用于增强 emulator 的节点生命周期、网络消息、分片、委员会和 epoch 可观测性。它不是多服务器部署，不是生产级集群，也不是生产级共识网络。

## 12. Validation Commands

```powershell
git diff --check

cd frontend
npm.cmd run build
cd ..

cd executor
go test ./...
cd ..

$env:PYTHONPATH = (Get-Location).Path
python -m pytest backend/tests -q
python -m pytest tests -q
python scripts/v0_sanity.py
```
