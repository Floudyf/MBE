# Porygon Acceptance Matrix

## Focused Go gates

- all Porygon plugin IDs register through the generic V5 registry;
- transaction/access roots are bound to proposal evidence;
- validators recompute and verify the Porygon plan;
- Cross-Batch Witness logical stage exists when enabled;
- a multi-ESC transaction is classified cross-shard;
- cross-shard execution count is exactly one per cross-shard transaction;
- multi-shard update evidence covers every involved ESC;
- final state root and receipt root equal the serial oracle.

## Platform gates

- `stateless_porygon` is a built-in formal method;
- Porygon plugins must be selected as one coherent profile;
- PBFT, workload, network, metrics and observability remain shared MBE components;
- existing built-in method profiles are unchanged;
- frontend and backend expose the same method ID and plugin composition.

## Required local commands

```text
cd executor
go test ./v5 -run Porygon -count=1
go test ./...
go vet ./...

cd <repo-root>
python -m pytest backend/tests/test_v5_porygon_registry.py -q
```
