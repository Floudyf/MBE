# Aria Acceptance Matrix

## Focused Go tests

- independent transactions commit in one epoch;
- same-sender nonce chains advance through deterministic internal epochs;
- Rule 1 and Rule 2 dependency conditions match the locked source;
- reservation tables retain minimum TIDs;
- unresolvable future nonces fail without an infinite retry loop;
- worker counts 1, 2, 4, and 8 preserve logical results;
- repeated runs preserve the plan digest;
- the V5 registry creates and executes `aria_block_executor` through the generic
  block-executor contract.

## Platform tests

- the backend manifest locks the paper, official repository, and source commit;
- the built-in `hash_aria` method changes the block executor while preserving
  hash routing, FIFO outer scheduling, normal commit, workload, topology, and
  fault settings;
- formal comparison rows preserve the same fairness key and workload snapshot;
- frontend and backend method profiles expose the same Aria configuration.

## Required local commands

```text
cd executor
go test ./realism/execution/... ./v5
go test ./...

cd <repo-root>
python -m pytest backend/tests/test_v5_aria_plugin.py \
  backend/tests/test_v5_formal_plan_validator_closure.py \
  backend/tests/test_v5_formal_row_compilation_propagation.py \
  backend/tests/test_v5_plugin_catalog.py \
  backend/tests/test_v5_plugin_platform.py -q
```

The one-click installer runs these checks and restores its backup when a required
check fails.
