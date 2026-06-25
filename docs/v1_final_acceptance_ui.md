# V1-final acceptance UI

V1-final is the frontend/backend acceptance integration for the completed V1
single-chain paper-experiment path. It does not introduce a new core mechanism
and it is not V1.9.

The goal is to make the web UI reflect the implemented V1.4-V1.8 work instead
of leaving those capabilities as static "planned" text.

## What the UI exposes

- V0 default experiment remains runnable from the same web entry.
- V1.1-V1.8 stage status is available from `GET /api/v1/status`.
- V1.8 baseline sweep can be launched from `POST /api/v1/sweep/run`.
- V1.8 results can be read from:
  - `GET /api/v1/sweep/summary`
  - `GET /api/v1/sweep/report`
  - `GET /api/v1/sweep/files`
- The frontend groups V1.8 summary fields into base, routing, dual-track, and
  aggregation metric sections.

The V1.8 sweep writes local replay artifacts under:

```text
.cache/v1_8_sweeps/latest/
```

Those artifacts are ignored by Git and are not part of source control.

## Boundaries

V1-final is still a single-chain runnable prototype. It does not implement:

- formal dual-chain or multi-chain execution;
- cross-chain protocols;
- MetaFlow;
- committee bridge;
- Pending Pool;
- production Fabric;
- multi-server Fabric.

Fabric chain-backed trace validation remains a CLI/WSL smoke-runner entry. The
web UI intentionally does not start Docker, Fabric, `network.sh`, `deployCC`, or
`peer invoke`.

Recommended Fabric smoke command, when the WSL2 + Docker Desktop +
fabric-samples environment is already prepared:

```bash
python scripts/v1_fabric_smoke.py --strict --channel mbechannel --out .cache/fabric_smoke/latest
```

## Recommended web acceptance flow

1. Start the backend and frontend with the existing development scripts.
2. Run the V0 default experiment to confirm V0 remains intact.
3. Review the V1.1-V1.8 status cards.
4. Click "运行 V1.8 baseline sweep".
5. Inspect the four baseline/ablation rows:
   - `baseline_hash_only`
   - `co_access_only`
   - `co_access_dual_track`
   - `full_v1`
6. Download or view:
   - `report.md`
   - `sweep_summary.csv`
   - `sweep_summary.json`

