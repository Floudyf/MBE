# V4.3.3 Experiment Design and Run Suite Alignment

## A. Current Problem

V4.3.2 added the experiment-flow API with Profile, Topology, Workload, and RunPlan preview models. That layer is useful, but the frontend still exposes two configuration sources:

- `V3ComposerPage` contains the full 11-module Composer and Formal benchmark configuration.
- The experiment-flow RunPlan selection and V4 realism recommendation act like a second lightweight configuration path.

This is not the desired experiment logic. The 11-module Composer / Formal benchmark configuration is the complete experiment design source. The V4 realism page should not become a second place to configure nodes, shards, tx count, and fault profile as if it were independent from the current experiment design.

Formal performance experiments must continue to run through the existing `FormalMetatrackExperimentPanel` payload and existing backend semantics. V4 realism validation should be derived from the current Experiment Plan, not re-entered by the user as a separate design.

## B. Correct Frontend Flow

The frontend should be organized by experiment lifecycle:

1. Experiment Design
2. Run Experiment
3. Results and Artifacts
4. Workload Library
5. Truth Boundaries
6. Advanced Features

Experiment Design owns the module configuration. Run Experiment only selects the run suite, matrix dimensions, seeds, and validation type derived from the current plan.

## C. 11-Module Single Experiment Design Source

The canonical blockchain experiment pipeline is:

1. Workload
2. TxPool
3. BlockProducer
4. Consensus
5. CommitteeEpoch
6. Routing
7. Execution
8. StateAccess
9. StateStorage
10. Commit
11. MetricsReport

An Experiment Plan is:

- 11-module configuration
- method, baseline, and ablation configuration
- workload configuration
- topology configuration
- seed, matrix, and metrics configuration

Profile, Topology, Workload, and RunPlan catalogs remain useful as defaults and compatibility previews, but they are not the full experiment design source.

## D. Run Experiment Page Responsibilities

The Run Experiment page does not reconfigure modules. It derives a run matrix from the current Experiment Plan and lets the user choose run types:

- `quick_validation`
- `main_experiment`
- `comparison_experiment`
- `ablation_experiment`
- `workload_sensitivity`
- `topology_scaling`
- `v4_realism_validation`

This page previews the matrix and can derive a V4 realism request. It must not replace the existing formal runner in this round.

## E. Backend Target

The experiment-flow backend keeps the static catalog from V4.3.2 and adds a preview layer:

- `ExperimentPlan`
- `ExperimentMethod`
- `ExperimentSuite`
- `ExperimentMatrix`
- `DerivedRunRequest`

The first implementation remains static and deterministic. It previews suites, matrix rows, runnable/blocked state, warnings, and a derived `V4RealismSmokeRequest` compatible with `/api/v4/realism/smoke`.

## F. Round Boundary

V4.3.3 is preview / derive / UI alignment only.

Allowed:

- docs and skill updates
- experiment-flow models, service functions, API routes, and tests
- frontend navigation alignment
- a Run Experiment preview page
- V4 realism page presentation changes

Forbidden:

- modifying `executor/`
- modifying Go runtime logic
- changing `go run ./cmd/mbe-supervisor` semantics
- changing `/api/v4/realism/*` paths
- replacing the formal benchmark runner
- unifying run registry
- making production or non-claim fields true
- automatic commit or push
