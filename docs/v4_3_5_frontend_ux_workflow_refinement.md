# V4.3.5 Frontend UX and Experiment Workflow Refinement

## A. Current Problems

The frontend still feels too dense for a productized experiment console. Multiple pages expose historical controls, compatibility settings, run feedback, and artifact lists in the same visual space, which makes the intended workflow harder to read.

The Experiment Design page still mixes method design with run conditions, compatibility catalog settings, execution feedback, and result views. Workload, node count, shard count, and seed are experiment conditions; they should not be treated as part of a reusable method template.

The Results and Artifacts page is still closer to a file list than an analysis center. Users need a single place to inspect run summaries, charts, log availability, individual downloads, and any future download-all entry.

The Run Experiment page already owns matrix preview and the V4.3.4 execution bridge, but it needs clearer experiment condition controls, execution feedback, and a lightweight stage visualization. This visualization must be honest: it can summarize stages from matrix status, child run status, summaries, or log counts, but it is not a live per-transaction stream.

## B. New Flow Definition

### Experiment Design Page

The Experiment Design page designs reusable method templates only.

It does not select workload, node count, shard count, or seed. It does not display formal experiment results as the main page content. It may provide quick validation for the current template and may save the template as a draft or runnable method template.

### Run Experiment Page

The Run Experiment page chooses experiment conditions and execution scope:

- main method template, comparison templates, and ablation templates;
- workload;
- nodes, shards, and validators per shard;
- transaction count, seed, and repeat count;
- suite type such as quick validation, main experiment, comparison, ablation, workload sensitivity, topology scaling, or V4 realism validation;
- matrix preview;
- dry-run or execute selected rows;
- lightweight run-stage and transaction-flow visualization.

### Results and Artifacts Page

The Results and Artifacts page is the result center:

- select `run_id` or `run_group_id`;
- show summary cards;
- show charts when summary data can support them;
- show log and artifact availability;
- keep single-file downloads;
- show download-all only when a real zip endpoint exists.

If no common zip endpoint exists, the UI must show a disabled download-all affordance with an explicit note that unified artifact downloads require a later run registry or artifact API. It must not fabricate unavailable links.

## C. Method Templates and Experiment Conditions

Method templates describe the blockchain mechanism pipeline. They include:

- template name;
- template role: `main`, `baseline`, `ablation`, or `custom`;
- TxPool;
- BlockProducer;
- Consensus;
- CommitteeEpoch;
- Routing;
- Execution;
- StateAccess;
- StateStorage;
- Commit;
- MetricsReport.

Experiment conditions describe how a method template is executed. They include:

- workload;
- nodes;
- shards;
- validators per shard;
- transaction count;
- seed;
- repeat count;
- runtime target.

The existing Composer may retain compatibility fields internally, including workload-related fields, but the product flow must make clear that workload and topology are selected on the Run Experiment page.

## D. Visualization Boundary

V4.3.5 may add stage statistics, status badges, chart placeholders, and a small CSS dot-flow animation. These views are derived from run summaries, log/artifact presence, matrix rows, or child run status.

This round must not implement WebSocket/SSE, real per-transaction event streaming, or a synthetic view that claims to represent live transaction-by-transaction behavior.

The visualization label should stay explicit:

```text
Stage statistics view; the current version is rendered from run summaries and log counts, not from a real-time per-transaction event stream.
```
