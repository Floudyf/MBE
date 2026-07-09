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

## V4.3.5.2 Experiment Design Workbench Declutter

### A. Why This Change Is Needed

After V4.3.5, the Experiment Design page still remains too long. It still exposes old entrances, compatibility controls, Draft details, Formal benchmark controls, historical runs, and artifacts close to the main design path.

Users designing experiments need a method template designer, not a list of every available platform feature. After separating method templates from experiment conditions, the design page should focus on module mechanism configuration.

### B. New Page Structure

The Experiment Design page should default to a workbench layout:

- left template sidebar for the current template, template role, validation state, default catalog presets, and saved templates;
- center method pipeline canvas for clickable method modules;
- right module configuration panel for plugin and parameter editing;
- bottom sticky action bar for quick validation, saving draft/runnable templates, and moving to Run Experiment;
- one bottom "Advanced and Compatibility" area, collapsed by default.

### C. Functional Boundary

Workload, nodes, shards, seed, and transaction scale belong to the Run Experiment page.

Method template modules belong to the Experiment Design page:

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

The Formal benchmark entrance remains available for compatibility, but it is not part of the default method-template design flow. Historical run results and artifacts should not appear in the default Experiment Design viewport.

The Composer must remain editable: module cards must be clickable, plugin and parameter edits must update the real Composer Draft state, and validation/saving must use that updated draft.

## V4.3.5.3 Workbench Sidebar and Layout Polish

V4.3.5.2 restored the interactive workbench, but the default sidebar still exposed too many template sources at once. Catalog presets, Composer templates, saved templates, and creation buttons competed with the pipeline canvas and made the page feel like an internal debug console.

The Experiment Design page sidebar should now show only the current template, a compact role selector, and a small recent-template list by default. Full catalog presets, Composer templates, and saved templates belong in a closed management area such as `details`, drawer, or modal. The default saved-template list should show only a few recent templates.

The center method pipeline must receive the most visual space. The sidebar should be narrow and scroll internally when needed, while the right configuration panel should show core controls first and fold longer descriptions, plugin notes, validation notes, and debug-style guidance.

The action bar must not float over the method pipeline. It may be sticky only when it does not obscure content; otherwise it should behave as a normal workbench footer/card below the three-column layout.

This polish round keeps the same functional boundaries: workload, nodes, shards, seed, and transaction scale remain on the Run Experiment page; the design page remains focused on method modules and must not lose module click, plugin selection, or parameter writeback behavior.
