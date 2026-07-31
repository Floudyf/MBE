import { expect, test, type APIRequestContext } from "@playwright/test";

type Summary = {
  run_group_id: string;
  status: string;
  plan_name: string;
  execution_backend: string;
  runtime_truth: string;
  total_child_runs: number;
  completed_child_runs: number;
  failed_child_runs: number;
  method_names: string[];
  method_ids: string[];
  suite_names: string[];
  source_label: "user" | "e2e" | "script";
  tags: string[];
  is_test: boolean;
};

const apiURL = (path: string) => `http://127.0.0.1:8000${path}`;

async function testRecord(request: APIRequestContext): Promise<Summary> {
  const read = async () => {
    const response = await request.get(apiURL("/api/v5/formal/run-groups/summaries?limit=20&include_tests=true"));
    expect(response.ok()).toBeTruthy();
    return (await response.json()).items as Summary[];
  };
  const existing = (await read()).find((item) => item.is_test);
  if (existing) return existing;

  const catalogResponse = await request.get(apiURL("/api/v5/plugins?backend=real_cluster"));
  expect(catalogResponse.ok()).toBeTruthy();
  const catalog = (await catalogResponse.json()).items as Array<{ category: string; plugin_id: string; default_config: Record<string, unknown> }>;
  const pluginSelections = Array.from(new Map(catalog.map((item) => [item.category, item])).values()).map((item) => ({
    category: item.category,
    plugin_id: item.plugin_id,
    config: { ...item.default_config },
  }));
  const workload = pluginSelections.find((item) => item.category === "workload");
  expect(workload).toBeTruthy();
  workload!.plugin_id = "deterministic_signed_synthetic";
  workload!.config = { ...workload!.config, cross_shard_ratio: 0, timeout_every: 0 };
  const created = await request.post(apiURL("/api/v5/formal/run-groups"), {
    data: {
      execution_backend: "real_cluster",
      plan: {
        name: "v5_results_history_e2e",
        base_spec: {
          name: "v5_results_history_e2e",
          execution_backend: "real_cluster",
          plugin_selections: pluginSelections,
          topology: { nodes: 4, shards: 1, validators_per_shard: 4 },
          tx_count: 20,
          seed: 11,
          duration_ms: 6000,
          fault_policy: { mode: "disabled" },
          requested_metrics: [],
        },
        suites: ["main_experiment"],
        methods: [{ method_id: "v5_catalog_default", display_name: "V5 Catalog Default", plugin_overrides: {}, role: "main" }],
        seeds: [11],
        repeats: 1,
        source_label: "e2e",
        tags: ["e2e"],
      },
    },
  });
  expect(created.ok()).toBeTruthy();
  const groupId = (await created.json()).run_group_id as string;
  await expect.poll(async () => {
    const detail = await request.get(apiURL(`/api/v5/formal/run-groups/${groupId}`));
    return detail.ok() ? (await detail.json()).group.status : "";
  }, { timeout: 180_000 }).toMatch(/^(completed|completed_with_failures|failed|cancelled)$/);
  const record = (await read()).find((item) => item.run_group_id === groupId && item.is_test);
  expect(record).toBeTruthy();
  return record!;
}

function summaryUrl(url: URL, groupId: string, includeTests: string) {
  return url.pathname.endsWith("/api/v5/formal/run-groups/summaries")
    && url.searchParams.get("search") === groupId
    && url.searchParams.get("include_tests") === includeTests;
}

function summaryRequest(response: import("@playwright/test").Response, groupId: string, includeTests: string) {
  return summaryUrl(new URL(response.url()), groupId, includeTests);
}

test("results history is collapsed, filters real summaries, and keeps current detail above it", async ({ page, request }) => {
  const record = await testRecord(request);
  await page.goto("/?e2e=1");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物" }).click();
  const history = page.getByTestId("v5-run-group-list");
  await expect(history).not.toHaveAttribute("open", "");
  await expect(page.getByTestId("v5-results-page")).toBeVisible();
  await history.locator("summary").click();
  const tests = history.getByRole("checkbox", { name: "显示测试记录" });
  await expect(tests).not.toBeChecked();

  const hiddenResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "false"));
  await history.getByLabel("搜索").fill(record.run_group_id);
  expect((await hiddenResponse).ok()).toBeTruthy();
  await expect(history.locator("tbody tr")).toHaveCount(0);

  const visibleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "true"));
  await tests.check();
  const visiblePayload = await (await visibleResponse).json() as { items: Summary[] };
  expect(visiblePayload.items).toHaveLength(1);
  expect(visiblePayload.items[0].run_group_id).toBe(record.run_group_id);
  expect(visiblePayload.items[0].is_test).toBe(true);
  await expect(history.locator("tbody tr")).toHaveCount(1);
  await expect(history.locator("tbody tr").first().getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);

  await history.getByTestId("v5-run-group-select").click();
  await expect(page.getByTestId("v5-group-rungroup").locator("dd")).toHaveText(record.run_group_id);
  await history.getByLabel("状态").selectOption(record.status);
  await expect(history.locator("tbody tr")).toHaveCount(1);
  if (record.method_ids[0]) {
    await history.getByLabel("方法 ID").fill(record.method_ids[0]);
    await expect(history.locator("tbody tr")).toHaveCount(1);
  }
  if (record.suite_names[0]) {
    await history.getByLabel("实验类型").selectOption(record.suite_names[0]);
    await expect(history.locator("tbody tr")).toHaveCount(1);
  }
  const pageOrder = await page.evaluate(() => ({
    summary: document.querySelector('[data-testid="v5-group-summary"]')?.getBoundingClientRect().top ?? -1,
    history: document.querySelector('[data-testid="v5-run-group-list"]')?.getBoundingClientRect().top ?? -1,
  }));
  expect(pageOrder.summary).toBeLessThan(pageOrder.history);
});

test("latest history response wins when include-tests changes quickly", async ({ page }) => {
  const record: Summary = {
    run_group_id: "v5grp_history_race_e2e",
    status: "completed",
    plan_name: "history race",
    execution_backend: "real_cluster",
    runtime_truth: "v5_real_cluster_candidate",
    total_child_runs: 1,
    completed_child_runs: 1,
    failed_child_runs: 0,
    method_names: ["V5 Catalog Default"],
    method_ids: ["v5_catalog_default"],
    suite_names: ["main_experiment"],
    source_label: "e2e",
    tags: ["e2e"],
    is_test: true,
  };
  await page.route("**/api/v5/formal/run-groups/summaries**", async (route) => {
    const url = new URL(route.request().url());
    if (url.searchParams.get("include_tests") === "false" && url.searchParams.get("search") === record.run_group_id) {
      await new Promise((resolve) => setTimeout(resolve, 500));
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [], total: 0, next_cursor: null }) });
      return;
    }
    if (url.searchParams.get("include_tests") === "false") {
      await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [], total: 0, next_cursor: null }) });
      return;
    }
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [record], total: 1, next_cursor: null }) });
  });
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "③ 结果与产物" }).click();
  const history = page.getByTestId("v5-run-group-list");
  await history.locator("summary").click();
  const staleRequest = page.waitForRequest((request) => summaryUrl(new URL(request.url()), record.run_group_id, "false"));
  const staleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "false"));
  const visibleResponse = page.waitForResponse((response) => summaryRequest(response, record.run_group_id, "true"));
  await history.getByLabel("搜索").fill(record.run_group_id);
  await staleRequest;
  await history.getByRole("checkbox", { name: "显示测试记录" }).check();
  expect((await visibleResponse).ok()).toBeTruthy();
  await expect(history.getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);
  expect((await staleResponse).ok()).toBeTruthy();
  await expect(history.getByTestId("v5-run-group-select")).toHaveText(record.run_group_id);
});

test("results page exposes legacy saved-config scan and dry-run cleanup evidence", async ({ page }) => {
  await page.route("**/api/v5/formal/run-groups/summaries**", async (route) => {
    await route.fulfill({ contentType: "application/json", body: JSON.stringify({ items: [], total: 0, next_cursor: null }) });
  });
  await page.route("**/api/v5/formal/cleanup/legacy-saved-configs", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "mbe_v5_legacy_saved_config_scan_v1",
        candidate_configs: [{ config_id: "v3cfg_legacy_formal_plan", config_kind: "formal_plan", name: "old formal", validation_status: "draft", reason: "legacy_formal_plan" }],
        preserved_configs: [{ config_id: "v3cfg_current_v5_method", config_kind: "method", name: "current method", validation_status: "runnable", reason: "current_v5_method_profile" }],
        candidate_count: 1,
      }),
    });
  });
  await page.route("**/api/v5/formal/cleanup/legacy-saved-configs?dry_run=true", async (route) => {
    await route.fulfill({
      contentType: "application/json",
      body: JSON.stringify({
        schema_version: "mbe_v5_cleanup_plan_v1",
        dry_run: true,
        deleted_run_group_ids: [],
        deleted_output_dirs: [],
        deleted_orphan_dirs: [],
        deleted_saved_config_ids: ["v3cfg_legacy_formal_plan"],
        preserved_run_group_ids: [],
        preserved_saved_config_ids: ["v3cfg_current_v5_method"],
        skipped_active_runs: [],
        skipped_output_dirs: [],
        released_bytes: 2048,
        errors: [],
        cleanup_report: {
          report_id: "cleanup_legacy_saved_configs_20260729",
          action: "legacy_saved_configs",
          json: ".cache/v5_formal_runs/cleanup/cleanup_legacy_saved_configs_20260729/cleanup_report.json",
          csv: ".cache/v5_formal_runs/cleanup/cleanup_legacy_saved_configs_20260729/cleanup_report.csv",
        },
      }),
    });
  });

  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button").nth(2).click();
  const history = page.getByTestId("v5-run-group-list");
  await history.locator("summary").click();
  await history.getByRole("button", { name: "扫描旧方案" }).click();
  await expect(page.getByTestId("v5-cleanup-evidence")).toContainText("v3cfg_legacy_formal_plan");
  await history.getByRole("button", { name: "旧方案 dry-run" }).click();
  await expect(page.getByTestId("v5-cleanup-evidence")).toContainText("cleanup_report");
  await expect(page.getByTestId("v5-cleanup-evidence")).toContainText("cleanup_report.json");
});

test("child detail exposes artifact contract missing evidence", async ({ page }) => {
  const group: Summary = {
    run_group_id: "v5grp_artifact_contract_e2e",
    status: "completed",
    plan_name: "artifact contract",
    execution_backend: "real_cluster",
    runtime_truth: "v5_real_cluster_candidate",
    total_child_runs: 1,
    completed_child_runs: 1,
    failed_child_runs: 0,
    method_names: ["Baseline"],
    method_ids: ["hash_serial"],
    suite_names: ["comparison_experiment"],
    source_label: "e2e",
    tags: ["e2e"],
    is_test: true,
  };
  const child = {
    run_group_id: group.run_group_id,
    child_run_id: "v5child_artifact_contract",
    status: "completed",
    suite_type: "comparison_experiment",
    method: { method_id: "hash_serial", display_name: "Baseline", plugin_overrides: {}, role: "baseline" },
    method_config_id: "hash_serial",
    workload_point: {},
    topology_point: { nodes: 8, shards: 2, validators_per_shard: 4 },
    fault_point: {},
    seed: 73,
    repeat_index: 0,
    scan_variable: "",
    scan_value: "",
    comparison_group_id: "comparison",
    execution_backend: "real_cluster",
    estimated_processes: 8,
    estimated_transactions: 1000,
    runnable: true,
    blockers: [],
    warnings: [],
    paper_candidate: false,
    result: {
      run_id: "v5_1000_artifact_contract",
      status: "completed",
      no_fallback: true,
      artifacts: [{
        name: "aggregate/remote_state_metrics_summary.json",
        size_bytes: 256,
        artifact_role: "aggregate_metric",
        truth_scope: "node_physical_and_replica_deduplicated",
        producer: "v5_metric_aggregator",
        schema_version: "mbe_remote_state_metrics_v2",
        download_url: "/api/v5/real-cluster/runs/v5_1000_artifact_contract/artifacts/aggregate/remote_state_metrics_summary.json",
      }],
      summary: {
        runtime_stage: "v5_real_cluster_candidate",
        runtime_truth: "v5_real_cluster_candidate",
        state_root_consistent: true,
        no_fallback: true,
        finality_evidence: {
          submitted_unique_tx_count: 1000,
          terminal_unique_tx_count: 1000,
          incomplete_unique_tx_count: 0,
          finalized_unique_logical_tx_count: 1000,
          throughput_tps: 10,
          end_to_end_tps: 9,
          logical_finality_tps: 12,
          completion_duration_ms: 111,
          tail_completion_overhead_ms: 11,
          p99_finality_ms: 41,
        },
        artifact_contract_status: "incomplete",
        missing_expected_artifacts: ["aggregate/mechanism_metrics_summary.json"],
        artifact_contract: {
          artifact_contract_status: "incomplete",
          expected_artifact_count: 8,
          actual_artifact_count: 7,
        },
      },
    },
    metrics: {
      throughput_tps: 10,
      end_to_end_tps: 9,
      logical_finality_tps: 12,
      completion_duration_ms: 111,
      tail_completion_overhead_ms: 11,
      p99_latency_ms: 42,
      p99_finality_ms: 41,
      finalized_tx_count: 1000,
      lifecycle_complete: true,
      planning_scheduler_event_count: 10,
      runtime_scheduler_event_count: 11,
      leader_scheduler_event_count: 6,
      replica_scheduler_event_count: 22,
      unique_logical_scheduling_decision_count: 5,
      blocked_logical_tx_count: 2,
      wakeup_logical_tx_count: 2,
      dependency_wait_event_count: 1,
      work_steal_attempt_count: 0,
      work_steal_success_count: 0,
      physical_remote_operation_count: 40,
      physical_remote_fetch_count: 24,
      physical_remote_writeback_count: 16,
      replica_deduplicated_remote_operation_count: 10,
      replica_deduplicated_remote_fetch_count: 6,
      replica_deduplicated_remote_writeback_count: 4,
      remote_fetches_per_logical_tx: 0.006,
      remote_writebacks_per_logical_tx: 0.004,
      remote_operations_per_logical_tx: 0.01,
      replica_amplification_factor: 4,
      pre_aggregation_physical_op_count: 7,
      post_aggregation_physical_op_count: 5,
      aggregated_key_count: 2,
      aggregated_logical_delta_count: 4,
      physical_ops_saved_count: 2,
      aggregation_reduction_ratio: 0.285714,
      logical_update_count_deprecated: 7,
      physical_update_count_deprecated: 5,
      artifact_contract_status: "incomplete",
      expected_artifact_count: 8,
      actual_artifact_count: 7,
      missing_expected_artifacts: ["aggregate/mechanism_metrics_summary.json"],
      missing: ["artifact_contract:missing:aggregate/mechanism_metrics_summary.json"],
    },
  };

  await page.route("**/api/v5/formal/**", async (route) => {
    const url = new URL(route.request().url());
    if (url.pathname.endsWith("/summaries")) return route.fulfill({ json: { items: [group], total: 1, next_cursor: null } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/metrics`)) return route.fulfill({ json: {} });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/artifacts`)) return route.fulfill({ json: { run_group_id: group.run_group_id, status: "ready", bundle_ready: false, bundle_size_bytes: 0, file_count: 0, files: [] } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/analysis`)) return route.fulfill({ json: { run_group_id: group.run_group_id, charts: [], groups: [] } });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}/children/${child.child_run_id}`)) return route.fulfill({ json: child });
    if (url.pathname.endsWith(`/run-groups/${group.run_group_id}`)) return route.fulfill({ json: { group, children: [child] } });
    return route.fulfill({ status: 404, json: { detail: "unexpected V5 formal endpoint" } });
  });

  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button").nth(2).click();
  await expect(page.getByTestId("v5-artifact-contract-status").locator("dd")).toHaveText("incomplete");
  await expect(page.getByTestId("v5-artifact-contract-expected").locator("dd")).toHaveText("8");
  await expect(page.getByTestId("v5-artifact-contract-actual").locator("dd")).toHaveText("7");
  await expect(page.getByTestId("v5-artifact-contract-missing").locator("dd")).toContainText("aggregate/mechanism_metrics_summary.json");
  await expect(page.getByTestId("v5-metric-end-to-end-tps").locator("dd")).toHaveText("9");
  await expect(page.getByTestId("v5-metric-logical-finality-tps").locator("dd")).toHaveText("12");
  await expect(page.getByTestId("v5-metric-completion-duration").locator("dd")).toHaveText("111");
  await expect(page.getByTestId("v5-metric-tail-completion-overhead").locator("dd")).toHaveText("11");
  await expect(page.getByTestId("v5-metric-p99-finality").locator("dd")).toHaveText("41");
  await expect(page.getByTestId("v5-metric-planning-scheduler-event-count").locator("dd")).toHaveText("10");
  await expect(page.getByTestId("v5-metric-runtime-scheduler-event-count").locator("dd")).toHaveText("11");
  await expect(page.getByTestId("v5-metric-unique-logical-scheduling-decision-count").locator("dd")).toHaveText("5");
  await expect(page.getByTestId("v5-metric-physical-remote-operation-count").locator("dd")).toHaveText("40");
  await expect(page.getByTestId("v5-metric-replica-deduplicated-remote-operation-count").locator("dd")).toHaveText("10");
  await expect(page.getByTestId("v5-metric-pre-aggregation-physical-op-count").locator("dd")).toHaveText("7");
  await expect(page.getByTestId("v5-metric-post-aggregation-physical-op-count").locator("dd")).toHaveText("5");
  await expect(page.getByTestId("v5-metric-physical-ops-saved-count").locator("dd")).toHaveText("2");
  await expect(page.getByTestId("v5-metric-logical-update-count-deprecated").locator("dd")).toHaveText("7");
  await expect(page.getByTestId("v5-child-artifact-catalog")).toContainText("aggregate_metric");
  await expect(page.getByTestId("v5-child-artifact-catalog")).toContainText("node_physical_and_replica_deduplicated");
  await expect(page.getByTestId("v5-child-artifact-catalog")).toContainText("v5_metric_aggregator / mbe_remote_state_metrics_v2");
});
