import { expect, type Page, type APIRequestContext } from "@playwright/test";

export type FormalRunCheck = {
  runId: string;
  runCount: number;
  completedRunCount: number;
  failedRunCount: number;
  chartGroupCount: number;
  zipOk: boolean;
  failureDiagnosticsVisible: boolean;
};

export function collectBrowserDiagnostics(page: Page) {
  const errors: string[] = [];
  page.on("console", (message) => {
    if (message.type() === "error") errors.push(`console: ${message.text()}`);
  });
  page.on("requestfailed", (request) => {
    errors.push(`request failed: ${request.method()} ${request.url()} ${request.failure()?.errorText || ""}`);
  });
  page.on("response", (response) => {
    const status = response.status();
    if (status >= 400 && response.url().includes("/api/")) {
      errors.push(`response ${status}: ${response.url()}`);
    }
  });
  return { errors };
}

export async function openV3Console(page: Page) {
  await page.goto("/");
  await expect(page.getByTestId("v3-runtime-topology-panel")).toBeVisible({ timeout: 30_000 });
  await expect(page.getByTestId("v3-formal-experiment-panel")).toBeVisible({ timeout: 30_000 });
  await openAllDetails(page);
}

export async function openAllDetails(page: Page) {
  await page.locator("details").evaluateAll((details) => {
    for (const item of details) (item as HTMLDetailsElement).open = true;
  });
}

export async function applyLogicalSyntheticPreset(page: Page) {
  await openAllDetails(page);
  await page.getByTestId("v3-topology-preset-quick").click();
  await openAllDetails(page);
  await selectTopology(page, "workload_source", "synthetic");
  await selectTopology(page, "node_runtime_mode", "logical_single_process");
  await selectTopology(page, "process_runtime_mode", "dry_run");
  await selectTopology(page, "network_adapter", "in_memory_message_bus");
  await selectTopology(page, "cross_shard_protocol", "none");
  await selectTopology(page, "state_backend", "memory_kv");
  await page.getByTestId("v3-formal-runtime-evidence-mode").selectOption("logical_single_process");
}

export async function applyMetaverseRealismPreset(page: Page) {
  await openAllDetails(page);
  await page.getByTestId("v3-topology-preset-realism").click();
  await openAllDetails(page);
  await selectTopology(page, "workload_source", "metaverse");
  await selectTopology(page, "node_runtime_mode", "local_multi_process");
  await selectTopology(page, "process_runtime_mode", "smoke");
  await selectTopology(page, "network_adapter", "localhost_tcp_preview");
  await selectTopology(page, "cross_shard_protocol", "relay_mvp");
  await selectTopology(page, "state_backend", "merkle_trie_mvp");
  await setTopologyNumber(page, "shard_count", "4");
  await setTopologyNumber(page, "validators_per_shard", "4");
  await setTopologyNumber(page, "executors_per_shard", "1");
  await setTopologyNumber(page, "storage_nodes_per_shard", "1");
  await setTopologyNumber(page, "max_local_processes", "8");
  await page.getByTestId("v3-formal-runtime-evidence-mode").selectOption("local_multi_process_validation");
}

export async function selectFormalExperimentType(page: Page, type: string) {
  await page.getByTestId("v3-formal-experiment-type").selectOption(type);
  await openAllDetails(page);
}

export async function setFormalTxCount(page: Page, value: number) {
  await fillNumber(page, "v3-formal-tx-count", String(value));
}

export async function setSeedCount(page: Page, value: number) {
  await fillNumber(page, "v3-formal-seed-count", String(value));
}

export async function selectTwoBaselines(page: Page) {
  const selected = new Set(["baseline_hash_serial", "metatrack_full"]);
  for (const id of ["baseline_hash_serial", "baseline_hash_prefetch", "baseline_hash_dual_track", "baseline_hash_aggregation", "metatrack_full"]) {
    const checkbox = page.getByTestId(`v3-formal-baseline-${id}`);
    if (selected.has(id)) await checkbox.check({ force: true });
    else await checkbox.uncheck({ force: true });
  }
}

export async function selectWorkloadScenarios(page: Page, scenarios: string[]) {
  const selected = new Set(scenarios);
  for (const id of ["asset_transfer", "avatar_update", "scene_hotspot", "item_transfer", "cross_scene_migration", "mixed_metaverse"]) {
    const chip = page.getByTestId(`v3-formal-workload-scenario-${id}`);
    const shouldSelect = selected.has(id);
    const isSelected = (await chip.getAttribute("data-selected")) === "true";
    if (shouldSelect !== isSelected) {
      await chip.click();
      await expect(chip).toHaveAttribute("data-selected", shouldSelect ? "true" : "false");
    }
  }
}

export async function previewFormalMatrix(page: Page, expectedMaxRunCount?: number) {
  const [response] = await Promise.all([
    page.waitForResponse((response) => response.url().includes("/api/v3/composer/formal-metatrack/preview") && response.request().method() === "POST"),
    page.getByTestId("v3-formal-preview-button").click(),
  ]);
  expect(response.ok()).toBe(true);
  const payload = await response.json();
  const runCount = Number(payload.run_count ?? 0);
  await expect(page.getByTestId("v3-formal-matrix-preview")).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("v3-formal-preview-run-count")).toHaveText(String(runCount));
  if (expectedMaxRunCount != null) expect(runCount).toBeLessThanOrEqual(expectedMaxRunCount);
  return runCount;
}

export async function runFormalExperiment(page: Page): Promise<FormalRunCheck> {
  await expect(page.getByTestId("v3-formal-run-button")).toBeEnabled({ timeout: 20_000 });
  const [response] = await Promise.all([
    page.waitForResponse((item) => item.url().includes("/api/v3/composer/formal-metatrack/run") && item.request().method() === "POST", { timeout: 240_000 }),
    page.getByTestId("v3-formal-run-button").click(),
  ]);
  expect(response.ok()).toBe(true);
  const payload = await response.json();
  const runId = String(payload.run_id || "");
  await expect(page.getByTestId("v3-formal-result-run-id")).toHaveText(runId, { timeout: 30_000 });
  return expectResultDashboardOrFailureDiagnostics(page);
}

export async function expectResultDashboardOrFailureDiagnostics(page: Page): Promise<FormalRunCheck> {
  await expect(page.getByTestId("v3-formal-result-panel")).toBeVisible({ timeout: 240_000 });
  await expect(page.getByTestId("v3-formal-data-file-list")).toBeVisible();
  const runId = (await page.getByTestId("v3-formal-result-run-id").innerText()).trim();
  const runCount = Number((await page.getByTestId("v3-formal-result-run-count").innerText()).trim());
  const completedRunCount = Number((await page.getByTestId("v3-formal-result-completed-count").innerText()).trim());
  const failedRunCount = Number((await page.getByTestId("v3-formal-result-failed-count").innerText()).trim());
  const failureDiagnosticsVisible = await page.getByTestId("v3-formal-failure-diagnostics").isVisible().catch(() => false);
  if (failedRunCount > 0) {
    expect(failureDiagnosticsVisible).toBe(true);
  } else {
    await expect(page.getByTestId("v3-formal-chart-dashboard")).toBeVisible();
  }
  const detail = await fetchFormalRunDetail(page.request, runId);
  const chartGroupCount = Array.isArray(detail.summary?.chart_preview?.groups) ? detail.summary.chart_preview.groups.length : 0;
  const zipOk = await downloadZipCheck(page, runId);
  return { runId, runCount, completedRunCount, failedRunCount, chartGroupCount, zipOk, failureDiagnosticsVisible };
}

export async function downloadZipCheck(page: Page, runId: string) {
  const response = await page.request.get(`/api/v3/composer/formal-metatrack/${encodeURIComponent(runId)}/artifacts.zip`);
  return response.ok();
}

export async function fetchFormalRunDetail(request: APIRequestContext, runId: string): Promise<any> {
  const response = await request.get(`/api/v3/composer/formal-metatrack/runs/${encodeURIComponent(runId)}`);
  expect(response.ok()).toBe(true);
  return response.json();
}

export async function createCompatibilitySavedMethod(request: APIRequestContext) {
  const name = `E2E compatibility method ${Date.now()}`;
  const response = await request.post("/api/v3/saved-configs", {
    data: {
      config_kind: "method",
      name,
      description: "E2E fixture for formal Go runtime compatibility normalization.",
      owner_label: "playwright_e2e",
      tags: ["e2e", "compatibility"],
      validation_status: "runnable",
      payload: { draft: compatibilityDraft() },
    },
  });
  expect(response.ok()).toBe(true);
  return response.json();
}

export async function configureSavedMethodFormalRun(page: Page, configId: string) {
  await openAllDetails(page);
  await page.getByTestId("v3-formal-method-source").selectOption("saved");
  await openAllDetails(page);
  await page.getByTestId(`v3-formal-method-config-${configId}`).check({ force: true });
  await selectFormalExperimentType(page, "workload_comparison");
  await setFormalTxCount(page, 100);
  await setSeedCount(page, 1);
  await selectWorkloadScenarios(page, ["scene_hotspot"]);
}

export async function expectNoCompatibilityTopError(page: Page) {
  const text = await page.locator("body").innerText();
  expect(text).not.toContain("V3 Go runtime requires MetricsPlugin=basic_metrics");
  expect(text).not.toContain("unsupported V3 Go runtime plugin ConsensusPlugin=blockemulator_aligned_pbft_preview");
}

export async function selectTopology(page: Page, id: string, value: string) {
  await page.getByTestId(`v3-topology-${id}`).selectOption(value);
}

export async function setTopologyNumber(page: Page, id: string, value: string) {
  await fillNumber(page, `v3-topology-${id}`, value);
}

export async function fillNumber(page: Page, testId: string, value: string) {
  const input = page.getByTestId(testId).first();
  await input.fill(value);
  await input.dispatchEvent("input");
  await input.dispatchEvent("change");
}

function compatibilityDraft() {
  return {
    template_id: "metatrack_ablation",
    preset_id: "e2e_compatibility",
    topology: {
      shard_count: 4,
      validators_per_shard: 4,
      executors_per_shard: 1,
      storage_nodes_per_shard: 1,
      supervisor_enabled: true,
      node_runtime_mode: "logical_single_process",
      process_runtime_mode: "dry_run",
      network_adapter: "in_memory_message_bus",
      network_mode: "in_memory_message_bus",
      cross_shard_protocol: "none",
      state_backend: "memory_kv",
      workload_source: "synthetic",
    },
    modules: {
      Workload: { module_id: "Workload", status: "fixed", plugin: "synthetic_hotspot" },
      TxPool: { module_id: "TxPool", status: "fixed", plugin: "fifo_pool" },
      BlockProducer: { module_id: "BlockProducer", status: "fixed", plugin: "time_or_count_block_producer" },
      Consensus: { module_id: "Consensus", status: "fixed", plugin: "blockemulator_aligned_pbft_preview" },
      CommitteeEpoch: { module_id: "CommitteeEpoch", status: "disabled", plugin: "disabled" },
      Routing: { module_id: "Routing", status: "variable", plugin: "metatrack_coaccess_routing" },
      Execution: { module_id: "Execution", status: "variable", plugin: "metatrack_dual_track_execution" },
      StateAccess: { module_id: "StateAccess", status: "variable", plugin: "access_list_prefetch" },
      StateStorage: { module_id: "StateStorage", status: "fixed", plugin: "hash_state_storage" },
      Commit: { module_id: "Commit", status: "variable", plugin: "constraint_checked_aggregation" },
      MetricsReport: { module_id: "MetricsReport", status: "output", plugin: "metatrack_metrics" },
    },
  };
}
