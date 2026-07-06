import { expect, test } from "@playwright/test";
import {
  applyLogicalSyntheticPreset,
  applyMetaverseRealismPreset,
  collectBrowserDiagnostics,
  configureSavedMethodFormalRun,
  createCompatibilitySavedMethod,
  expectNoCompatibilityTopError,
  expectResultDashboardOrFailureDiagnostics,
  fetchFormalRunDetail,
  openAllDetails,
  openV3Console,
  previewFormalMatrix,
  runFormalExperiment,
  selectFormalExperimentType,
  selectTopology,
  selectTwoBaselines,
  selectWorkloadScenarios,
  setFormalTxCount,
  setSeedCount,
} from "./helpers/v3FormalHelpers";

test("synthetic logical formal workload comparison succeeds", async ({ page }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  await openV3Console(page);
  await applyLogicalSyntheticPreset(page);
  await selectFormalExperimentType(page, "workload_comparison");
  await setFormalTxCount(page, 100);
  await setSeedCount(page, 1);
  await selectTwoBaselines(page);
  await selectWorkloadScenarios(page, ["scene_hotspot"]);

  const runCount = await previewFormalMatrix(page, 2);
  expect(runCount).toBeLessThanOrEqual(2);
  const result = await runFormalExperiment(page);

  expect(result.runCount).toBeLessThanOrEqual(2);
  expect(result.completedRunCount).toBeGreaterThan(0);
  expect(result.failedRunCount).toBe(0);
  expect(result.chartGroupCount).toBeGreaterThan(0);
  expect(result.zipOk).toBe(true);
  expect(diagnostics.errors).toEqual([]);
});

test("metaverse local multi-process realism formal workload comparison is observable", async ({ page }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  await openV3Console(page);
  await applyMetaverseRealismPreset(page);
  await selectFormalExperimentType(page, "workload_comparison");
  await setFormalTxCount(page, 1000);
  await setSeedCount(page, 1);
  await selectTwoBaselines(page);
  await selectWorkloadScenarios(page, ["scene_hotspot", "cross_scene_migration", "mixed_metaverse"]);

  const runCount = await previewFormalMatrix(page, 6);
  expect(runCount).toBe(6);
  const result = await runFormalExperiment(page);

  expect(result.runCount).toBe(6);
  expect(result.completedRunCount).toBeGreaterThanOrEqual(1);
  if (result.failedRunCount === 0) {
    expect(result.chartGroupCount).toBeGreaterThan(0);
  } else {
    expect(result.failureDiagnosticsVisible).toBe(true);
  }
  expect(result.zipOk).toBe(true);
  await expect(page.getByTestId("v3-formal-run-history")).toBeVisible();
  await expect(page.getByTestId("v3-formal-run-history").getByText(result.runId)).toBeVisible();
  expect(diagnostics.errors).toEqual([]);
});

test("formal plugin compatibility normalizes metrics and consensus preview plugins", async ({ page, request }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  const saved = await createCompatibilitySavedMethod(request);
  await openV3Console(page);
  await applyLogicalSyntheticPreset(page);
  await configureSavedMethodFormalRun(page, saved.config_id);

  const runCount = await previewFormalMatrix(page, 1);
  expect(runCount).toBe(1);
  await expect(page.getByTestId("v3-formal-preview-warnings")).toContainText("MetricsReport=metatrack_metrics");
  await expect(page.getByTestId("v3-formal-preview-warnings")).toContainText("Consensus=blockemulator_aligned_pbft_preview");

  const result = await runFormalExperiment(page);
  await expectNoCompatibilityTopError(page);
  expect(result.runCount).toBe(1);
  expect(result.completedRunCount).toBeGreaterThanOrEqual(1);
  expect(result.zipOk).toBe(true);
  expect(diagnostics.errors).toEqual([]);
});

test("hotspot sensitivity formal path produces scan-variable figure data", async ({ page }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  await openV3Console(page);
  await applyLogicalSyntheticPreset(page);
  await selectFormalExperimentType(page, "hotspot_sensitivity");
  await setFormalTxCount(page, 500);
  await setSeedCount(page, 1);
  await selectTwoBaselines(page);
  await page.getByTestId("v3-formal-hotspot-points").fill("0.2, 0.4, 0.6");

  const runCount = await previewFormalMatrix(page, 6);
  expect(runCount).toBeLessThanOrEqual(6);
  const result = await runFormalExperiment(page);
  const detail = await fetchFormalRunDetail(page.request, result.runId);

  expect(detail.summary.scan_variable).toBe("hotspot_ratio");
  const xValues = new Set((detail.summary.chart_preview?.groups || []).map((row: any) => String(row.x)));
  expect([...xValues]).toEqual(expect.arrayContaining(["0.2", "0.4", "0.6"]));
  expect(result.zipOk).toBe(true);
  expect(diagnostics.errors).toEqual([]);
});

test("cross-shard sensitivity formal path produces scan-variable figure data", async ({ page }) => {
  test.slow();
  const diagnostics = collectBrowserDiagnostics(page);
  await openV3Console(page);
  await applyLogicalSyntheticPreset(page);
  await selectFormalExperimentType(page, "cross_shard_sensitivity");
  await setFormalTxCount(page, 500);
  await setSeedCount(page, 1);
  await selectTwoBaselines(page);
  await page.getByTestId("v3-formal-cross-shard-points").fill("0.1, 0.3, 0.5");

  const runCount = await previewFormalMatrix(page, 6);
  expect(runCount).toBeLessThanOrEqual(6);
  const result = await runFormalExperiment(page);
  const detail = await fetchFormalRunDetail(page.request, result.runId);

  expect(detail.summary.scan_variable).toBe("cross_shard_ratio");
  const xValues = new Set((detail.summary.chart_preview?.groups || []).map((row: any) => String(row.x)));
  expect([...xValues]).toEqual(expect.arrayContaining(["0.1", "0.3", "0.5"]));
  expect(result.zipOk).toBe(true);
  expect(diagnostics.errors).toEqual([]);
});

test("preview-only existing trace source blocks formal run", async ({ page, request }) => {
  const diagnostics = collectBrowserDiagnostics(page);
  const before = await request.get("/api/v3/composer/formal-metatrack/runs?limit=1");
  const beforeRuns = before.ok() ? (await before.json()).runs || [] : [];
  await openV3Console(page);
  await applyLogicalSyntheticPreset(page);
  await selectTopology(page, "workload_source", "existing_trace_preview");
  await selectFormalExperimentType(page, "workload_comparison");
  await setFormalTxCount(page, 100);
  await setSeedCount(page, 1);
  await selectWorkloadScenarios(page, ["scene_hotspot"]);

  await previewFormalMatrix(page);
  await expect(page.getByTestId("v3-formal-preview-errors")).toContainText("existing_trace_preview");
  await expect(page.getByTestId("v3-formal-run-button")).toBeDisabled();

  const after = await request.get("/api/v3/composer/formal-metatrack/runs?limit=1");
  const afterRuns = after.ok() ? (await after.json()).runs || [] : [];
  expect(afterRuns[0]?.run_id || "").toBe(beforeRuns[0]?.run_id || "");
  expect(diagnostics.errors).toEqual([]);
});
