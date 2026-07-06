import { expect, test } from "@playwright/test";
import {
  applyMetaverseRealismPreset,
  collectBrowserDiagnostics,
  openV3Console,
  previewFormalMatrix,
  runFormalExperiment,
  selectFormalExperimentType,
  selectTwoBaselines,
  selectWorkloadScenarios,
  setFormalTxCount,
  setSeedCount,
} from "./helpers/v3FormalHelpers";

test("runs a minimal formal workload comparison workflow", async ({ page }) => {
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
  expect(result.zipOk).toBe(true);
  if (result.failedRunCount === 0) {
    expect(result.chartGroupCount).toBeGreaterThan(0);
  } else {
    expect(result.failureDiagnosticsVisible).toBe(true);
  }

  expect(diagnostics.errors).toEqual([]);
});
