import { expect, test } from "@playwright/test";
import { selectOnlySuite } from "./v5-formal-test-helpers";

test("comparison preview expands the formal four methods across seeds", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("primary-navigation").locator("button").nth(1).click();
  await page.getByTestId("workload-mode-derived").click();
  await expect(page.getByLabel("dataset tx count").locator("option")).toHaveText(["1K", "10K", "50K", "100K", "250K", "Full"]);
  await page.getByTestId("workload-mode-synthetic").click();
  await selectOnlySuite(page, "comparison_experiment");
  await page.getByLabel("tx_count").fill("20");
  await page.getByLabel("seeds").fill("11,12");

  const preview = page.waitForResponse((response) => response.url().includes("/api/v5/formal/preview") && response.request().method() === "POST");
  await page.getByTestId("v5-formal-preview-button").click();
  const body = await (await preview).json();

  expect(body.rows).toHaveLength(8);
  for (const id of ["hash_serial", "hash_block_stm", "metatrack_serial", "metatrack_block_stm"]) {
    const rows = body.rows.filter((row: { method_config_id: string }) => row.method_config_id === id);
    expect(rows).toHaveLength(2);
    expect(rows.map((row: { seed: number }) => row.seed).sort()).toEqual([11, 12]);
    for (const row of rows) {
      expect(row.suite_type).toBe("comparison_experiment");
      expect(row.execution_backend).toBe("real_cluster");
      expect(row.runnable).toBe(true);
      expect(row.blockers).toEqual([]);
    }
  }
  expect(new Set(body.rows.map((row: { topology_point: unknown }) => JSON.stringify(row.topology_point))).size).toBe(1);
  expect(new Set(body.rows.map((row: { workload_point: unknown }) => JSON.stringify(row.workload_point))).size).toBe(1);
});
