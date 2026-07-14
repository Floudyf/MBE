import { expect, test } from "@playwright/test";
import { createRunnableMethod, deleteMethod, openRunWithMethods, selectOnlySuite } from "./v5-formal-test-helpers";

test("workload sensitivity keeps every selected method for every workload point", async ({ page, request }) => {
  const method = await createRunnableMethod(request, `Sensitivity ${Date.now()}`);
  try {
    await openRunWithMethods(page, method.config_id);
    await selectOnlySuite(page, "workload_sensitivity");
    await page.getByLabel("nodes").fill("8"); await page.getByLabel("shards").fill("2"); await page.getByLabel("validators per shard").fill("4");
    const editor = page.getByTestId("v5-point-editor-负载扫描点");
    await editor.getByRole("button", { name: "添加扫描点" }).click();
    await editor.getByRole("button", { name: "添加扫描点" }).click();
    const fields = editor.locator('input[type="number"]');
    await fields.nth(0).fill("20"); await fields.nth(1).fill("0"); await fields.nth(2).fill("0");
    await fields.nth(3).fill("80"); await fields.nth(4).fill("0.25"); await fields.nth(5).fill("0");
    await expect(page.getByTestId("v5-estimated-children")).toHaveText("4");
    await expect(page.getByTestId("v5-estimated-process-starts")).toHaveText("32");
    await expect(page.getByTestId("v5-estimated-transactions")).toHaveText("200");
    const preview = page.waitForResponse((response) => response.url().includes("/api/v5/formal/preview") && response.request().method() === "POST");
    await page.getByRole("button", { name: "预览正式实验矩阵" }).click();
    const body = await (await preview).json();
    expect(body.rows).toHaveLength(4);
    for (const id of ["v5_catalog_default", method.config_id]) {
      const rows = body.rows.filter((row: { method_config_id: string }) => row.method_config_id === id);
      expect(rows).toHaveLength(2);
      expect(rows.map((row: { estimated_transactions: number }) => row.estimated_transactions).sort()).toEqual([20, 80]);
    }
    const pointA = body.rows.filter((row: { estimated_transactions: number }) => row.estimated_transactions === 20);
    const pointB = body.rows.filter((row: { estimated_transactions: number }) => row.estimated_transactions === 80);
    for (const row of [...pointA, ...pointB]) { expect(row.suite_type).toBe("workload_sensitivity"); expect(row.execution_backend).toBe("real_cluster"); expect(row.runnable).toBe(true); expect(row.scan_variable).not.toBe(""); }
    expect(pointA.every((row: { workload_point: { cross_shard_ratio: number } }) => row.workload_point.cross_shard_ratio === 0)).toBe(true);
    expect(pointB.every((row: { workload_point: { cross_shard_ratio: number } }) => row.workload_point.cross_shard_ratio === 0.25)).toBe(true);
  } finally { await deleteMethod(request, method.config_id); }
});
