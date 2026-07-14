import { expect, test } from "@playwright/test";
import { createRunnableMethod, deleteMethod, openRunWithMethods, selectOnlySuite } from "./v5-formal-test-helpers";

test("comparison preview requires two methods and expands both methods across seeds", async ({ page, request }) => {
  const method = await createRunnableMethod(request, `Comparison ${Date.now()}`);
  try {
    await page.goto("/");
    await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
    await page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox").check();
    await selectOnlySuite(page, "comparison_experiment");
    await page.getByRole("button", { name: "预览正式实验矩阵" }).click();
    await expect(page.getByTestId("v5-formal-run-page")).toContainText("方法对比实验至少需要两个方法");
    await expect(page.locator("[data-method-config-id]")).toHaveCount(0);
    await page.getByTestId(`v5-run-method-${method.config_id}`).getByRole("checkbox").check();
    await page.getByLabel("seeds").fill("11,12");
    const preview = page.waitForResponse((response) => response.url().includes("/api/v5/formal/preview") && response.request().method() === "POST");
    await page.getByRole("button", { name: "预览正式实验矩阵" }).click();
    const body = await (await preview).json();
    expect(body.rows).toHaveLength(4);
    for (const id of ["v5_catalog_default", method.config_id]) {
      const rows = body.rows.filter((row: { method_config_id: string }) => row.method_config_id === id);
      expect(rows).toHaveLength(2); expect(rows.map((row: { seed: number }) => row.seed).sort()).toEqual([11, 12]);
      for (const row of rows) { expect(row.suite_type).toBe("comparison_experiment"); expect(row.execution_backend).toBe("real_cluster"); expect(row.runnable).toBe(true); expect(row.blockers).toEqual([]); }
    }
    expect(new Set(body.rows.map((row: { topology_point: unknown }) => JSON.stringify(row.topology_point))).size).toBe(1);
    expect(new Set(body.rows.map((row: { workload_point: unknown }) => JSON.stringify(row.workload_point))).size).toBe(1);
  } finally { await deleteMethod(request, method.config_id); }
});
