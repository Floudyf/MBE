import { expect, test } from "@playwright/test";

test("protects unsaved V5 method changes before entering Run", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("v5-method-name").fill("未保存方法");
  await page.getByTestId("v5-method-category-routing").locator("select").selectOption("metatrack_coaccess_routing");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  const dialog = page.getByRole("dialog", { name: "未保存方法设计" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("button", { name: "保存并进入运行实验" })).toBeVisible();
  await expect(dialog.getByRole("button", { name: "放弃修改并离开" })).toBeVisible();
  await dialog.getByRole("button", { name: "继续编辑" }).click();
  await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  await dialog.getByRole("button", { name: "放弃修改并离开" }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("未选择");
  await expect(page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox")).not.toBeChecked();
  await expect(page.getByRole("button", { name: "预览正式实验矩阵" })).toBeDisabled();
});

test("consumes a save-and-run navigation command only once", async ({ page, request }) => {
  let createCount = 0;
  page.on("request", (entry) => { if (entry.url().includes("/api/v3/saved-configs") && entry.method() === "POST") createCount += 1; });
  let configId = "";
  try {
    await page.goto("/");
    await page.getByTestId("v5-method-name").fill(`stale save ${Date.now()}`);
    const validation = page.waitForResponse((response) => response.url().includes("/api/v5/experiment-spec/validate") && response.request().method() === "POST");
    await page.getByTestId("v5-method-validate").click();
    expect((await validation).ok()).toBeTruthy();
    await page.getByTestId("v5-method-description").fill("validated design handoff");
    await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
    const dialog = page.getByRole("dialog", { name: "未保存方法设计" });
    const saved = page.waitForResponse((response) => response.url().includes("/api/v3/saved-configs") && response.request().method() === "POST");
    await dialog.getByRole("button", { name: "保存并进入运行实验" }).click();
    const payload = await (await saved).json();
    configId = payload.config_id;
    await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
    await page.getByTestId("primary-navigation").getByRole("button", { name: "① 实验设计" }).click();
    await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
    await page.waitForTimeout(300);
    expect(createCount).toBe(1);
    await expect(page.getByTestId("v5-method-design-page")).not.toContainText("请先完成方法名称并验证当前插件组合");
  } finally {
    if (configId) await request.delete(`/api/v3/saved-configs/${configId}`);
  }
});
