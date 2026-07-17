import { expect, test } from "@playwright/test";

for (const viewport of [{ width: 1440, height: 900 }, { width: 1280, height: 800 }, { width: 1024, height: 768 }, { width: 1920, height: 1080 }]) {
  test(`V5 Chinese primary pages fit ${viewport.width}x${viewport.height}`, async ({ page }) => {
    await page.setViewportSize(viewport);
    await page.goto("/");
    const nav = page.getByTestId("primary-navigation");
    const pages: Array<{ index: number; testId: string; label: string }> = [
      { index: 0, testId: "v5-method-design-page", label: "method design" },
      { index: 1, testId: "v5-formal-run-page", label: "formal run" },
      { index: 2, testId: "v5-results-page", label: "results" },
      { index: 3, testId: "v5-workload-library-page", label: "workload library" },
    ];
    for (const entry of pages) {
      await nav.getByRole("button").nth(entry.index).click();
      await expect(page.getByTestId(entry.testId)).toBeVisible();
      const overflow = await page.evaluate(() => ({ scrollWidth: document.documentElement.scrollWidth, clientWidth: document.documentElement.clientWidth }));
      expect(overflow.scrollWidth, `${entry.label} page overflow: scrollWidth=${overflow.scrollWidth}, clientWidth=${overflow.clientWidth}`).toBeLessThanOrEqual(overflow.clientWidth + 1);
    }
  });
}

test("V5 method design page exposes workflow information architecture", async ({ page }) => {
  await page.setViewportSize({ width: 1440, height: 900 });
  await page.goto("/");
  await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
  await expect(page.locator(".v5-method-steps")).toHaveCount(0);
  await expect(page.getByText("实验矩阵")).toHaveCount(0);
  await expect(page.getByTestId("v5-method-design-page").getByRole("heading", { name: "实验方法设计" })).toHaveCount(1);
  await expect(page.getByTestId("v5-method-design-page").getByText("V5 插件化方法模板")).toHaveCount(0);
  await expect(page.getByTestId("v5-method-design-page").getByText("METHOD PROFILE")).toHaveCount(0);
  await expect(page.getByTestId("v5-method-design-page").getByText("PLUGIN ARCHITECTURE")).toHaveCount(0);
  await expect(page.getByTestId("v5-method-design-page").getByText("SAVED PROFILES")).toHaveCount(0);
  await page.getByRole("tab", { name: "流程配置" }).click();
  for (const group of ["交易入口", "分片控制", "排序与共识", "执行与状态", "运行环境", "结果证据"]) {
    await expect(page.getByText(group, { exact: true })).toBeVisible();
  }
  const flowGroups = await page.locator(".v5-method-group > header h4").allTextContents();
  expect(flowGroups.slice(0, 6)).toEqual(["交易入口", "分片控制", "排序与共识", "执行与状态", "运行环境", "结果证据"]);
  await expect(page.getByTestId("v5-method-category-scheduler")).toContainText("区块前排序");
  await expect(page.getByTestId("v5-method-category-execution")).toContainText("执行策略");
  await expect(page.getByTestId("v5-method-category-block_executor")).toContainText("区块执行引擎");
  await expect(page.getByTestId("v5-method-summary-sidebar")).toContainText("模块覆盖");
  await expect(page.locator(".v5-module-card.focused")).toHaveCount(0);
  await expect(page.locator(".v5-category-row.focused")).toHaveCount(0);

  for (const tab of ["组件清单", "默认参数", "兼容性与来源", "流程配置"]) {
    await page.getByRole("tab", { name: tab }).click();
    await expect(page.getByRole("tab", { name: tab })).toHaveAttribute("aria-selected", "true");
  }

  await page.getByRole("tab", { name: "组件清单" }).click();
  await expect(page.locator(".v5-category-row.header").first()).toContainText("当前插件选择");
  await expect(page.locator(".v5-category-row").nth(1)).not.toHaveClass(/focused/);
  await page.getByRole("tab", { name: "默认参数" }).click();
  await expect(page.getByText("方法独立参数尚未进入 Formal Matrix")).toBeVisible();
  const parameterCards = await page.locator(".v5-parameter-card").count();
  expect(parameterCards).toBeGreaterThan(0);
  await expect(page.getByText(/无参数组件/)).toBeVisible();
  await page.getByText(/无参数组件/).click();
  await expect(page.locator(".v5-compact-list")).toBeVisible();
  await page.getByRole("tab", { name: "兼容性与来源" }).click();
  await expect(page.getByText("兼容性与来源").first()).toBeVisible();
  await expect(page.locator(".v5-dependency-row.header span")).toHaveCount(5);
  await page.locator(".v5-dependency-entry summary").first().click();
  await expect(page.locator(".v5-dependency-details").first()).toBeVisible();

  const initialOverrides = await page.getByTestId("v5-summary-overrides-count").innerText();
  await page.getByRole("tab", { name: "流程配置" }).click();
  const blockExecutorCard = page.getByTestId("v5-method-category-block_executor");
  const initialBorder = await blockExecutorCard.evaluate((element) => getComputedStyle(element).borderColor);
  await blockExecutorCard.hover();
  const hoverBorder = await blockExecutorCard.evaluate((element) => getComputedStyle(element).borderColor);
  expect(hoverBorder).not.toBe(initialBorder);
  await page.mouse.move(20, 20);
  await expect(blockExecutorCard).not.toHaveClass(/focused/);

  const txpoolSelect = page.getByTestId("v5-method-category-txpool").locator("select");
  await txpoolSelect.focus();
  const focusBorder = await page.getByTestId("v5-method-category-txpool").evaluate((element) => getComputedStyle(element).borderColor);
  expect(focusBorder).not.toBe(initialBorder);

  const routingSelect = page.getByTestId("v5-method-category-routing").locator("select");
  const options = await routingSelect.locator("option").evaluateAll((items) => items.map((item) => (item as HTMLOptionElement).value));
  const alternate = options.find((value) => value !== options[0]);
  if (alternate) {
    await routingSelect.selectOption(alternate);
    await expect(page.getByTestId("v5-summary-overrides-count")).not.toHaveText(initialOverrides);
    await expect(page.getByTestId("v5-method-category-routing")).toContainText("方法覆盖");
    await expect(page.getByTestId("v5-method-category-routing")).not.toHaveClass(/focused/);
  }

  const visibleSavedCards = await page.locator(".v5-saved-method-card").count();
  expect(visibleSavedCards).toBeLessThanOrEqual(5);
  const showAll = page.getByRole("button", { name: "查看全部方法" });
  if (await showAll.isVisible()) {
    await showAll.click();
    await expect(page.getByRole("button", { name: "收起" })).toBeVisible();
  }

  const sidebarPosition = await page.getByTestId("v5-method-summary-sidebar").evaluate((element) => getComputedStyle(element).position);
  expect(sidebarPosition).toBe("sticky");
});
