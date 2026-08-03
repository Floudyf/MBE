import { expect, test } from "@playwright/test";

test("protects unsaved V5 method changes before entering Run", async ({ page }) => {
  await page.goto("/");
  await expect(page.getByTestId("v5-method-category-routing").locator("select")).toContainText("MetaTrack");
  await page.getByTestId("v5-method-name").fill("unsaved method");
  await page.getByTestId("v5-method-category-routing").locator("select").selectOption("metatrack_coaccess_routing");
  await page.getByTestId("primary-navigation").getByRole("button").nth(1).click();
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("button")).toHaveCount(3);
  await dialog.getByRole("button").nth(2).click();
  await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
  await page.getByTestId("primary-navigation").getByRole("button").nth(1).click();
  await dialog.getByRole("button").nth(1).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("Baseline");
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("Block-STM");
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("MetaTrack");
  await expect(page.getByTestId("v5-run-preferred-method")).toContainText("metatrack_block_stm");
  await expect(page.getByTestId("v5-run-method-hash_serial").getByRole("checkbox")).toBeChecked();
  await expect(page.getByTestId("v5-run-method-hash_block_stm").getByRole("checkbox")).toBeChecked();
  await expect(page.getByTestId("v5-run-method-metatrack_serial").getByRole("checkbox")).toBeChecked();
  await expect(page.getByTestId("v5-run-method-metatrack_block_stm").getByRole("checkbox")).toBeChecked();
});

test("consumes a save-and-run navigation command only once", async ({ page, request }) => {
  let createCount = 0;
  page.on("request", (entry) => { if (entry.url().includes("/api/v3/saved-configs") && entry.method() === "POST") createCount += 1; });
  let configId = "";
  try {
    await page.goto("/");
    await expect(page.getByTestId("v5-method-category-routing").locator("select")).toContainText("MetaTrack");
    await page.getByTestId("v5-method-name").fill(`stale save ${Date.now()}`);
    const validation = page.waitForResponse((response) => response.url().includes("/api/v5/experiment-spec/validate") && response.request().method() === "POST");
    await page.getByTestId("v5-method-validate").click();
    expect((await validation).ok()).toBeTruthy();
    await page.getByTestId("v5-method-description").fill("validated design handoff");
    await page.getByTestId("primary-navigation").getByRole("button").nth(1).click();
    const dialog = page.getByRole("dialog");
    const saved = page.waitForResponse((response) => response.url().includes("/api/v3/saved-configs") && response.request().method() === "POST");
    await dialog.getByRole("button").nth(0).click();
    const payload = await (await saved).json();
    configId = payload.config_id;
    await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
    await page.getByTestId("primary-navigation").getByRole("button").nth(0).click();
    await expect(page.getByTestId("v5-method-design-page")).toBeVisible();
    await page.waitForTimeout(300);
    expect(createCount).toBe(1);
  } finally {
    if (configId) await request.delete(`/api/v3/saved-configs/${configId}`);
  }
});
