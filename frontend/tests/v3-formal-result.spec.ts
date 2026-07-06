import { expect, test } from "@playwright/test";
import {
  collectBrowserDiagnostics,
  expectResultDashboardOrFailureDiagnostics,
  openV3Console,
} from "./helpers/v3FormalHelpers";

test("opens the latest formal result from history", async ({ page }) => {
  const diagnostics = collectBrowserDiagnostics(page);
  await openV3Console(page);

  const history = page.getByTestId("v3-formal-run-history");
  await expect(history).toBeVisible({ timeout: 30_000 });

  const enabledOpenButtons = page.locator('[data-testid="v3-formal-history-open-result"]:not([disabled])');
  await expect.poll(async () => enabledOpenButtons.count(), { timeout: 30_000 }).toBeGreaterThan(0);

  await enabledOpenButtons.first().click();
  const result = await expectResultDashboardOrFailureDiagnostics(page);

  await expect(history.locator("strong").getByText(result.runId, { exact: true })).toBeVisible();
  expect(diagnostics.errors).toEqual([]);
});
