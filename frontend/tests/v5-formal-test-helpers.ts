import { expect, type APIRequestContext, type Page } from "@playwright/test";

type Plugin = { category: string; plugin_id: string; default_config: Record<string, unknown> };

export async function createRunnableMethod(request: APIRequestContext, name: string) {
  const catalogResponse = await request.get("/api/v5/plugins?backend=real_cluster");
  expect(catalogResponse.ok()).toBeTruthy();
  const catalog = (await catalogResponse.json()).items as Plugin[];
  const selections = Array.from(new Map(catalog.map((plugin) => [plugin.category, plugin])).values()).map((plugin) => ({ category: plugin.category, plugin_id: plugin.plugin_id, config: { ...plugin.default_config } }));
  const workload = selections.find((item) => item.category === "workload")!;
  workload.plugin_id = "deterministic_signed_synthetic";
  workload.config = { ...workload.config, cross_shard_ratio: 0, timeout_every: 0 };
  const overrides: Record<string, string> = { routing: "metatrack_coaccess_routing", execution: "dual_track_execution", scheduler: "fast_first_scheduler", commit: "commutative_hot_update_aggregation" };
  for (const selection of selections) {
    const pluginId = overrides[selection.category];
    if (!pluginId) continue;
    const plugin = catalog.find((item) => item.category === selection.category && item.plugin_id === pluginId);
    if (!plugin) throw new Error(`missing required test plugin ${selection.category}/${pluginId}`);
    selection.plugin_id = plugin.plugin_id; selection.config = { ...plugin.default_config };
  }
  const validation = await request.post("/api/v5/experiment-spec/validate", { data: { name: "e2e_method_validation", execution_backend: "real_cluster", plugin_selections: selections, topology: { nodes: 4, shards: 1, validators_per_shard: 4 }, tx_count: 20, seed: 11, duration_ms: 6000, fault_policy: { mode: "disabled" }, requested_metrics: [] } });
  expect(validation.ok()).toBeTruthy();
  const compatibility = await validation.json();
  expect(compatibility.valid).toBe(true);
  const profileSelections = selections.filter((item) => !["workload", "fault_injection"].includes(item.category));
  const created = await request.post("/api/v3/saved-configs", { data: { config_kind: "method", name, owner_label: "e2e", tags: ["e2e", "custom"], validation_status: "runnable", last_validation: compatibility, payload: { schema_version: "v5_plugin_profile_v1", plugin_selections: profileSelections, plugin_parameters: profileSelections, compatibility_snapshot: compatibility, source_composer_draft: { source: "e2e", role: "custom" } } } });
  expect(created.ok()).toBeTruthy();
  return await created.json() as { config_id: string; name: string };
}

export async function deleteMethod(request: APIRequestContext, configId: string) { if (configId) await request.delete(`/api/v3/saved-configs/${configId}`); }

export async function openRunWithMethods(page: Page, methodId: string) {
  await page.goto("/");
  await page.getByTestId("primary-navigation").getByRole("button", { name: "② 运行实验" }).click();
  await expect(page.getByTestId("v5-formal-run-page")).toBeVisible();
  await page.getByTestId("v5-run-method-v5_catalog_default").getByRole("checkbox").check();
  await page.getByTestId(`v5-run-method-${methodId}`).getByRole("checkbox").check();
}

export async function selectOnlySuite(page: Page, suite: string) {
  const main = page.getByTestId("v5-suite-main_experiment").getByRole("checkbox");
  if (await main.isChecked()) await main.uncheck();
  const control = page.getByTestId(`v5-suite-${suite}`).getByRole("checkbox");
  if (!(await control.isChecked())) await control.check();
}
