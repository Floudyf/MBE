import { expect, test } from "@playwright/test";

import type { V5FormalChildRun, V5WorkloadDatasetSummary } from "../src/api";
import { buildThetaSweepPoints, buildThetaTpsSeries, thetaOptionsForDataset } from "../src/v5SkewExperiment";

const thetaValues = Array.from({ length: 13 }, (_, index) => index / 10);

const datasets = [{
  dataset_id: "alien",
  variant_definitions: [{
    variant_mode: "validated_prefix",
    kind: "derived",
    parameters: [{ name: "target_theta", options: thetaValues }],
  }],
}] as unknown as V5WorkloadDatasetSummary[];

function child(methodId: string, theta: number, tps: number, eligible = true): V5FormalChildRun {
  return {
    child_run_id: `${methodId}-${theta}-${tps}`,
    run_group_id: "group",
    suite_type: "workload_sensitivity",
    method: { method_id: methodId, display_name: methodId, plugin_overrides: {} },
    method_config_id: methodId,
    workload_point: { tx_count: 1000, target_theta: theta },
    topology_point: {},
    fault_point: {},
    seed: 11,
    repeat_index: 0,
    scan_variable: "target_theta+tx_count",
    scan_value: "",
    execution_backend: "real_cluster",
    estimated_processes: 8,
    estimated_transactions: 1000,
    runnable: true,
    blockers: [],
    warnings: [],
    status: "completed",
    execution_status: "completed",
    formal_eligibility: eligible,
    metrics: { end_to_end_tps: tps },
  };
}

test("theta main preset expands manifest options into independent child points", () => {
  const options = thetaOptionsForDataset(datasets, "alien", "validated_prefix");
  expect(options).toEqual(thetaValues);
  const oneK = buildThetaSweepPoints(1000, options);
  expect(oneK).toHaveLength(13);
  expect(oneK.reduce((sum, point) => sum + point.tx_count, 0)).toBe(13000);
  expect(oneK[0]).toEqual({ tx_count: 1000, target_theta: 0 });
  expect(oneK[12]).toEqual({ tx_count: 1000, target_theta: 1.2 });
  const tenK = buildThetaSweepPoints(10000, options);
  expect(tenK.reduce((sum, point) => sum + point.tx_count, 0)).toBe(130000);
});

test("theta TPS series averages repeats and excludes invalid child results", () => {
  const series = buildThetaTpsSeries([
    child("metatrack_serial", 0, 100),
    child("metatrack_serial", 0, 120),
    child("metatrack_serial", 0.1, 90),
    child("hash_block_stm", 0, 80),
    child("hash_block_stm", 0.1, 999, false),
  ]);
  const meta = series.find((item) => item.methodId === "metatrack_serial");
  expect(meta?.points).toEqual([
    { theta: 0, tps: 110, sampleCount: 2 },
    { theta: 0.1, tps: 90, sampleCount: 1 },
  ]);
  const block = series.find((item) => item.methodId === "hash_block_stm");
  expect(block?.points).toEqual([{ theta: 0, tps: 80, sampleCount: 1 }]);
});
