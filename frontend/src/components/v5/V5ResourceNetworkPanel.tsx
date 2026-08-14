// MBE_V5_RESULTS_UI_TRUTH_CN_FINAL_20260814_V5
import type { V5FormalChildRun } from "../../api";
import V5MetricHelp from "./V5MetricHelp";

type Row = {
  methodId: string;
  methodName: string;
  sampleCount: number;
  cpuCores: number | null;
  peakRSS: number | null;
  meanRSS: number | null;
  networkBytes: number | null;
  messages: number | null;
  messagesPerTx: number | null;
  bytesPerTx: number | null;
  categories: Record<string, { message_count?: number; bytes?: number }>;
};

const categoryLabels: Record<string, string> = {
  client_ingress: "客户端入口",
  transaction_gossip: "交易传播（Gossip）",
  consensus: "共识消息",
  cross_shard: "跨分片",
  remote_state: "远程状态",
  recovery_control: "恢复与控制",
  other: "其他",
};

export default function V5ResourceNetworkPanel({ children }: { children: V5FormalChildRun[] }) {
  const rows = aggregate(children);
  const hasAny = rows.some((row) => [row.cpuCores, row.peakRSS, row.networkBytes, row.messages].some((value) => value !== null));
  return <section className="v5-dashboard-section" data-testid="v5-resource-network-panel">
    <div className="v5-dashboard-heading">
      <div>
        <h3>资源与通信</h3>
        <p className="muted">CPU/RSS 只统计真实验证节点操作系统进程；网络只统计正式完成窗口内成功接收的应用层 TCP JSON 信封。</p>
      </div>
    </div>
    {!hasAny && <div className="notice"><strong>当前记录没有资源或通信汇总。</strong><span>历史实验组不会补造 CPU/RSS；安装观测闭合包后产生的新子实验才会生成资源与网络观测产物。</span></div>}
    <div className="v5-observability-kpis">
      <MetricCard label="平均验证节点集群 CPU" value={formatMean(rows.map((row) => row.cpuCores), " 核")} help="全部验证节点的 CPU 时间增量 ÷ 正式完成窗口墙钟时间，结果表示平均占用的 CPU 核数。" />
      <MetricCard label="验证节点集群峰值 RSS" value={formatBytes(maxValue(rows.map((row) => row.peakRSS)))} help="每个采样时刻先汇总全部验证节点 RSS，再取其中最大值；不会把各节点在不同时刻的独立峰值相加。" />
      <MetricCard label="平均网络流量 / 方法" value={formatBytes(mean(rows.map((row) => row.networkBytes)))} help="对各方法已完成有效子实验的成功接收网络字节取均值；正式横向数值见下表。" />
      <MetricCard label="每终态交易消息数" value={formatNumber(formatMeanNumber(rows.map((row) => row.messagesPerTx)), 3)} help="成功送达消息数 ÷ 终态逻辑交易数。ACG/Nezha 的合法 HS 中止同样属于终态结果。" />
    </div>
    <ResourceComparisonCharts rows={rows} />
    <div className="table-wrap">
      <table className="v5-observability-table">
        <thead><tr><th>方法</th><th>样本数</th><th>平均 CPU 核</th><th>峰值 RSS</th><th>平均 RSS</th><th>网络流量</th><th>消息数</th><th>消息/终态交易</th><th>KiB/终态交易</th></tr></thead>
        <tbody>{rows.map((row) => <tr key={row.methodId}>
          <td>{row.methodName}</td><td>{row.sampleCount}</td><td>{formatNumber(row.cpuCores, 3)}</td><td>{formatBytes(row.peakRSS)}</td><td>{formatBytes(row.meanRSS)}</td><td>{formatBytes(row.networkBytes)}</td><td>{formatNumber(row.messages, 0)}</td><td>{formatNumber(row.messagesPerTx, 3)}</td><td>{row.bytesPerTx === null ? "—" : formatNumber(row.bytesPerTx / 1024, 3)}</td>
        </tr>)}</tbody>
      </table>
    </div>
    <h4>网络流量构成</h4>
    <div className="table-wrap"><table className="v5-network-composition-table">
      <thead><tr><th>方法</th>{Object.keys(categoryLabels).map((key) => <th key={key}>{categoryLabels[key]}</th>)}</tr></thead>
      <tbody>{rows.map((row) => <tr key={`network-${row.methodId}`}><td>{row.methodName}</td>{Object.keys(categoryLabels).map((key) => <td key={key}>{formatBytes(row.categories[key]?.bytes ?? null)}</td>)}</tr>)}</tbody>
    </table></div>
  </section>;
}

function ResourceComparisonCharts({ rows }: { rows: Row[] }) {
  const definitions = [
    { label: "平均 CPU 核数", get: (row: Row) => row.cpuCores, format: (value: number | null) => formatNumber(value, 2) },
    { label: "集群峰值 RSS", get: (row: Row) => row.peakRSS, format: (value: number | null) => formatBytes(value) },
    { label: "成功接收网络流量", get: (row: Row) => row.networkBytes, format: (value: number | null) => formatBytes(value) },
    { label: "消息/终态交易", get: (row: Row) => row.messagesPerTx, format: (value: number | null) => formatNumber(value, 3) },
  ];
  return <div className="v5-resource-chart-grid">{definitions.map((definition) => {
    const maximum = Math.max(1, ...rows.map(definition.get).filter((value): value is number => value !== null));
    return <section className="v5-mini-resource-chart" key={definition.label}><strong>{definition.label}</strong><div className="v5-horizontal-bars">{rows.map((row) => { const value = definition.get(row); return <div className="v5-horizontal-bar-row" key={`${definition.label}-${row.methodId}`}><span>{row.methodName}</span><div><i style={{ width: value === null ? "0%" : `${Math.max(1.5, (value / maximum) * 100)}%` }} /></div><strong>{definition.format(value)}</strong></div>; })}</div></section>;
  })}</div>;
}

function MetricCard({ label, value, help }: { label: string; value: string; help: string }) {
  return <div className="v5-observability-card"><span>{label} <V5MetricHelp text={help} /></span><strong>{value}</strong></div>;
}

function aggregate(children: V5FormalChildRun[]): Row[] {
  const buckets = new Map<string, V5FormalChildRun[]>();
  for (const child of children) {
    if (child.status !== "completed" || child.individual_result_valid === false || child.paper_candidate === false) continue;
    const key = child.method_config_id || child.method?.method_id || "unknown";
    buckets.set(key, [...(buckets.get(key) ?? []), child]);
  }
  return [...buckets.entries()].map(([methodId, items]) => {
    const metrics = items.map(childMetrics);
    const categories = mergeCategories(metrics.map((item) => asRecord(item.network_categories)));
    return {
      methodId,
      methodName: shortMethodName(methodId, items[0]?.method?.display_name ?? methodId),
      sampleCount: items.length,
      cpuCores: mean(metrics.map((item) => number(item.average_cluster_cpu_cores))),
      peakRSS: mean(metrics.map((item) => number(item.cluster_rss_peak_bytes))),
      meanRSS: mean(metrics.map((item) => number(item.cluster_rss_mean_bytes))),
      networkBytes: mean(metrics.map((item) => number(item.delivered_network_bytes))),
      messages: mean(metrics.map((item) => number(item.delivered_network_message_count))),
      messagesPerTx: mean(metrics.map((item) => number(item.network_messages_per_terminal_tx))),
      bytesPerTx: mean(metrics.map((item) => number(item.network_bytes_per_terminal_tx))),
      categories,
    };
  }).sort((a, b) => a.methodName.localeCompare(b.methodName));
}

function childMetrics(child: V5FormalChildRun): Record<string, unknown> {
  return asRecord(child.metrics);
}

function mergeCategories(values: Array<Record<string, unknown>>): Record<string, { message_count?: number; bytes?: number }> {
  const out: Record<string, { message_count?: number; bytes?: number }> = {};
  for (const key of Object.keys(categoryLabels)) {
    const rows = values.map((value) => asRecord(value[key]));
    out[key] = { message_count: mean(rows.map((row) => number(row.message_count))) ?? undefined, bytes: mean(rows.map((row) => number(row.bytes))) ?? undefined };
  }
  return out;
}

function asRecord(value: unknown): Record<string, unknown> { return value && typeof value === "object" && !Array.isArray(value) ? value as Record<string, unknown> : {}; }
function number(value: unknown): number | null { const parsed = Number(value); return value === null || value === undefined || value === "" || !Number.isFinite(parsed) ? null : parsed; }
function mean(values: Array<number | null>): number | null { const valid = values.filter((value): value is number => value !== null); return valid.length ? valid.reduce((sum, value) => sum + value, 0) / valid.length : null; }
function formatMean(values: Array<number | null>, suffix: string): string { const value = mean(values); return value === null ? "—" : `${value.toFixed(2)}${suffix}`; }
function formatMeanNumber(values: Array<number | null>): number | null { return mean(values); }
function maxValue(values: Array<number | null>): number | null { const valid = values.filter((value): value is number => value !== null); return valid.length ? Math.max(...valid) : null; }
function formatNumber(value: number | null, digits = 2): string { return value === null ? "—" : value.toLocaleString(undefined, { maximumFractionDigits: digits }); }
function formatBytes(value: number | null): string { if (value === null) return "—"; const units = ["B", "KiB", "MiB", "GiB"]; let current = value; let index = 0; while (current >= 1024 && index < units.length - 1) { current /= 1024; index += 1; } return `${current.toFixed(index === 0 ? 0 : 2)} ${units[index]}`; }

function shortMethodName(methodId: string, value: string): string {
  const id = methodId.toLowerCase();
  if (id === "hash_serial") return "Serial";
  if (id === "hash_block_stm") return "Block-STM";
  if (id === "hash_aria") return "Aria";
  if (id === "hash_groundhog") return "Groundhog";
  if (id === "hash_cg") return "CG";
  if (id === "hash_acg") return "ACG/Nezha";
  if (id === "hash_bsx") return "BSX";
  if (id === "hash_batch_si") return "Batch-SI";
  const lower = value.toLowerCase();
  if (lower.includes("address conflict graph")) return "ACG/Nezha";
  if (lower.includes("batch-schedule-execute")) return "BSX";
  if (lower.includes("conflict graph") && !lower.includes("address")) return "CG";
  if (lower.includes("block-stm")) return "Block-STM";
  if (lower.includes("batch-si")) return "Batch-SI";
  if (lower.includes("groundhog")) return "Groundhog";
  if (lower.includes("aria")) return "Aria";
  if (lower.includes("serial")) return "Serial";
  return value.replace(/Stateful Hash/gi, "有状态 Hash").replace(/Stateless Hash/gi, "无状态 Hash").replace(/with Block-STM backend/gi, "+ Block-STM");
}
