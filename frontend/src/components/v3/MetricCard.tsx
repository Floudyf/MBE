type Props = {
  label: string;
  value: unknown;
  hint?: string;
};

export default function MetricCard({ label, value, hint }: Props) {
  return (
    <div className="metric-card">
      <span>{label}</span>
      <strong>{formatValue(value)}</strong>
      {hint && <small>{hint}</small>}
    </div>
  );
}

export function formatValue(value: unknown): string {
  if (value === undefined || value === null || value === "") return "暂无数据";
  if (typeof value === "boolean") return value ? "是" : "否";
  if (typeof value === "number") return Number.isInteger(value) ? String(value) : value.toFixed(2);
  const numeric = Number(value);
  if (Number.isFinite(numeric) && String(value).trim() !== "") {
    return Number.isInteger(numeric) ? String(numeric) : numeric.toFixed(2);
  }
  return String(value);
}
