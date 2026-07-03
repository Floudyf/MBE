import { formatValue } from "./MetricCard";

export type MiniBarDatum = {
  label: string;
  value: unknown;
};

type Props = {
  title: string;
  data: MiniBarDatum[];
};

export default function MiniBarChart({ title, data }: Props) {
  const numeric = data
    .map((item) => ({ ...item, numberValue: Number(item.value) }))
    .filter((item) => Number.isFinite(item.numberValue) && item.numberValue >= 0);
  const max = Math.max(0, ...numeric.map((item) => item.numberValue));

  return (
    <div className="mini-chart">
      <h4>{title}</h4>
      {numeric.length === 0 || max === 0 ? (
        <p className="muted">暂无数据</p>
      ) : (
        <div className="mini-chart-bars">
          {numeric.map((item) => (
            <div key={item.label} className="mini-chart-row">
              <span>{item.label}</span>
              <div className="mini-chart-track">
                <i style={{ width: `${Math.max(4, (item.numberValue / max) * 100)}%` }} />
              </div>
              <b>{formatValue(item.numberValue)}</b>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
