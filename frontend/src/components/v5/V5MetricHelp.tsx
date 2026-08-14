export default function V5MetricHelp({ text }: { text: string }) {
  return <span className="v5-metric-help" title={text} aria-label={text}>ⓘ</span>;
}
