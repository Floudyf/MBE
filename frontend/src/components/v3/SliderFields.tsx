type SliderNumberProps = {
  label: string;
  value: number;
  min: number;
  max: number;
  step?: number;
  helper?: string;
  onChange: (value: number) => void;
};

export function SliderNumberField({ label, value, min, max, step = 1, helper, onChange }: SliderNumberProps) {
  const normalized = clamp(value, min, max);
  return (
    <label className="field-card slider-field">
      <span>{label}</span>
      <input type="range" min={min} max={max} step={step} value={normalized} onChange={(event) => onChange(clamp(Number(event.target.value), min, max))} />
      <input type="number" min={min} max={max} step={step} value={normalized} onChange={(event) => onChange(clamp(Number(event.target.value), min, max))} />
      <small>{helper || `${min} - ${max}`}</small>
    </label>
  );
}

export function IntegerSliderField(props: SliderNumberProps) {
  return <SliderNumberField {...props} step={props.step || 1} onChange={(value) => props.onChange(Math.trunc(value))} />;
}

export function RatioSliderField({ label, value, helper, onChange }: { label: string; value: number; helper?: string; onChange: (value: number) => void }) {
  const normalized = normalizeRatio(value);
  return (
    <label className="field-card slider-field">
      <span>{label}</span>
      <input type="range" min={0} max={1} step={0.01} value={normalized} onChange={(event) => onChange(normalizeRatio(Number(event.target.value)))} />
      <input type="number" min={0} max={1} step={0.01} value={normalized} onChange={(event) => onChange(normalizeRatio(Number(event.target.value)))} />
      <small>{Math.round(normalized * 100)}% · {helper || "比例参数使用 0-1，也可输入 80 表示 80%。"}</small>
    </label>
  );
}

export function PresetChipGroup({ label, items, onSelect }: { label: string; items: { id: string; label: string }[]; onSelect: (id: string) => void }) {
  return (
    <div className="field-card">
      <span>{label}</span>
      <div className="chip-row">
        {items.map((item) => <button type="button" key={item.id} className="preset-chip" onClick={() => onSelect(item.id)}>{item.label}</button>)}
      </div>
    </div>
  );
}

export function MultiSelectChipGroup({ label, options, selected, onChange }: { label: string; options: { id: string; label: string }[]; selected: string[]; onChange: (values: string[]) => void }) {
  function toggle(id: string) {
    onChange(selected.includes(id) ? selected.filter((item) => item !== id) : [...selected, id]);
  }
  return (
    <div className="field-card">
      <span>{label}</span>
      <div className="chip-row">
        {options.map((option) => (
          <button type="button" key={option.id} className={`preset-chip ${selected.includes(option.id) ? "selected" : ""}`} onClick={() => toggle(option.id)}>
            {option.label}
          </button>
        ))}
      </div>
    </div>
  );
}

function normalizeRatio(value: number): number {
  if (!Number.isFinite(value)) return 0;
  const decimal = value > 1 ? value / 100 : value;
  return clamp(Number(decimal.toFixed(4)), 0, 1);
}

function clamp(value: number, min: number, max: number): number {
  if (!Number.isFinite(value)) return min;
  return Math.min(max, Math.max(min, value));
}
