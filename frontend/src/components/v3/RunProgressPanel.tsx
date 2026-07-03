export type RunProgressMode = "idle" | "draft" | "controlled" | "success" | "error";

type Props = {
  mode: RunProgressMode;
  activeStep?: number;
  error?: string;
};

const draftSteps = ["校验配置", "生成实验配置", "提交运行", "等待本地 runtime", "读取 summary", "收集产物", "渲染结果"];
const controlledSteps = ["提交受控对照", "执行预设组合", "聚合 summary", "收集产物", "渲染结果"];

export default function RunProgressPanel({ mode, activeStep = 0, error = "" }: Props) {
  if (mode === "idle") return null;
  const steps = mode === "controlled" ? controlledSteps : draftSteps;
  const done = mode === "success";
  const failed = mode === "error";
  const index = done ? steps.length : Math.min(activeStep, steps.length - 1);

  return (
    <section className={`run-progress-panel ${failed ? "failed" : done ? "done" : ""}`}>
      <div className="v3-section-head">
        <div>
          <p className="eyebrow">运行进度</p>
          <h3>{mode === "controlled" ? "受控对照试运行" : "配置草稿试运行"}</h3>
        </div>
        <span>{failed ? "失败" : done ? "完成" : "进行中"}</span>
      </div>
      <ol>
        {steps.map((step, stepIndex) => (
          <li key={step} className={stepIndex < index || done ? "complete" : stepIndex === index ? "active" : ""}>
            <i>{stepIndex < index || done ? "✓" : stepIndex + 1}</i>
            <span>{step}</span>
          </li>
        ))}
      </ol>
      {failed && error && <p className="file-error">{error}</p>}
      {!failed && <p className="muted">这是前端已知阶段提示，不代表后端实时流式进度。</p>}
    </section>
  );
}
