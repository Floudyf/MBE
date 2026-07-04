import type { V3DraftValidationResponse } from "../../api";
import type { ComposerDraft } from "./composerDraft";
import HelpTip from "./HelpTip";

type Props = {
  runnable: boolean;
  running: boolean;
  onRunSmoke: () => void;
  draft?: ComposerDraft | null;
  backendValidation?: V3DraftValidationResponse | null;
  validatingDraft?: boolean;
  runningDraft?: boolean;
  draftError?: string;
  onValidateDraft?: () => void;
  onRunDraftSmoke?: () => void;
};

export default function RunLevelPanel({
  runnable,
  running,
  onRunSmoke,
  draft,
  backendValidation,
  validatingDraft = false,
  runningDraft = false,
  draftError = "",
  onValidateDraft,
  onRunDraftSmoke,
}: Props) {
  const draftRunnable = Boolean(backendValidation?.is_runnable);
  const backendMessages = [
    ...(backendValidation?.errors || []),
    ...(backendValidation?.warnings || []),
  ].slice(0, 4);

  return (
    <section className="final-card v3-run-compact">
      <p className="eyebrow">运行入口</p>
      <h3>本地快速验证 <HelpTip title="快速验证">来自软件工程 smoke test，表示受限交易数的链路试运行，用于确认配置、运行链路、summary 和 artifacts 是否正常，不代表论文级正式实验。</HelpTip></h3>
      <ul className="v3-run-list">
        <li><span className="status-dot ok" />内置快速验证：使用已有 MetaTrack 组合检查运行链路</li>
        <li><span className={draftRunnable ? "status-dot ok" : "status-dot planned"} />配置草稿试运行：只运行当前单组配置</li>
      </ul>
      <div className="v3-warning-card">
        当前运行入口用于快速验证配置链路和 artifacts 生成。它不会按 metaverse_tx_count 执行完整 runtime 压测。
      </div>
      <div className="v3-run-buttons">
        <button type="button" disabled={!runnable || running} onClick={onRunSmoke}>
          {running ? "内置快速验证运行中..." : "运行内置快速验证"}
        </button>
        <button type="button" className="v3-secondary-button" disabled={validatingDraft || runningDraft || !draft} onClick={onValidateDraft}>
          {validatingDraft ? "校验中..." : "校验当前草稿"}
        </button>
        <button type="button" className="v3-secondary-button" disabled={!draftRunnable || runningDraft || validatingDraft} onClick={onRunDraftSmoke}>
          {runningDraft ? "Draft Smoke 运行中..." : "运行当前配置快速验证"}
        </button>
      </div>
      {draft && (
        <p className={draft.hasValidationErrors ? "file-error" : "muted"}>
          本地草稿校验：{draft.hasValidationErrors ? "仍有问题，请先调整模块配置。" : "可预览。"}
        </p>
      )}
      <div className="v3-backend-validation">
        <strong>后端权威校验：</strong>
        {!backendValidation && <span className="muted">尚未校验；运行配置草稿试运行前需要后端校验。</span>}
        {backendValidation && (
          <span className={backendValidation.is_runnable ? "v3-inline-ok" : "file-error"}>
            {backendValidation.is_runnable ? "当前草稿可运行。" : backendValidation.is_valid ? "可预览，但不可运行。" : "校验未通过。"}
          </span>
        )}
      </div>
      {backendMessages.length > 0 && (
        <ul className="v3-check-list compact">
          {backendMessages.map((message) => <li key={message}>{message}</li>)}
        </ul>
      )}
      {draftError && <p className="file-error">{draftError}</p>}
    </section>
  );
}
