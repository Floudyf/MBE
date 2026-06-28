import type { V3DraftValidationResponse } from "../../api";
import type { ComposerDraft } from "./composerDraft";

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
      <h3>当前支持</h3>
      <ul className="v3-run-list">
        <li><span className="status-dot ok" />运行已有 Smoke 实验：内置 MetaTrack 四组对比</li>
        <li><span className={draftRunnable ? "status-dot ok" : "status-dot planned"} />运行当前 Draft Smoke：只运行当前单组配置</li>
      </ul>
      <div className="v3-run-buttons">
        <button type="button" disabled={!runnable || running} onClick={onRunSmoke}>
          {running ? "Smoke 运行中..." : "运行已有 Smoke 实验"}
        </button>
        <button type="button" className="v3-secondary-button" disabled={validatingDraft || runningDraft || !draft} onClick={onValidateDraft}>
          {validatingDraft ? "校验中..." : "校验当前 Draft"}
        </button>
        <button type="button" className="v3-secondary-button" disabled={!draftRunnable || runningDraft || validatingDraft} onClick={onRunDraftSmoke}>
          {runningDraft ? "Draft Smoke 运行中..." : "运行当前 Draft Smoke"}
        </button>
      </div>
      {draft && (
        <p className={draft.hasValidationErrors ? "file-error" : "muted"}>
          本地 Draft 校验：{draft.hasValidationErrors ? "仍有问题，请先调整模块配置。" : "可预览。"}
        </p>
      )}
      <div className="v3-backend-validation">
        <strong>后端 Draft 校验：</strong>
        {!backendValidation && <span className="muted">尚未校验，运行 Draft Smoke 前需要后端权威校验。</span>}
        {backendValidation && (
          <span className={backendValidation.is_runnable ? "v3-inline-ok" : "file-error"}>
            {backendValidation.is_runnable ? "可运行当前 Draft Smoke。" : backendValidation.is_valid ? "可预览，但不可运行。" : "校验未通过。"}
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
