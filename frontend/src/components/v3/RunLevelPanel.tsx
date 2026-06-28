import type { ComposerDraft } from "./composerDraft";

type Props = {
  runnable: boolean;
  running: boolean;
  onRunSmoke: () => void;
  draft?: ComposerDraft | null;
};

export default function RunLevelPanel({ runnable, running, onRunSmoke, draft }: Props) {
  return (
    <section className="final-card v3-run-compact">
      <p className="eyebrow">运行入口</p>
      <h3>当前支持</h3>
      <ul className="v3-run-list">
        <li><span className="status-dot ok" />运行已有 Smoke 实验</li>
        <li><span className="status-dot planned" />运行当前 Composer Draft，后续支持</li>
      </ul>
      <div className="v3-run-buttons">
        <button type="button" disabled={!runnable || running} onClick={onRunSmoke}>
          {running ? "Smoke 运行中..." : "运行当前 Smoke 实验"}
        </button>
        <button type="button" disabled className="v3-secondary-button">
          运行当前 Draft，后续支持
        </button>
      </div>
      {draft && (
        <p className={draft.hasValidationErrors ? "file-error" : "muted"}>
          {draft.hasValidationErrors ? "Draft 仍有校验问题；不会进入运行。" : "Draft 可预览；本轮不运行自定义 Draft。"}
        </p>
      )}
    </section>
  );
}
