type Props = {
  templateName: string;
  validationStatus: string;
  dirtyState: string;
  quickValidating: boolean;
  savingDisabled: boolean;
  runnableSaveDisabled: boolean;
  onQuickValidate: () => void;
  onSaveDraft: () => void;
  onSaveRunnable: () => void;
  onNext: () => void;
};

export default function ComposerActionBar({
  templateName,
  validationStatus,
  dirtyState,
  quickValidating,
  savingDisabled,
  runnableSaveDisabled,
  onQuickValidate,
  onSaveDraft,
  onSaveRunnable,
  onNext,
}: Props) {
  return (
    <section className="composer-action-bar">
      <div>
        <strong>{templateName || "Untitled method template"}</strong>
        <small>{validationStatus} / {dirtyState}</small>
      </div>
      <div className="button-row">
        <button type="button" onClick={onQuickValidate} disabled={quickValidating}>{quickValidating ? "验证中..." : "快速验证当前模板"}</button>
        <button type="button" className="v3-secondary-button" onClick={onSaveDraft} disabled={savingDisabled}>保存草稿模板</button>
        <button type="button" className="v3-secondary-button" onClick={onSaveRunnable} disabled={runnableSaveDisabled}>保存为可运行模板</button>
        <button type="button" onClick={onNext}>下一步：运行实验</button>
      </div>
    </section>
  );
}
