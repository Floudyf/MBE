from __future__ import annotations

from typing import Any

from backend.app.models.v3_composer_draft import V3ComposerDraftRequest, V3DraftValidationResponse
from backend.app.services.v3_composer_catalog import (
    CATALOG,
    GO_RUNTIME_PLUGIN_CLASSES,
    METATRACK_FIXED_MODULES,
    METATRACK_VARIABLE_MODULES,
    OUTPUT_MODULES,
    REQUIRED_MODULES,
    module_label,
    plugin_owner,
)
from backend.app.services.v3_experiment_templates import get_template


def model_dump(model: Any) -> dict[str, Any]:
    if hasattr(model, "model_dump"):
        return model.model_dump()
    return model.dict()


def validate_v3_composer_draft(request: V3ComposerDraftRequest) -> V3DraftValidationResponse:
    errors: list[str] = []
    warnings: list[str] = []
    fixed_modules: list[str] = []
    variable_modules: list[str] = []
    disabled_modules: list[str] = []
    planned_modules: list[str] = []
    output_modules: list[str] = []
    normalized_modules: dict[str, dict[str, Any]] = {}
    plugin_selection: dict[str, str] = {}
    has_preview_only = False
    template: dict[str, Any] = {}

    try:
        template = get_template(request.template_id)
    except (KeyError, ValueError):
        errors.append(f"未知模板 {request.template_id}，后端无法校验 Draft。")
    template_id = str(template.get("template_id") or request.template_id or "template_unset")
    template_variable_module = str(template.get("variable_module") or "")
    template_allowed_plugins = set(str(item) for item in template.get("allowed_variable_plugins", []))
    template_locked_modules = {
        str(key): str(value)
        for key, value in (template.get("locked_modules") or template.get("fairness", {}).get("locked_modules") or {}).items()
    }
    presets = [preset for preset in template.get("presets", []) if isinstance(preset, dict)]
    presets_by_id = {str(preset.get("preset_id", "")): preset for preset in presets if preset.get("preset_id")}
    default_preset_id = str(template.get("default_preset_id") or (presets[0].get("preset_id") if presets else "") or "")
    requested_preset_id = (request.preset_id or "").strip()
    preset_id = requested_preset_id or default_preset_id
    preset = presets_by_id.get(preset_id, {}) if preset_id else {}
    if requested_preset_id and requested_preset_id not in presets_by_id:
        errors.append(
            f"Invalid preset: {requested_preset_id} does not belong to {template_id}."
        )
    if preset and str(preset.get("variable_module", "")) and str(preset.get("variable_module", "")) != template_variable_module:
        errors.append(
            f"Invalid preset: {preset_id} belongs to {preset.get('variable_module')}, but template {template_id} uses {template_variable_module}."
        )
    is_single_module_template = template_id.startswith("single_module_") and bool(template_variable_module)

    if not request.modules:
        errors.append("Draft modules 不能为空。")

    for module_id in request.modules:
        if module_id not in CATALOG:
            errors.append(f"未知模块 {module_id} 当前不属于单链 Composer。")

    for module_id in REQUIRED_MODULES:
        if module_id not in request.modules:
            errors.append(f"缺少必需模块：{module_label(module_id)}。")

    for module_id, draft_module in request.modules.items():
        if module_id not in CATALOG:
            continue
        catalog_module = CATALOG[module_id]
        if draft_module.module_id != module_id:
            errors.append(f"{module_label(module_id)} 的 module_id 与 modules 字典键不一致。")

        status = draft_module.status
        plugin = (draft_module.plugin or "").strip()
        if not plugin:
            errors.append(f"{catalog_module.label} 必须选择插件。")
            plugin = catalog_module.default_plugin

        plugin_capability = catalog_module.plugins.get(plugin)
        if plugin_capability is None:
            owner = plugin_owner(plugin)
            if owner:
                errors.append(f"{plugin} 只能用于{module_label(owner)}模块，不能用于{catalog_module.label}。")
            else:
                errors.append(f"{catalog_module.label} 使用了未知插件 {plugin}。")
            plugin = catalog_module.default_plugin
            plugin_capability = catalog_module.plugins[plugin]

        if catalog_module.required and status == "disabled":
            errors.append(f"{catalog_module.label}是必需模块，不能关闭。")
        if status == "disabled" and not catalog_module.allow_disabled:
            errors.append(f"{catalog_module.label}不支持关闭。")
        if catalog_module.output_only and status != "output":
            if status == "variable":
                errors.append(f"{catalog_module.label}是输出模块，不能作为实验变量。")
            status = "output"
        if status == "variable" and is_single_module_template and module_id != template_variable_module:
            errors.append(
                fairness_error(
                    template_id,
                    template_variable_module,
                    module_id,
                    plugin_selection_key(module_id),
                    "fixed",
                    plugin,
                )
            )
        if status == "variable" and not catalog_module.allow_variable and not (is_single_module_template and module_id == template_variable_module):
            if module_id == "CommitteeEpoch":
                errors.append("委员会 / Epoch 当前不能作为可运行实验变量。")
            elif module_id in METATRACK_FIXED_MODULES:
                errors.append(f"当前模板 metatrack_ablation 中，{catalog_module.label}属于固定环境，不能作为实验变量。请切换到 consensus_only 模板。")
            else:
                errors.append(f"{catalog_module.label}当前不能作为实验变量。")
        if module_id == "CommitteeEpoch" and status == "variable":
            errors.append("委员会 / Epoch 当前不能作为可运行实验变量。")

        if plugin_capability.planned:
            planned_modules.append(module_id)
            errors.append(f"{plugin_capability.label} 当前为规划中插件，不能用于运行。")
        if plugin_capability.preview_only:
            has_preview_only = True
            warnings.append(f"{plugin_capability.label} 当前仅用于预览，后端尚未接入 Draft 运行。")

        if request.template_id == "metatrack_ablation" and status == "variable" and module_id not in METATRACK_VARIABLE_MODULES:
            if module_id == "Consensus":
                errors.append("当前模板 metatrack_ablation 中，共识排序属于固定环境，不能作为实验变量。请切换到 consensus_only 模板。")
            else:
                errors.append(f"当前模板 metatrack_ablation 中，{catalog_module.label}不能作为实验变量。")

        normalized_plugin = plugin
        if module_id == "StateStorage" and plugin == "memory_kv":
            normalized_plugin = "hash_state_storage"
            warnings.append("状态存储 memory_kv 已归一化为 Go runtime 支持的 hash_state_storage。")
        if module_id == "MetricsReport" and plugin == "metatrack_metrics":
            normalized_plugin = "basic_metrics"
            warnings.append("指标 / 报告 metatrack_metrics 已归一化为 Go runtime 当前支持的 basic_metrics。")

        if status == "default":
            fixed_modules.append(module_id)
        elif status == "fixed":
            fixed_modules.append(module_id)
        elif status == "variable":
            variable_modules.append(module_id)
        elif status == "disabled":
            disabled_modules.append(module_id)
        elif status == "planned":
            planned_modules.append(module_id)
        elif status == "output":
            output_modules.append(module_id)

        plugin_selection[module_id] = normalized_plugin
        normalized_modules[module_id] = {
            "module_id": module_id,
            "display_name": catalog_module.label,
            "status": status,
            "plugin": normalized_plugin,
            "requested_plugin": draft_module.plugin,
            "params": draft_module.params,
            "runnable": bool(plugin_capability.runnable and not plugin_capability.planned and not plugin_capability.preview_only),
            "preview_only": plugin_capability.preview_only,
            "planned": plugin_capability.planned,
        }

    if request.template_id == "metatrack_ablation":
        for module_id in METATRACK_FIXED_MODULES:
            if module_id in variable_modules:
                errors.append(f"当前模板 metatrack_ablation 中，{module_label(module_id)}属于固定环境，不能作为实验变量。")
        for module_id in OUTPUT_MODULES:
            if module_id in variable_modules:
                errors.append(f"{module_label(module_id)}是输出模块，不能作为实验变量。")
    elif not errors and not is_single_module_template:
        warnings.append("当前模板暂不支持 run-draft-smoke。")

    # Go runtime fixed capability boundary.
    fixed_runtime_requirements = {
        "TxPool": "fifo_pool",
        "BlockProducer": "time_or_count_block_producer",
        "CommitteeEpoch": "disabled",
        "MetricsReport": "basic_metrics",
    }
    for module_id, expected in fixed_runtime_requirements.items():
        if module_id in plugin_selection and plugin_selection[module_id] != expected:
            errors.append(f"当前 Go-backed Draft Smoke 要求 {module_label(module_id)} 使用 {expected}。")
    allowed_consensus_plugins = {"simple_leader", "poa_light", "pbft_light_model"}
    consensus_plugin = plugin_selection.get("Consensus")
    if consensus_plugin and consensus_plugin not in allowed_consensus_plugins:
        errors.append("当前 Go-backed Draft Smoke 仅支持 Consensus 使用 simple_leader、poa_light 或 pbft_light_model；PBFT / HotStuff / Raft 仍为 planned / unsupported。")
    allowed_execution_plugins = {"serial_execution", "parallel_light_execution", "metatrack_dual_track_execution", "dual_track_execution"}
    execution_plugin = plugin_selection.get("Execution")
    if execution_plugin and execution_plugin not in allowed_execution_plugins:
        errors.append("当前 Go-backed Draft Smoke 仅支持 Execution 使用 serial_execution、parallel_light_execution 或 metatrack_dual_track_execution；Block-STM / Calvin / real rollback 仍为 planned / unsupported。")
    allowed_state_access_plugins = {"direct_fetch", "remote_state_access_model", "cached_state_access", "access_list_prefetch"}
    state_access_plugin = plugin_selection.get("StateAccess")
    if state_access_plugin and state_access_plugin not in allowed_state_access_plugins:
        errors.append("当前 Go-backed Draft Smoke 仅支持 StateAccess 使用 direct_fetch、remote_state_access_model、cached_state_access 或 access_list_prefetch；real proof / witness / MPT / snapshot 仍为 planned / unsupported。")
    fairness_scope = build_fairness_scope(
        template_id=template_id,
        variable_module=template_variable_module,
        allowed_variable_plugins=sorted(template_allowed_plugins),
        locked_modules=template_locked_modules,
        fairness_rule=str(template.get("fairness_rule") or template.get("fairness", {}).get("fairness_rule", "")),
        preset=preset,
    )
    fairness_validated = False
    if is_single_module_template:
        fairness_validated = True
        for module_id in variable_modules:
            if module_id != template_variable_module:
                fairness_validated = False
                errors.append(
                    fairness_error(
                        template_id,
                        template_variable_module,
                        module_id,
                        plugin_selection_key(module_id),
                        "fixed",
                        plugin_selection.get(module_id, ""),
                    )
                )
        selected_variable_plugin = plugin_selection.get(template_variable_module, "")
        if selected_variable_plugin not in template_allowed_plugins:
            fairness_validated = False
            errors.append(
                f"Fairness violation: template {template_id} only allows {template_variable_module} plugins {sorted(template_allowed_plugins)}, but selected {selected_variable_plugin}."
            )
        for module_id, expected in template_locked_modules.items():
            actual = plugin_selection.get(module_id)
            if actual is not None and actual != expected:
                fairness_validated = False
                errors.append(
                    fairness_error(
                        template_id,
                        template_variable_module,
                        module_id,
                        plugin_selection_key(module_id),
                        expected,
                        actual,
                    )
                )
    elif template_id in {"", "template_unset"}:
        fairness_scope = build_fairness_scope(
            template_id="template_unset",
            variable_module="",
            allowed_variable_plugins=[],
            locked_modules={},
            fairness_rule="No single-module template selected; legacy Draft Smoke validation applies.",
        )

    normalized_draft = {
        "template_id": template_id,
        "experiment_template": template_id,
        "run_mode": "draft_smoke",
        "modules": normalized_modules,
        "plugin_selection": plugin_selection,
        "variable_modules": sorted(set(variable_modules)),
        "fixed_modules": sorted(set(fixed_modules)),
        "disabled_modules": sorted(set(disabled_modules)),
        "planned_modules": sorted(set(planned_modules)),
        "output_modules": sorted(set(output_modules)),
        "fairness_scope": fairness_scope,
        "variable_module": fairness_scope.get("variable_module", ""),
        "locked_modules": fairness_scope.get("locked_modules", {}),
        "fairness_validated": bool(fairness_validated and not errors) if is_single_module_template else False,
        "preset_id": preset_id,
        "preset_name": str(preset.get("preset_name", "")),
        "primary_metrics": list(preset.get("primary_metrics", [])) if preset else [],
        "secondary_metrics": list(preset.get("secondary_metrics", [])) if preset else [],
        "expected_artifacts": list(preset.get("expected_artifacts", [])) if preset else [],
        "result_guide": str(preset.get("result_guide", "")),
        "truthfulness_note": str(preset.get("truthfulness_note") or template.get("truthfulness_note", "")),
    }

    is_valid = not errors
    is_runnable = bool(is_valid and template_id in {"metatrack_ablation", "single_module_txpool", "single_module_blockproducer", "single_module_consensus", "single_module_routing", "single_module_execution", "single_module_state_access"} and not has_preview_only)
    return V3DraftValidationResponse(
        is_valid=is_valid,
        is_runnable=is_runnable,
        run_mode="draft_smoke",
        normalized_draft=normalized_draft,
        variable_modules=normalized_draft["variable_modules"],
        fixed_modules=normalized_draft["fixed_modules"],
        disabled_modules=normalized_draft["disabled_modules"],
        planned_modules=normalized_draft["planned_modules"],
        output_modules=normalized_draft["output_modules"],
        errors=dedupe(errors),
        warnings=dedupe(warnings),
    )


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def plugin_selection_key(module_id: str) -> str:
    return GO_RUNTIME_PLUGIN_CLASSES.get(module_id, module_id)


def fairness_error(template_id: str, variable_module: str, module_id: str, plugin_class: str, expected: str, actual: str) -> str:
    return (
        f"Fairness violation: template {template_id} only allows {variable_module} to vary, "
        f"but {plugin_class} changed from {expected} to {actual}."
    )


def build_fairness_scope(
    *,
    template_id: str,
    variable_module: str,
    allowed_variable_plugins: list[str],
    locked_modules: dict[str, str],
    fairness_rule: str,
    preset: dict[str, Any] | None = None,
) -> dict[str, Any]:
    preset = preset or {}
    return {
        "experiment_template": template_id,
        "preset_id": str(preset.get("preset_id", "")),
        "preset_name": str(preset.get("preset_name", "")),
        "variable_module": variable_module,
        "allowed_variable_plugins": allowed_variable_plugins,
        "locked_modules": locked_modules,
        "fairness_rule": fairness_rule,
        "primary_metrics": list(preset.get("primary_metrics", [])),
        "expected_artifacts": list(preset.get("expected_artifacts", [])),
        "result_guide": str(preset.get("result_guide", "")),
        "truthfulness_note": str(preset.get("truthfulness_note", "")),
    }
