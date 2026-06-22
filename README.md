# MBE — Metaverse Blockchain Experiment Platform

当前目标是 V0 平台骨架闭环：基础前端、FastAPI 后端、默认组件编排、`asset_hotspot` 合成负载、流式 `trace.jsonl.gz`、Go 虚拟时钟回放与基础指标。

## V0 范围

V0 只使用默认 MockChain 单链组件包。它不包含 Fabric、EVM、PBFT、HotStuff、DAG、复杂路由或执行、跨链协议、Grafana、多用户权限或分布式部署。

运行时版本固定为 Python 3.12.x、Go 1.24.x、Node.js 22 LTS、React 18.x、TypeScript 5.x、FastAPI 0.115.x 与 Uvicorn 0.30.x。完整实施依据见 `AGENTS.md`、`docs/v0_implementation_plan.md` 和 `docs/platform_plan_full.md`。

## 当前目录

- `frontend/`：V0 最小 UI 的预留位置。
- `backend/`：FastAPI API、Composer 与本地运行控制的预留位置。
- `workload/`、`trace/`：合成负载与流式 trace 的预留位置。
- `chain/mockchain/`：V0 唯一链后端的预留位置。
- `executor/`：Go 回放执行器及虚拟时钟的预留位置。
- `configs/`：默认实验、插件清单与未来 schema 的位置。

运行 `make help` 查看当前骨架任务；尚未实现运行、回放或 sanity 测试。
