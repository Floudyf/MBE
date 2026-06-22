# MBE — Metaverse Blockchain Experiment Platform

当前目标是 V0 平台骨架闭环：基础前端、FastAPI 后端、默认组件编排、`asset_hotspot` 合成负载、流式 `trace.jsonl.gz`、Go 虚拟时钟回放与基础指标。

## V0 范围

V0 只使用默认 MockChain 单链组件包。它不包含 Fabric、EVM、PBFT、HotStuff、DAG、复杂路由或执行、跨链协议、Grafana、多用户权限或分布式部署。

运行时版本固定为 Python 3.12.x、Go 1.24.x、Node.js 22 LTS 或 Node.js 24 LTS、React 18.x、TypeScript 5.x、FastAPI 0.115.x 与 Uvicorn 0.30.x。完整实施依据见 `AGENTS.md`、`docs/v0_implementation_plan.md` 和 `docs/platform_plan_full.md`。

## 当前目录

- `frontend/`：V0 最小 UI 的预留位置。
- `backend/`：FastAPI API、Composer 与本地运行控制的预留位置。
- `workload/`、`trace/`：合成负载与流式 trace 的预留位置。
- `chain/mockchain/`：V0 唯一链后端的预留位置。
- `executor/`：Go 回放执行器及虚拟时钟的预留位置。
- `configs/`：默认实验、插件清单与未来 schema 的位置。

运行 `make help` 查看当前骨架任务；尚未实现运行、回放或 sanity 测试。

## Python 开发与测试

在项目根目录使用 Python 3.12 创建并激活虚拟环境：

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt
```

运行 asset_hotspot workload 测试：

```powershell
python -m pytest tests/workload/test_asset_hotspot.py -q
```

也可运行 `make test-workload`。

## V0 后端控制层

从仓库根目录安装后端依赖并启动 FastAPI 控制层：

```powershell
python -m pip install -r backend/requirements.txt
python -m uvicorn backend.app.main:app --reload
```

健康检查：

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

运行默认实验并查看指标：

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/run
Invoke-RestMethod http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/summary
```

运行产生的 `experiments/runs/` 产物仅供本地查看，已由 `.gitignore` 排除，不会提交。

## V0 前端

前端使用 Node.js 22 LTS 或 Node.js 24 LTS、React 18、TypeScript 5 和 Vite。在启动 FastAPI 后端后，从仓库根目录运行：

```powershell
cd frontend
npm install
npm run dev
```

默认前端地址由 Vite 输出（通常为 `http://127.0.0.1:5173`），并连接本地后端 `http://127.0.0.1:8000`。页面提供默认实验运行、V0 Composer 插件预览、`runtime.log` 刷新和 summary 指标刷新。也可从根目录使用 `make frontend-dev` 或 `make frontend-build`。

## V0 端到端 Sanity Check

从仓库根目录运行以下命令，可重新生成默认 `asset_hotspot` trace、执行 Go replay，并检查 trace、summary、latency 与 runtime log 的关键产物和指标：

```powershell
python scripts/v0_sanity.py
# 或
make v0-sanity
```

Sanity check 生成的 `experiments/runs/` 文件是本地实验产物，不会提交到 Git。
