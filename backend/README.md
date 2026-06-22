# Backend

Reserved for the V0 FastAPI experiment API, Composer preview API, local process runner, and log streaming. No Fabric/EVM controller, multi-user service, or distributed scheduler belongs in V0.

## Run the V0 backend

From the repository root, install the backend dependencies and start Uvicorn:

```powershell
python -m pip install -r backend/requirements.txt
python -m uvicorn backend.app.main:app --reload
```

The health endpoint confirms that the control layer is available:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/health
```

It returns `{"status": "ok"}`.

## Default V0 experiment

Run the default asset-hotspot experiment:

```powershell
Invoke-RestMethod -Method Post http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/run
```

View its summary metrics after the run completes:

```powershell
Invoke-RestMethod http://127.0.0.1:8000/api/v0/experiments/v0_default_asset_hotspot/summary
```

Generated experiment artifacts under `experiments/runs/` are local outputs and are not committed to the repository.
