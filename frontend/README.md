# V0 Frontend

The V0 frontend is a minimal React + Vite control panel for the default single-chain experiment. It presents the default plugin composition, starts the existing backend run API, reads `runtime.log`, and displays the backend summary metrics.

## Run locally

Use Node.js 22 LTS. From this directory:

```powershell
npm install
npm run dev
```

Vite prints the local UI URL (normally `http://127.0.0.1:5173`). Start the FastAPI backend separately at `http://127.0.0.1:8000`.

The Vite development server proxies API requests to the default backend address, `http://127.0.0.1:8000`, so local browser requests do not need a backend CORS change. To use another backend address, set `VITE_API_BASE_URL` before starting Vite.

```powershell
$env:VITE_API_BASE_URL = "http://127.0.0.1:8000"
npm run dev
```

Build a production bundle with `npm run build`.
