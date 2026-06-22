.DEFAULT_GOAL := help

.PHONY: help check-config sanity v0-sanity generate-trace test-workload test-backend frontend-dev frontend-build replay

help:
	@echo "MBE V0 platform skeleton"
	@echo "  make check-config  Check the default V0 configuration exists"
	@echo "  make sanity        Reserved for the V0 sanity test"
	@echo "  make v0-sanity     Run the V0 end-to-end sanity check"
	@echo "  make generate-trace Generate the default asset_hotspot trace"
	@echo "  make test-workload Run the asset_hotspot workload test"
	@echo "  make test-backend  Run the FastAPI health smoke test"
	@echo "  make frontend-dev  Start the V0 React development server"
	@echo "  make frontend-build Build the V0 React frontend"
	@echo "  make replay        Run the V0 serial executor replay"

replay:
	cd executor && go run ./cmd/replay

generate-trace:
	python -m workload.asset_hotspot.cli --config configs/experiments/v0_default_asset_hotspot.yaml --output experiments/runs/v0_default_asset_hotspot

test-workload:
	python -m pytest tests/workload/test_asset_hotspot.py -q

test-backend:
	python -m pytest backend/tests/test_health.py -q

frontend-dev:
	cd frontend && npm run dev

frontend-build:
	cd frontend && npm run build

check-config:
	@test -f configs/experiments/v0_default_asset_hotspot.yaml
	@echo "Default V0 configuration is present."

sanity:
	@echo "V0 sanity test is not implemented yet."

v0-sanity:
	python scripts/v0_sanity.py
