.DEFAULT_GOAL := help

.PHONY: help check-config sanity generate-trace test-workload replay

help:
	@echo "MBE V0 platform skeleton"
	@echo "  make check-config  Check the default V0 configuration exists"
	@echo "  make sanity        Reserved for the V0 sanity test"
	@echo "  make generate-trace Generate the default asset_hotspot trace"
	@echo "  make test-workload Run the asset_hotspot workload test"
	@echo "  make replay        Run the V0 serial executor replay"

replay:
	cd executor && go run ./cmd/replay

generate-trace:
	python -m workload.asset_hotspot.cli --config configs/experiments/v0_default_asset_hotspot.yaml --output experiments/runs/v0_default_asset_hotspot

test-workload:
	python -m pytest tests/workload/test_asset_hotspot.py -q

check-config:
	@test -f configs/experiments/v0_default_asset_hotspot.yaml
	@echo "Default V0 configuration is present."

sanity:
	@echo "V0 sanity test is not implemented yet."
