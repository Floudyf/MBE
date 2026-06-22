.DEFAULT_GOAL := help

.PHONY: help check-config sanity

help:
	@echo "MBE V0 platform skeleton"
	@echo "  make check-config  Check the default V0 configuration exists"
	@echo "  make sanity        Reserved for the V0 sanity test"

check-config:
	@test -f configs/experiments/v0_default_asset_hotspot.yaml
	@echo "Default V0 configuration is present."

sanity:
	@echo "V0 sanity test is not implemented yet."
