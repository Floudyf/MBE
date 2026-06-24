# V1.8 baseline / sweep / report

Run `python scripts/v1_8_sweep.py --dry-run` to inspect the four configurations, or `python scripts/v1_8_sweep.py --out .cache/v1_8_sweeps/latest` for a small replay sweep. Outputs are CSV, JSON, and Markdown under the selected ignored cache directory. This stage compares existing V1.5 routing, V1.6 dual-track, and V1.7 aggregation metrics; it introduces no new mechanism, Fabric run, cross-chain behaviour, MetaFlow, or multi-server deployment.
