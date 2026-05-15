# Workflows

## Recipe-Driven Build

1. Inspect available curated assets if needed (`hhplab status --json` for automation, `hhplab list curated` for browsing).
2. Ingest required global assets (`boundaries`, `tiger`, `acs5-tract`, `pit`, `zori`, `pep`, optionally `acs1-metro` for metro-native ACS1 measures, and optionally `acs1-county` plus ACS5 tract support inputs for SAE workflows).
3. Generate required crosswalks.
4. Run `hhplab build recipe-preflight` when you want a no-execute readiness gate.
5. Run a YAML recipe for deterministic panel construction.
6. Export a bundle for downstream analysis.

If you are not using the default repo-local layout, set storage roots once via
CLI flags, environment, or config:

```bash
export HHPLAB_ASSET_STORE_ROOT=/srv/hhplab-assets
export HHPLAB_OUTPUT_ROOT=/srv/hhplab-outputs
```

Example command sequence:

```bash
# 1) Ingest core sources
hhplab ingest boundaries --source hud_exchange --vintage 2025
hhplab ingest tiger --year 2023 --type all
hhplab ingest acs5-tract --acs 2019-2023 --tracts 2023
hhplab ingest acs1-metro --vintage 2023          # optional: metro-native ACS1 measures
hhplab ingest acs1-county --vintage 2023         # optional: county-native ACS1 measures
hhplab ingest pit-vintage --vintage 2024
hhplab ingest zori --geography county
hhplab ingest pep --series auto

# 2) Crosswalks
hhplab generate xwalks --boundary 2025 --tracts 2023 --counties 2023

# 3) Automation / CI readiness check
hhplab status --json
hhplab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml --json

# 4) Optional: inspect the resolved task graph while authoring/debugging
hhplab build recipe-plan --recipe recipes/metro25-glynnfox.yaml --json

# 5) Recipe execution
hhplab build recipe --recipe recipes/metro25-glynnfox.yaml

# Optional: ACS1/ACS5 small-area estimation example
hhplab build recipe-preflight --recipe recipes/examples/coc-sae-acs1-2023.yaml --json
hhplab build recipe --recipe recipes/examples/coc-sae-acs1-2023.yaml

# 6) Export bundle
hhplab build recipe-export --manifest <manifest_path> --destination exports/bundle
```

`<manifest_path>` should come from the `artifacts.manifest_path` field returned
by `hhplab build recipe --json`, especially when `output_root` is outside the
repository tree.

## Workflow Principles

- Treat recipe files as auditable execution plans, not ad-hoc scripts.
- `hhplab build recipe` is the default human entrypoint.
- `hhplab build recipe-preflight --json` is the default no-execute automation/CI gate.
- `hhplab build recipe-plan --json` is for authoring/debugging, not for readiness checking.
- `hhplab aggregate ...` is a parallel path for standalone CoC artifacts, not a default prerequisite for recipe execution.
- See `recipes/examples/README.md` for runnable example recipes that cover CoC, metro, PIT, ACS5, PEP, ZORI, and ACS1 paths.
- Use `recipes/examples/coc-sae-acs1-2023.yaml` for ACS1 county-to-CoC SAE
  planning, and
  `recipes/top50-msas-cocs-pit-pep-density-acs1-poverty-2010-2024.yaml` for
  modeled ACS1 poverty tract integration.
- Use `--non-interactive` (or `HHPLAB_NON_INTERACTIVE=1`) for agent automation.

---

**Previous:** [[08-Temporal-Terminology]] | **Next:** [[10-Methodology-ACS-Aggregation]]
