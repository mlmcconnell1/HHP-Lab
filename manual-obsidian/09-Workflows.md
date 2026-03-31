# Workflows

## Recipe-Driven Build

1. Inspect available curated assets if needed (`coclab status --json` for automation, `coclab list curated` for browsing).
2. Ingest required global assets (`boundaries`, `tiger`, `acs5-tract`, `pit`, `zori`, `pep`, and optionally `acs1-metro` for metro unemployment).
3. Generate required crosswalks.
4. Run `coclab build recipe-preflight` when you want a no-execute readiness gate.
5. Run a YAML recipe for deterministic panel construction.
6. Export a bundle for downstream analysis.

Example command sequence:

```bash
# 1) Ingest core sources
coclab ingest boundaries --source hud_exchange --vintage 2025
coclab ingest tiger --year 2023 --type all
coclab ingest acs5-tract --acs 2019-2023 --tracts 2023
coclab ingest acs1-metro --vintage 2023          # optional: metro ACS1 unemployment
coclab ingest pit-vintage --vintage 2024
coclab ingest zori --geography county
coclab ingest pep --series auto

# 2) Crosswalks
coclab generate xwalks --boundary 2025 --tracts 2023 --counties 2023

# 3) Automation / CI readiness check
coclab status --json
coclab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml --json

# 4) Optional: inspect the resolved task graph while authoring/debugging
coclab build recipe-plan --recipe recipes/metro25-glynnfox.yaml --json

# 5) Recipe execution
coclab build recipe --recipe recipes/metro25-glynnfox.yaml

# 6) Export bundle
coclab build recipe-export --manifest data/curated/panel/<file>.manifest.json --output exports/bundle
```

## Workflow Principles

- Treat recipe files as auditable execution plans, not ad-hoc scripts.
- `coclab build recipe` is the default human entrypoint.
- `coclab build recipe-preflight --json` is the default no-execute automation/CI gate.
- `coclab build recipe-plan --json` is for authoring/debugging, not for readiness checking.
- `coclab aggregate ...` is a parallel path for standalone CoC artifacts, not a default prerequisite for recipe execution.
- See `recipes/examples/README.md` for runnable example recipes that cover CoC, metro, PIT, ACS5, PEP, ZORI, and ACS1 paths.
- Use `--non-interactive` (or `COCLAB_NON_INTERACTIVE=1`) for agent automation.

---

**Previous:** [[08-Temporal-Terminology]] | **Next:** [[10-Methodology-ACS-Aggregation]]
