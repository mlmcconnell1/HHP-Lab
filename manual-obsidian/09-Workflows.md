# Workflows

## Recipe-Driven Build

1. Run a preflight readiness check (`coclab status --json` for automation).
2. Ingest required global assets (`boundaries`, `tiger`, `acs5-tract`, `pit`, `zori`, `pep`).
3. Generate required crosswalks.
4. Aggregate source datasets into curated folders.
5. Resolve the recipe plan before execution (`coclab build recipe-plan`).
6. Run a YAML recipe for deterministic panel construction.
7. Export a bundle for downstream analysis.

Example command sequence:

```bash
# 1) Ingest core sources
coclab ingest boundaries --source hud_exchange --vintage 2025
coclab ingest tiger --year 2023 --type all
coclab ingest acs5-tract --acs 2019-2023 --tracts 2023
coclab ingest pit-vintage --vintage 2024
coclab ingest zori --geography county
coclab ingest pep --series auto

# 2) Crosswalks
coclab generate xwalks --boundary 2025 --tracts 2023 --counties 2023

# 3) Aggregates
coclab aggregate acs --weighting population
coclab aggregate zori --align pit_january
coclab aggregate pep
coclab aggregate pit

# 4) Preflight + plan
coclab status --json
coclab build recipe-plan --recipe recipes/glynn_fox_v1.yaml --json

# 5) Recipe execution
coclab build recipe --recipe recipes/glynn_fox_v1.yaml

# 6) Export bundle
coclab build recipe-export --manifest data/curated/panel/<file>.manifest.json --output exports/bundle
```

## Workflow Principles

- Treat recipe files as auditable execution plans, not ad-hoc scripts.
- Use `--non-interactive` (or `COCLAB_NON_INTERACTIVE=1`) for agent automation.
- Use `coclab build recipe --dry-run --json` and `coclab build recipe-plan --json` together as a pre-execution check.

---

**Previous:** [[08-Temporal-Terminology]] | **Next:** [[10-Methodology-ACS-Aggregation]]
