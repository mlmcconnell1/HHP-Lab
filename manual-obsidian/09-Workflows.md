# Workflows

## Recommended Workflow: Recipe-Driven Build

1. Run a preflight readiness check (`coclab status --json` for automation).
2. Ingest required global assets (`boundaries`, `tiger`, `acs5-tract`, `pit`, `zori`, `pep`).
3. Create a named build and pin base assets.
4. Generate required crosswalks into the build.
5. Aggregate source datasets into build-local curated folders.
6. Resolve the recipe plan before execution (`coclab build recipe-plan`).
7. Run a YAML recipe for deterministic panel construction.
8. Optionally inventory artifacts (`coclab list artifacts --build <name> --json`).
9. Export a bundle for downstream analysis.

Example command sequence:

```bash
# 1) Ingest core sources
coclab ingest boundaries --source hud_exchange --vintage 2025
coclab ingest tiger --year 2023 --type all
coclab ingest acs5-tract --acs 2019-2023 --tracts 2023
coclab ingest pit-vintage --vintage 2024
coclab ingest zori --geography county
coclab ingest pep --series auto

# 2) Build scaffold
coclab build create --name demo --years 2018-2024

# 3) Crosswalks
coclab generate xwalks --build demo --boundary 2025 --tracts 2023 --counties 2023

# 4) Aggregates
coclab aggregate acs --build demo --weighting population
coclab aggregate zori --build demo --align pit_january
coclab aggregate pep --build demo
coclab aggregate pit --build demo

# 5) Preflight + plan
coclab status --json
coclab build recipe-plan --recipe recipes/glynn_fox_v1.yaml --json

# 6) Recipe execution
coclab build recipe --recipe recipes/glynn_fox_v1.yaml

# 7) Artifact inventory (optional)
coclab list artifacts --build demo --json

# 8) Export bundle
coclab build export --name demo_bundle --build demo
```

## Alternate Workflow: Imperative Panel Build

For existing pipelines/tests that still use the panel assembler directly:

```bash
coclab build panel --build demo --start 2018 --end 2024 --weighting population
```

Use this path when you want the legacy panel contract; use recipe execution when you need explicit multi-step composition and recipe manifests.

## Workflow Principles

- Pin and record boundary assets via named builds.
- Keep heavy transformations build-scoped.
- Treat recipe files as auditable execution plans, not ad-hoc scripts.
- Use `--non-interactive` (or `COCLAB_NON_INTERACTIVE=1`) for agent automation.
- Use `coclab build recipe --dry-run --json` and `coclab build recipe-plan --json` together as a pre-execution check.

---

**Previous:** [[08-Temporal-Terminology]] | **Next:** [[10-Methodology-ACS-Aggregation]]
