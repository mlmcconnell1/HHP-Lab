# CLI Reference

This chapter documents the currently supported CLI surface from `coclab/cli/main.py`.

## Top-Level Groups

- `coclab agents`
- `coclab status`
- `coclab ingest ...`
- `coclab list ...`
- `coclab validate ...`
- `coclab diagnostics ...`
- `coclab generate ...`
- `coclab build ...`
- `coclab aggregate ...`
- `coclab show ...`
- `coclab registry ...`
- `coclab migrate ...`

## High-Value Commands

### Environment Preflight (Agent-Safe)

```bash
coclab status
coclab status --json
```

`status` performs a one-shot readiness scan across:
- curated assets under `data/curated/` (boundaries, TIGER, crosswalks, PIT, ACS, measures, ZORI)
- named builds and build manifests
- missing-prerequisite checks with actionable hints

Exit behavior:
- exits `1` when required prerequisites are missing (for example boundaries or census geometry)
- exits `0` for healthy/partially-ready states without hard errors

### Build Scaffolding

```bash
coclab build create --name demo --years 2018-2024
coclab build create --name metro-gf --years 2011-2016 --geo-type metro --definition-version glynn_fox_v1
coclab build list
```

`build create` initializes:
- `builds/<name>/data/curated/`
- `builds/<name>/data/raw/`
- `builds/<name>/base/`
- `builds/<name>/manifest.json`

Current options also let you pin build metadata:
- `--geo-type {coc,metro}` to mark the target analysis geography
- `--definition-version` for metro builds

### Crosswalk Generation

```bash
coclab generate xwalks --build demo --boundary 2025 --tracts 2023
```

Important options:
- `--build` (required)
- `--boundary`
- `--tracts`
- `--counties`
- `--type {tracts,counties,all}`
- `--population-weights`
- `--auto-fetch` to fetch tract population inputs when population weights are requested

### Aggregation Commands

```bash
coclab aggregate acs --build demo
coclab aggregate zori --build demo
coclab aggregate pep --build demo
coclab aggregate pit --build demo
```

All four require `--build` and write into `builds/<name>/data/curated/<dataset>/`.

Current ACS aggregation details:
- `aggregate acs` reads cached tract files only; it does not call Census APIs
- `aggregate acs` supports `--align {vintage_end_year,window_center_year}`
- `aggregate acs` defaults to `--weighting area`; use `--weighting population` when population-weighted interpolation is intended

### Imperative Panel Build

```bash
coclab build panel --build demo --start 2018 --end 2024 --weighting population
```

Optional ZORI integration:

```bash
coclab build panel --build demo --start 2018 --end 2024 --include-zori
```

#### Metro Panel Build

Use `--geo-type metro` with `--definition-version` to build a metro-targeted panel:

```bash
coclab build panel --build metro-gf --start 2011 --end 2016 \
    --geo-type metro --definition-version glynn_fox_v1
```

Metro builds aggregate PIT counts from member CoCs and derive ACS/PEP/ZORI measures from county membership tables. The `--definition-version` flag is required when `--geo-type` is `metro`.

Current options also include:
- `--strict` to fail the command on conformance errors
- `--skip-conformance` to suppress post-build checks
- `--zori-yearly-path` to point at an explicit yearly ZORI artifact

### Recipe Execution (Recommended)

```bash
# Validate only
coclab build recipe --recipe recipes/glynn_fox_v1.yaml --dry-run --json

# Execute
coclab build recipe --recipe recipes/glynn_fox_v1.yaml
```

Current behavior:
- Runs schema + adapter + dataset-path checks before execution
- Executes `materialize -> resample -> join -> persist`
- Persists panel output to canonical `data/curated/panel/...` when the target declares `outputs: [panel]` (default)
- Writes recipe sidecar manifest: `*.manifest.json`
- Supports `--no-cache` to disable recipe asset caching

### Recipe Plan (No Execution)

```bash
coclab build recipe-plan --recipe recipes/glynn_fox_v1.yaml
coclab build recipe-plan --recipe recipes/glynn_fox_v1.yaml --json
```

Use this to resolve and inspect planned tasks (`materialize`, `resample`, `join`) before execution.

### Recipe Provenance Utilities

```bash
coclab build recipe-provenance --manifest data/curated/panel/<file>.manifest.json
coclab build recipe-export --manifest data/curated/panel/<file>.manifest.json --output /tmp/bundle
```

### Bundle Export

```bash
coclab build export --name analysis_demo --build demo
```

Key points:
- `--build` is required in current implementation
- Creates `exports/export-N/`
- Produces `MANIFEST.json` and `README.md`
- Supports `--include`, `--panel`, `--compress`, and vintage filters such as `--boundary-vintage`

### Artifact Inventory

```bash
coclab list artifacts --build demo --json
```

Key options:
- `--build` (required)
- `--include-global / --build-only` to include or exclude global `data/curated` artifacts
- `--geo-type {coc,metro}` to filter the inventory
- `--definition-version` to scope metro inventories
- `--json` for machine-readable inventory (roles, row/column counts where available, schema hash, provenance hints)

### Core Ingestion Commands

```bash
coclab ingest boundaries --source hud_exchange --vintage 2025
coclab ingest tiger --year 2023 --type all
coclab ingest acs5-tract --acs 2019-2023 --tracts 2023
coclab ingest pit-vintage --vintage 2024
coclab ingest zori --geography county
coclab ingest pep --series auto
```

Useful PEP options:
- `--start` / `--end` to trim the emitted year range
- `--prefer-postcensal-2020` when combining series

## JSON and Non-Interactive Modes

### Structured JSON Outputs

`--json` is available on these high-value commands:
- `coclab status`
- `coclab build recipe`
- `coclab build recipe-plan`
- `coclab build recipe-provenance`
- `coclab build recipe-export`
- `coclab list artifacts`
- `coclab list census`
- `coclab list measures`
- `coclab list xwalks`
- `coclab diagnostics xwalk`
- `coclab diagnostics panel`

Current caveat: `list census/measures/xwalks --json` emits JSON when matches are found. Empty or missing-directory cases may still emit human text and exit `0`.

### Non-Interactive CLI Use

For automation and agents, disable prompts with either:
- global flag: `coclab --non-interactive ...`
- environment variable: `COCLAB_NON_INTERACTIVE=1`

Example:

```bash
COCLAB_NON_INTERACTIVE=1 coclab status --json
```

In non-interactive mode, destructive actions still require explicit consent flags. Example: `coclab registry delete-entry ...` requires `--yes`.

## Operational Guidance

- Prefer `builds/<name>` workflow for repeatable analysis runs.
- Use `build recipe` when you need explicit, auditable pipeline structure.
- Use `status` + `build recipe-plan --json` as a preflight pair in automation.
- Use `build export` only after validating expected artifacts exist in the build.

For exact option signatures, use:

```bash
coclab --help
coclab <group> --help
coclab <group> <command> --help
```

---

**Previous:** [[03-Architecture]] | **Next:** [[05-Recipe-Format]]
