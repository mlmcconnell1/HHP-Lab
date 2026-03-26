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

### Recipe Execution

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

- Use `build recipe` for explicit, auditable pipeline structure.
- Use `status` + `build recipe-plan --json` as a preflight pair in automation.
- Use `build recipe-export` to produce portable bundles from recipe outputs.

For exact option signatures, use:

```bash
coclab --help
coclab <group> --help
coclab <group> <command> --help
```

---

**Previous:** [[03-Architecture]] | **Next:** [[05-Recipe-Format]]
