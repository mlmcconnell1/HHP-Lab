# CLI Reference

This chapter documents the currently supported CLI surface from `hhplab/cli/main.py`.

## Top-Level Groups

- `hhplab agents`
- `hhplab status`
- `hhplab ingest ...`
- `hhplab list ...`
- `hhplab validate ...`
- `hhplab diagnostics ...`
- `hhplab generate ...`
- `hhplab build ...`
- `hhplab aggregate ...`
- `hhplab show ...`
- `hhplab registry ...`
- `hhplab migrate ...`

## High-Value Commands

### Environment Preflight (Agent-Safe)

```bash
hhplab status
hhplab status --json
```

`status` performs a one-shot readiness scan across:
- curated assets under the configured data directory (`data/curated/` by default)
- recipe output namespaces under the configured output root (`outputs/` by default)
- missing-prerequisite checks with actionable hints

Exit behavior:
- exits `1` when required prerequisites are missing (for example boundaries or census geometry)
- exits `0` for healthy/partially-ready states without hard errors

### Crosswalk Generation

```bash
hhplab generate xwalks --boundary 2025 --tracts 2023
```

Important options:
- `--boundary`
- `--tracts`
- `--counties`
- `--type {tracts,counties,all}`
- `--population-weights`
- `--auto-fetch` to fetch tract population inputs when population weights are requested

### Aggregation Commands

```bash
hhplab aggregate acs --years 2018-2024 --weighting population
hhplab aggregate zori --years 2018-2024 --align pit_january
hhplab aggregate pep --years 2018-2024
hhplab aggregate pit --years 2018-2024
```

All four write to `asset_store_root/curated/<dataset>/` by default
(`data/curated/<dataset>/` with built-in defaults). `--years` is required.

These commands produce standalone CoC aggregate artifacts. They are not a
prerequisite for recipe execution unless a recipe explicitly points to
aggregate outputs.

Use them for:
- materializing curated prerequisites ahead of a recipe build
- debugging a single dataset family in isolation
- generating standalone CoC artifacts outside recipe execution

Current ACS aggregation details:
- `aggregate acs` reads cached tract files only; it does not call Census APIs
- `aggregate acs` supports `--align {vintage_end_year,window_center_year}`
- `aggregate acs` defaults to `--weighting area`; use `--weighting population` when population-weighted interpolation is intended

### Recipe Execution

```bash
# Default human workflow
hhplab build recipe --recipe recipes/metro25-glynnfox.yaml

# Automation / CI readiness check
hhplab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml --json

# Same command path without execution
hhplab build recipe --recipe recipes/metro25-glynnfox.yaml --dry-run
```

Current behavior:
- Runs schema + adapter validation and the same preflight checks used by `recipe-preflight` before execution
- Executes `materialize -> resample -> join -> persist`; recipes that use
  `small_area_estimate` insert that step before the final join
- Persists panel output under the configured `output_root/<recipe-name>/` when
  the target declares `outputs: [panel]` (default). Built-in default:
  `<project_root>/outputs/<recipe-name>/`
- Writes recipe sidecar manifest: `*.manifest.json`
- Supports `--no-cache` to disable recipe asset caching
- Supports `--asset-store-root` and `--output-root` for one-off path overrides
- Emits explicit Connecticut county-transition notes when county-native recipe
  inputs need planning-region to legacy-county normalization
- `--json` output includes `artifacts` with resolved output paths (`panel_path`, `manifest_path`, and `diagnostics_path` when declared)

### Recipe Preflight (No Execution)

```bash
hhplab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml
hhplab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml --json
```

Use this for a no-execute readiness gate in automation/CI, or when you want a
complete blocker/warning report without starting the build.

Current special-case note:

- When a recipe mixes Connecticut planning-region county IDs with a
  legacy-county crosswalk, preflight emits `ct_county_alignment` findings
  instead of treating the build as an unexplained green pass.
- `small_area_estimate` recipes are validated for ACS1 county source artifacts,
  ACS5 tract support artifacts, target crosswalk availability, compatible
  vintages, and ACS1 2020 unavailability. Missing-artifact findings include
  remediation guidance when a direct ingest or normalization path exists.

### Recipe Plan (No Execution)

```bash
hhplab build recipe-plan --recipe recipes/metro25-glynnfox.yaml
hhplab build recipe-plan --recipe recipes/metro25-glynnfox.yaml --json
```

Use this to resolve and inspect planned tasks (`materialize`, `resample`,
`small_area_estimate`, `join`) while authoring or debugging a recipe. For a
readiness gate, use `recipe-preflight`.

### Legacy CLI Migration

Recipe commands are now the only supported end-to-end orchestration surface.
Legacy named-build orchestration has been removed.

Use this mapping when migrating older workflows:

- `hhplab build recipe --recipe <file>` replaces named build creation + end-to-end execution.
- `hhplab build recipe-preflight --recipe <file> --json` replaces the old “check the build state first” pattern.
- `hhplab status --output-root <path>` replaces `hhplab status --builds-dir <path>`.
- `hhplab aggregate <dataset> --years <spec>` replaces `hhplab aggregate <dataset> --build <name>` when you need standalone curated artifacts.
- `hhplab generate xwalks --boundary <year> --tracts <year>` replaces `hhplab generate xwalks --build <name> ...`.

Low-level commands remain supported as curated-prerequisite tools. They are no
longer documented as a parallel orchestration framework.

### Recipe Provenance Utilities

```bash
hhplab build recipe-provenance --manifest <manifest_path>
hhplab build recipe-export --manifest <manifest_path> --destination /tmp/bundle
```

`recipe-export` accepts `--asset-store-root` and `--output-root` so manifests
from non-default storage locations can still be resolved correctly.

### Storage Root Configuration

Canonical paths resolve from two storage roots:

- `asset_store_root`: reusable internal assets (`raw/`, `curated/`)
- `output_root`: recipe-built outputs and manifest sidecars

Resolution precedence is:

- CLI flags: `--asset-store-root`, `--output-root`
- Environment: `HHPLAB_ASSET_STORE_ROOT`, `HHPLAB_OUTPUT_ROOT`
- Repo config: `hhplab.yaml`
- User config: `~/.config/hhplab/config.yaml`
- Built-in defaults

Built-in defaults:

- `asset_store_root = <project_root>/data`
- `output_root = <project_root>/outputs`

### Core Ingestion Commands

```bash
hhplab ingest boundaries --source hud_exchange --vintage 2025
hhplab ingest tiger --year 2023 --type all
hhplab ingest acs5-tract --acs 2019-2023 --tracts 2023
hhplab ingest acs1-metro --vintage 2023
hhplab ingest acs1-county --vintage 2023
hhplab ingest pit-vintage --vintage 2024
hhplab ingest zori --geography county
hhplab ingest pep --series auto
```

Boundary ingestion uses a multi-source fallback chain: national boundary file first, then legacy NatlTerrDC URL, then per-state shapefiles. This makes historical vintage ingestion more reliable.

ACS1 metro ingestion (`acs1-metro`) fetches ACS 1-year detailed-table data at CBSA geography, keeps canonical CBSA IDs for the full metro universe by default, and can materialize subset-profile outputs such as Glynn/Fox when `--definition-version` requests one. The curated artifact includes B23025 employment counts and `unemployment_rate_acs1`, plus current ACS1 income-distribution, housing-cost, utility-cost, tenure, housing-stock, and household-size measures. Options: `--vintage`, `--definition-version`, `--api-key`, `--json`.

ACS1 county ingestion (`acs1-county`) fetches the same ACS 1-year detailed-table set at county geography and writes `data/curated/acs/acs1_county__A{vintage}.parquet`. Census only publishes ACS1 for qualifying counties, so non-threshold counties are absent by design. Options: `--vintage`, `--api-key`, `--json`.

County ACS1 artifacts are also the source for ACS1/ACS5 small-area-estimation
recipes. Those recipes consume normalized ACS1 county source artifacts such as
`data/curated/acs/acs1_county_sae__A2023.parquet` and ACS5 tract support
artifacts such as
`data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet`. There is no
separate top-level SAE ingest command; use `build recipe-plan` and
`build recipe-preflight` on a recipe with a `small_area_estimate` step to see
the exact required artifacts and remediation.

Useful PEP options:
- `--start` / `--end` to trim the emitted year range
- `--prefer-postcensal-2020` when combining series

### Dataset Discovery

```bash
hhplab list curated                       # List all curated files with metadata
hhplab list curated --subdir pit          # Filter by subdirectory
hhplab list curated --json                # JSON output for automation
```

`list curated` shows Parquet file paths, row counts, column lists, and file sizes. Useful for exploring what curated data is available before building recipes.

## JSON and Non-Interactive Modes

### Structured JSON Outputs

`--json` is available on these high-value commands:
- `hhplab status`
- `hhplab build recipe`
- `hhplab build recipe-preflight`
- `hhplab build recipe-plan`
- `hhplab build recipe-provenance`
- `hhplab build recipe-export`
- `hhplab list census`
- `hhplab list curated`
- `hhplab list measures`
- `hhplab list xwalks`
- `hhplab ingest acs1-metro`
- `hhplab diagnostics xwalk`
- `hhplab diagnostics panel`

Current caveat: `list census/measures/xwalks --json` emits JSON when matches are found. Empty or missing-directory cases may still emit human text and exit `0`.

`build recipe --json` now includes an `artifacts` key with resolved output paths for each pipeline, enabling automation to locate panel, manifest, and diagnostics files without path guessing.

### Non-Interactive CLI Use

For automation and agents, disable prompts with either:
- global flag: `hhplab --non-interactive ...`
- environment variable: `HHPLAB_NON_INTERACTIVE=1`

Example:

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab status --json
```

In non-interactive mode, destructive actions still require explicit consent flags. Example: `hhplab registry delete-entry ...` requires `--yes`.

## Operational Guidance

- Use `build recipe` as the default human entrypoint.
- Use `status` + `build recipe-preflight --json` before `build recipe --json` in automation.
- Use `build recipe-plan --json` when you need to inspect the resolved task graph.
- Use `aggregate` only for standalone CoC artifacts or recipes that explicitly depend on aggregate outputs.
- Use `build recipe-export` to produce portable bundles from recipe outputs.

For exact option signatures, use:

```bash
hhplab --help
hhplab <group> --help
hhplab <group> <command> --help
```

---

**Previous:** [[03-Architecture]] | **Next:** [[05-Recipe-Format]]
