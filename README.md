# HHP-Lab

HHP-Lab is a Python toolkit and CLI for building analysis-ready homelessness panels from HUD Continuum of Care data, Census geography and population products, ACS tract measures, and Zillow rent data.

The repository was renamed from CoC-Lab to HHP-Lab to better reflect the
current scope. Historical references to CoC-Lab are still preserved where they
describe legacy artifacts or documents, including the beads database/JSONL
files and historical files in `devdocs/`.

The project started as CoC boundary infrastructure, but it now supports three
analysis geography families:

- `coc`: HUD Continuum of Care geographies with explicit boundary vintages
- `metro`: canonical Census metro-universe geographies keyed by a `definition_version`, with optional subset-profile metadata layered on top
- `msa`: Census Metropolitan Statistical Areas keyed by 5-digit CBSA/MSA identifiers plus a delineation `definition_version`

The current metro-universe implementation uses official Census delineations
such as `census_msa_2023` as the canonical `metro` surface. The 25 Glynn/Fox
metros from *Dynamics of Homelessness in Urban America* now live as a subset
profile (`glynn_fox` / `glynn_fox_v1`) over that universe. The `msa` surface
remains separate and uses the same official delineations keyed explicitly as
MSAs / CBSAs.

Full operational documentation lives in [HHP-Lab-manual/HHP-Lab-Manual.md](HHP-Lab-manual/HHP-Lab-Manual.md).

## What HHP-Lab Does

- Ingests curated source data for CoC boundaries, TIGER tracts and counties, ACS, PEP, PIT, and ZORI
- Builds tract-to-CoC and county-to-CoC crosswalks
- Aggregates ACS, PIT, PEP, and ZORI inputs into analysis-ready outputs
- Assembles panel datasets across years with provenance metadata embedded in parquet artifacts
- Supports recipe-driven builds, export bundles, curated-layout validation, and machine-readable CLI output for automation

## Package Boundaries

Source-owned aggregation code lives with its provider package, such as
`hhplab.acs`, `hhplab.pep`, `hhplab.pit`, and `hhplab.rents`. The
`hhplab.measures` package is intentionally narrower: it is a compatibility
facade for legacy ACS measure imports plus the home for reusable
crosswalk/measure attribution diagnostics used by CLI commands and tests. New
source-specific aggregation helpers should not be added there.

## Supported Inputs

| Provider | Product | Native geometry | Coverage |
| --- | --- | --- | --- |
| HUD | PIT | CoC | 2007-ongoing |
| Census | ACS 5-year | tract | 2009-ongoing |
| Census | PEP | county | 2010-ongoing |
| Zillow | ZORI | county | 2015-ongoing |

Important temporal rules:

- ACS vintage for PIT year `Y` is typically `Y-1`
- ACS tract geography follows decennial vintages: 2000-era, 2010-era, then 2020-era
- ZORI support starts in January 2015, so metro panels that require rent data cannot cover 2011-2014 with the current curated Zillow artifact

## Installation

HHP-Lab targets Python 3.12+.

```bash
uv sync --extra dev
uv run hhplab --help
```

## CLI Highlights

Common entry points:

- `hhplab status --json`: scan curated assets, recipe outputs, and prerequisite gaps
- `hhplab aggregate {acs|pit|pep|zori}`: produce standalone CoC aggregate artifacts
- `hhplab build recipe --recipe <file>`: run a recipe build (validation + preflight included)
- `hhplab build recipe-preflight --recipe <file> --json`: readiness report without execution
- `hhplab build recipe-plan --recipe <file> --json`: inspect the resolved task graph while authoring/debugging
- `hhplab validate curated-layout`: check naming and layout policy
- `hhplab list curated`: discover curated data assets

Automation features:

- Most inventory and planning commands support `--json`
- `hhplab --non-interactive ...` and `HHPLAB_NON_INTERACTIVE=1` disable prompts
- `hhplab agents` prints the geography/year matching rules used by the project
- `hhplab build recipe` and `hhplab build recipe-export` accept
  `--asset-store-root` and `--output-root` overrides

## Storage Roots

HHP-Lab now resolves canonical data locations from configurable storage roots.
Resolution precedence is:

- CLI flags: `--asset-store-root`, `--output-root`
- Environment: `HHPLAB_ASSET_STORE_ROOT`, `HHPLAB_OUTPUT_ROOT`
- Repo config: `hhplab.yaml`
- User config: `~/.config/hhplab/config.yaml`
- Built-in defaults

Built-in defaults preserve the historical layout:

- `asset_store_root = <project_root>/data`
- `output_root = <project_root>/outputs`

Relative paths are resolved by source:

- CLI flags and environment variables are relative to the current working directory
- `hhplab.yaml` values are relative to the repo root
- `~/.config/hhplab/config.yaml` values are relative to `~/.config/hhplab/`

Internal curated assets resolve under `asset_store_root/curated/...`. Recipe
panels and their manifest sidecars resolve under `output_root/`.

## Quick Start

Recipe-driven builds are the only supported end-to-end orchestration workflow.

Human path:

```bash
uv run hhplab build recipe --recipe recipes/metro25-glynnfox.yaml
```

Automation / CI path:

```bash
uv run hhplab status --json
uv run hhplab build recipe-preflight --recipe recipes/metro25-glynnfox.yaml --json
uv run hhplab build recipe --recipe recipes/metro25-glynnfox.yaml --json
```

Optional task-graph inspection while authoring/debugging:

```bash
uv run hhplab build recipe-plan --recipe recipes/metro25-glynnfox.yaml --json
```

Most recipe builds consume curated source artifacts directly. Use the
`aggregate` command group only when you want standalone CoC aggregate
artifacts or when a specific recipe explicitly points at those outputs.

## Legacy CLI Migration

Named build orchestration has been retired.

- Use `hhplab build recipe --recipe <file>` as the single end-to-end build entrypoint.
- Use `hhplab build recipe-preflight --recipe <file> --json` as the no-execute readiness gate.
- Use `hhplab build recipe-plan --recipe <file> --json` only to inspect resolved tasks while authoring or debugging.
- Use low-level commands such as `ingest`, `generate xwalks`, and `aggregate` only to materialize curated prerequisites or standalone debug artifacts.

If you previously used legacy commands/flags:

- `hhplab status --builds-dir ...` -> `hhplab status --output-root ...`
- `hhplab aggregate <dataset> --build <name>` -> `hhplab aggregate <dataset> --years <spec>`
- `hhplab generate xwalks --build <name>` -> `hhplab generate xwalks --boundary <year> --tracts <year>`

Recipe outputs now live under `output_root/<recipe-name>/`. Curated prerequisite
artifacts continue to live under `asset_store_root/curated/`.

Bead IDs are the one intentional exception to the rename: issue tracking
remains in the historical `coclab-*` namespace. Existing bead slugs are not
migrated, and new beads should continue using the same `coclab-*` prefix.

## Analysis Geography Support

Use the three geography families differently:

| Geography | Choose it when | Primary identifier | Common artifact family |
| --- | --- | --- | --- |
| `coc` | You want official HUD CoC units with explicit boundary vintages. | `coc_id` | `coc__B...`, `panel__Y...@B...` |
| `metro` | You want the canonical Census metro universe, optionally filtered to a named subset profile such as Glynn/Fox. | `metro_id` + `definition_version` | `metro_universe__...`, `metro_subset_membership__...`, `panel__metro__...` |
| `msa` | You want official Census MSAs / CBSAs. | `msa_id` + `definition_version` | `msa_definitions__...`, `pit__msa__...`, `panel__msa__...` |

`metro` and `msa` are intentionally separate. Use `metro` when you want the
project's metro analysis surface, either as the full canonical universe or as a
declared subset profile over that universe. Use `msa` when you want official
Census MSA / CBSA outputs keyed directly by `msa_id`. Do not treat one as a
renamed version of the other.

For new metro recipes:

- Use `geometry: { type: metro, source: census_msa_2023 }` for the full
  canonical metro universe.
- Add `subset_profile: glynn_fox` and
  `subset_profile_definition_version: glynn_fox_v1` when you want the
  historical 25-metro Glynn/Fox slice.
- Treat legacy `geometry: { type: metro, source: glynn_fox_v1 }` as a
  compatibility shim. Runtime execution resolves it through the canonical metro
  universe plus the Glynn/Fox subset profile, but contributors should prefer
  the explicit subset form in new docs and recipes.

Remaining compatibility shims are intentionally narrow:

- legacy metro recipe inputs may still use `source: glynn_fox_v1`
- legacy metro artifact families such as `metro_definitions__glynn_fox_v1` and
  `metro_county_membership__glynn_fox_v1` still exist for regression coverage
  and migration safety

Removal path:

- keep the shim while committed recipes, examples, and downstream users still
  depend on the legacy form
- prove parity between legacy Glynn/Fox outputs and explicit subset-derived
  canonical outputs in regression tests
- drop the legacy syntax only after the committed recipes and migration docs no
  longer require it

The repository includes committed examples for all three surfaces under
[recipes/examples](recipes/examples/README.md). For the MSA-specific workflow,
see [background/msa_geography.md](background/msa_geography.md) and
[recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml](recipes/examples/msa-census-pit-acs-pep-2020-2021.yaml).

## Map Targets

Recipe targets can emit `map` artifacts in addition to panel-style outputs.
Declare `outputs: [map]` and provide a `map_spec` with one or more overlay
layers:

```yaml
targets:
  - id: coc_map
    geometry: { type: coc, vintage: 2025 }
    outputs: [map]
    map_spec:
      layers:
        - geometry: { type: coc, vintage: 2025 }
          selector_ids: [CO-500]
          label: Primary CoC
          tooltip_fields: [coc_id, coc_name]
          style_mode: distinct
```

Set `style_mode: distinct` when you want the renderer to assign deterministic
per-feature colors within a single layer so adjacent CoCs or multiple selected
MSAs can be told apart visually. Omit it, or use `style_mode: uniform`, when
one shared layer style is preferable.

Map layer prerequisites are geometry-specific:

- `coc`: curated CoC boundaries for the requested boundary vintage, for example
  `hhplab ingest boundaries --source hud_exchange --vintage 2025`
- `msa`: official MSA boundary polygons for the requested definition version and
  county geometry year, for example
  `hhplab ingest msa-boundaries --definition-version census_msa_2023 --year 2023`
- `metro`: generated metro boundary polygons for the requested definition version
  and county geometry vintage, for example
  `hhplab generate metro-boundaries --definition-version glynn_fox_v1 --counties 2025`

Run `uv run hhplab build recipe-preflight --recipe <file> --json` before
execution to surface missing map boundary artifacts with exact remediation
commands.

## Project Layout

- `hhplab/`: Python package and CLI implementation
- `recipes/`: committed example recipes, including Glynn/Fox metro panel builds
- `tests/`: regression coverage for CLI, aggregation, panel assembly, recipes, and metro logic
- `HHP-Lab-manual/`: the full project manual
- `data/`: default local asset store when built-in storage-root defaults are used

## Development

Run the full test suite with:

```bash
uv run --extra dev pytest
```

## License

Copyright (c) 2026, Matt McConnell

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

At least one dependency used within HHP-Lab is subject to its own license terms.
