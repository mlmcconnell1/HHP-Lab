# CoC Lab

CoC Lab is a Python toolkit and CLI for building analysis-ready homelessness panels from HUD Continuum of Care data, Census geography and population products, ACS tract measures, and Zillow rent data.

The project started as CoC boundary infrastructure, but it now supports two analysis geography families:

- `coc`: HUD Continuum of Care geographies with explicit boundary vintages
- `metro`: synthetic researcher-defined metro geographies keyed by a `definition_version`

The current metro implementation includes the 25 Glynn/Fox metros from *Dynamics of Homelessness in Urban America* via `glynn_fox_v1`.

Full operational documentation lives in [manual-obsidian/CoC-Lab-Manual.md](manual-obsidian/CoC-Lab-Manual.md).

## What CoC Lab Does

- Ingests curated source data for CoC boundaries, TIGER tracts and counties, ACS, PEP, PIT, and ZORI
- Builds tract-to-CoC and county-to-CoC crosswalks
- Aggregates ACS, PIT, PEP, and ZORI inputs into analysis-ready outputs
- Assembles panel datasets across years with provenance metadata embedded in parquet artifacts
- Supports recipe-driven builds, export bundles, curated-layout validation, and machine-readable CLI output for automation

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

CoC Lab targets Python 3.12+.

```bash
uv sync --extra dev
uv run coclab --help
```

The CLI entrypoint is `coclab`.

## CLI Highlights

Common entry points:

- `coclab status --json`: scan curated assets and named builds
- `coclab build create`: create a named build scaffold
- `coclab aggregate {acs|pit|pep|zori}`: produce CoC-scoped aggregate artifacts for a build
- `coclab build panel`: assemble an analysis geography x year panel
- `coclab build recipe --recipe <file> --dry-run --json`: validate and plan recipe-driven builds
- `coclab list artifacts --build <build> --json`: discover artifacts deterministically
- `coclab validate curated-layout`: check naming and layout policy
- `coclab build export --build <build> --name <name>`: create a portable export bundle

Automation features:

- Most inventory and planning commands support `--json`
- `coclab --non-interactive ...` and `COCLAB_NON_INTERACTIVE=1` disable prompts
- `coclab agents` prints the geography/year matching rules used by the project

## Quick Start

Create a CoC build and assemble a panel:

```bash
uv run coclab build create --name demo --years 2020-2024
uv run coclab aggregate pit --build demo
uv run coclab aggregate acs --build demo --weighting population
uv run coclab build panel --build demo --start 2020 --end 2024
```

Inspect readiness and artifacts:

```bash
uv run coclab status --json
uv run coclab list artifacts --build demo --json
uv run coclab validate curated-layout
```

## Metro Geography Support

Metro support is now a first-class part of the analysis model. The key differences from CoC builds are:

- metro builds use `geo_type=metro`
- metro outputs are keyed by `geo_id` or `metro_id`, not synthetic `coc_id` values
- metro builds require a `definition_version`, currently `glynn_fox_v1`
- metro manifests do not pin CoC boundary base assets the way CoC builds do

Create a metro build scaffold:

```bash
uv run coclab build create \
  --name gf-metro \
  --years 2015-2016 \
  --geo-type metro \
  --definition-version glynn_fox_v1
```

Recipe-driven metro workflows are the most direct way to build metro panels. The repository includes ready-made examples in [recipes/glynn_fox_metro_panel.yaml](recipes/glynn_fox_metro_panel.yaml) and [recipes/glynn_fox_metro_panel_no_zori.yaml](recipes/glynn_fox_metro_panel_no_zori.yaml).

Plan or execute one of those recipes with:

```bash
uv run coclab build recipe-plan --recipe recipes/glynn_fox_metro_panel.yaml --json
uv run coclab build recipe --recipe recipes/glynn_fox_metro_panel.yaml --dry-run --json
```

If metro aggregate artifacts already exist for a build, `coclab build panel` can also assemble a metro panel when the build manifest records `geo_type=metro` and `definition_version=glynn_fox_v1`.

## Project Layout

- `coclab/`: Python package and CLI implementation
- `recipes/`: committed example recipes, including Glynn/Fox metro panel builds
- `tests/`: regression coverage for CLI, aggregation, panel assembly, recipes, and metro logic
- `manual-obsidian/`: the full project manual
- `data/`: curated and raw data artifacts used by builds

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

At least one dependency used within CoC Lab is subject to its own license terms.
