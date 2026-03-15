# Python API

This chapter documents stable import surfaces that exist in the current codebase.

## Core Imports

```python
from coclab.ingest import ingest_hud_exchange, ingest_hud_opendata
from coclab.registry import list_boundaries, latest_vintage
from coclab.census.ingest import (
    ingest_tiger_tracts,
    ingest_tiger_counties,
    ingest_tract_relationship,
)
from coclab.xwalks import (
    build_coc_tract_crosswalk,
    build_coc_county_crosswalk,
    build_tract_crosswalk,
    build_county_crosswalk,
)
from coclab.measures import aggregate_to_coc, aggregate_to_geo
from coclab.panel import (
    AlignmentPolicy,
    PANEL_COLUMNS,
    METRO_PANEL_COLUMNS,
    PanelRequest,
    build_panel,
    run_conformance,
    save_panel,
)
```

## Recipe API

```python
from pathlib import Path

from coclab.recipe.loader import load_recipe
from coclab.recipe.executor import execute_recipe
from coclab.recipe.default_adapters import register_defaults

register_defaults()
recipe = load_recipe(Path("recipes/test.yaml"))
results = execute_recipe(recipe)
```

Notes:
- Call `register_defaults()` before adapter validation/execution in custom code.
- `execute_recipe()` runs all pipelines defined in the recipe.

## Build Helpers

```python
from coclab.builds import (
    build_curated_dir,
    build_manifest_path,
    read_build_manifest,
    require_build_dir,
    resolve_build_dir,
)

build_dir = require_build_dir("demo")
manifest = read_build_manifest(build_dir)
curated_dir = build_curated_dir(build_dir)
```

## Provenance Helpers

```python
from coclab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

prov = ProvenanceBlock(
    boundary_vintage="2025",
    tract_vintage="2020",
    acs_vintage="2023",
    weighting="population",
    geo_type="coc",
)
# write_parquet_with_provenance(df, path, prov)
# meta = read_provenance(path)
```

## Notes on Stability

- `coclab.__init__` currently re-exports only `census`, `measures`, `provenance`, and `xwalks`
- panel helpers are stable through `coclab.panel`
- geometry-neutral APIs now exist alongside CoC-specific wrappers (`aggregate_to_geo`, `build_tract_crosswalk`, `build_county_crosswalk`)

## Caution on Internal Functions

Many modules expose additional functions not intended as stable public API. Prefer:
- documented package-level exports (`__init__.py`)
- CLI commands for end-to-end workflows
- recipe schema + executor for composition

---

**Previous:** [[05-Recipe-Format]] | **Next:** [[07-Data-Model]]
