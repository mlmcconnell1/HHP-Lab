# Python API

This chapter documents stable import surfaces that exist in the current codebase.

## Core Imports

```python
from hhplab.hud import ingest_hud_exchange, ingest_hud_opendata
from hhplab.registry import list_boundaries, latest_vintage
from hhplab.census.ingest import (
    ingest_tiger_tracts,
    ingest_tiger_counties,
    ingest_tract_relationship,
)
from hhplab.xwalks import (
    build_coc_tract_crosswalk,
    build_coc_county_crosswalk,
    build_tract_crosswalk,
    build_county_crosswalk,
)
from hhplab.measures import aggregate_to_coc, aggregate_to_geo
from hhplab.panel import (
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

from hhplab.config import StorageConfig, load_config
from hhplab.paths import curated_dir, output_root
from hhplab.recipe.loader import load_recipe
from hhplab.recipe.executor import execute_recipe, resolve_pipeline_artifacts
from hhplab.recipe.default_adapters import register_defaults

register_defaults()
recipe = load_recipe(Path("recipes/test.yaml"))
results = execute_recipe(recipe)

# Resolve output paths for a pipeline with explicit storage roots
cfg = load_config(
    asset_store_root=Path("/srv/hhplab-assets"),
    output_root=Path("/srv/hhplab-outputs"),
)
artifacts = resolve_pipeline_artifacts(
    recipe,
    "build_coc_panel",
    storage_config=cfg,
)
# {"panel_path": "/srv/hhplab-outputs/my-recipe/panel__...", "manifest_path": "..."}
```

Notes:
- Call `register_defaults()` before adapter validation/execution in custom code.
- `execute_recipe()` runs all pipelines defined in the recipe.
- `resolve_pipeline_artifacts()` returns canonical output paths for a pipeline's declared outputs (`panel_path`, `manifest_path`, `diagnostics_path`).
- `load_config()` returns a `StorageConfig` resolved from CLI-style overrides,
  env vars, repo config, user config, and defaults.
- `curated_dir()` and `output_root()` provide stable path helpers for code that
  should follow the configured storage-root layout.

## Storage and Recipe Helpers

Use storage-root helpers plus recipe manifests/artifact resolution instead of
legacy named-build helpers.

```python
from pathlib import Path

from hhplab.config import load_config
from hhplab.paths import curated_dir, output_root
from hhplab.recipe.manifest import read_manifest

cfg = load_config()
measures_dir = curated_dir("measures", cfg)
recipe_outputs = output_root(cfg) / "my-recipe"
manifest = read_manifest(recipe_outputs / "panel__Y2020-2021@B2025.manifest.json")
```

## Provenance Helpers

```python
from hhplab.provenance import ProvenanceBlock, read_provenance, write_parquet_with_provenance

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

- `hhplab.__init__` currently re-exports only `census`, `measures`, `provenance`, and `xwalks`
- panel helpers are stable through `hhplab.panel`
- geometry-neutral APIs now exist alongside CoC-specific wrappers (`aggregate_to_geo`, `build_tract_crosswalk`, `build_county_crosswalk`)

## Caution on Internal Functions

Many modules expose additional functions not intended as stable public API. Prefer:
- documented package-level exports (`__init__.py`)
- CLI commands for end-to-end workflows
- recipe schema + executor for composition

---

**Previous:** [[05-Recipe-Format]] | **Next:** [[07-Data-Model]]
