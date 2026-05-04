# Architecture

## System Layers

```mermaid
flowchart TB
    subgraph Sources[Source Systems]
        HUD[HUD boundaries + PIT]
        CENSUS[Census TIGER + ACS + PEP]
        ZILLOW[Zillow ZORI]
    end

    subgraph Ingest[Ingestion]
        INGEST[hhplab ingest ...]
        RAW[asset_store_root/raw]
        CURATED[asset_store_root/curated]
    end

    subgraph BuildLayer[Build Layer]
        XWALK[hhplab generate xwalks]
        AGG[hhplab aggregate {acs,zori,pep,pit}]
    end

    subgraph RecipeLayer[Recipe Layer]
        RECIPE[hhplab build recipe]
        PLAN[planner + executor]
        PANEL[output_root]
        RMAN[.manifest.json sidecar]
    end

    subgraph Export[Export]
        BUNDLE[hhplab build recipe-export]
        MANIFEST[.manifest.json sidecar]
    end

    Sources --> Ingest
    CURATED --> BuildLayer
    BuildLayer --> RecipeLayer
    RecipeLayer --> Export
```

## Major Subsystems

- `hhplab/cli/`: Typer CLI command groups
- `hhplab/analysis_geo.py`: analysis geography abstraction (`AnalysisGeometryRef`, canonical `geo_type`/`geo_id` columns)
- `hhplab/recipe/`: schema, adapters, planner, executor, recipe manifests
- `hhplab/builds.py`: build directory and manifest helpers
- `hhplab/xwalks/`: tract/county crosswalk generation (geography-neutral via `geo_id_col` parameter)
- `hhplab/measures/`, `hhplab/rents/`, `hhplab/pep/`, `hhplab/pit/`: dataset-specific ingestion and aggregation (generalized to arbitrary target geographies)
- `hhplab/metro/`: metro definition data, PIT/ACS/PEP/ZORI aggregation to metro, validation, I/O
- `hhplab/acs/ingest/metro_acs1.py`: ACS 1-year CBSA-level detailed-table ingestion for metro-native measures

## Storage Model

- Reusable ingests, crosswalks, registries, and aggregate artifacts live under
  `asset_store_root/curated/`
- Raw snapshots live under `asset_store_root/raw/`
- Recipe panel outputs persist under `output_root/<recipe-name>/` when target
  outputs include `panel`
- Built-in defaults are:
  `asset_store_root = <project_root>/data`,
  `output_root = <project_root>/outputs`

## Architectural Intent

- Keep raw source acquisition separate from analysis-ready artifacts
- Keep alignment and weighting policy explicit in code paths and metadata
- Prefer deterministic naming + manifests over implicit discovery

---

**Previous:** [[02-Installation]] | **Next:** [[04-CLI-Reference]]
