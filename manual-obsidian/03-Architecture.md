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
        INGEST[coclab ingest ...]
        RAW[asset_store_root/raw]
        CURATED[asset_store_root/curated]
    end

    subgraph BuildLayer[Build Layer]
        XWALK[coclab generate xwalks]
        AGG[coclab aggregate {acs,zori,pep,pit}]
    end

    subgraph RecipeLayer[Recipe Layer]
        RECIPE[coclab build recipe]
        PLAN[planner + executor]
        PANEL[output_root]
        RMAN[.manifest.json sidecar]
    end

    subgraph Export[Export]
        BUNDLE[coclab build recipe-export]
        MANIFEST[.manifest.json sidecar]
    end

    Sources --> Ingest
    CURATED --> BuildLayer
    BuildLayer --> RecipeLayer
    RecipeLayer --> Export
```

## Major Subsystems

- `coclab/cli/`: Typer CLI command groups
- `coclab/analysis_geo.py`: analysis geography abstraction (`AnalysisGeometryRef`, canonical `geo_type`/`geo_id` columns)
- `coclab/recipe/`: schema, adapters, planner, executor, recipe manifests
- `coclab/builds.py`: build directory and manifest helpers
- `coclab/xwalks/`: tract/county crosswalk generation (geography-neutral via `geo_id_col` parameter)
- `coclab/measures/`, `coclab/rents/`, `coclab/pep/`, `coclab/pit/`: dataset-specific ingestion and aggregation (generalized to arbitrary target geographies)
- `coclab/metro/`: metro definition data, PIT/ACS/PEP/ZORI aggregation to metro, validation, I/O
- `coclab/acs/ingest/metro_acs1.py`: ACS 1-year CBSA-level unemployment ingestion for metros

## Storage Model

- Reusable ingests, crosswalks, registries, and aggregate artifacts live under
  `asset_store_root/curated/`
- Raw snapshots live under `asset_store_root/raw/`
- Recipe panel outputs persist under `output_root/` when target outputs include
  `panel`
- Built-in defaults preserve the old layout:
  `asset_store_root = <project_root>/data`,
  `output_root = <project_root>/data/curated/panel`

## Architectural Intent

- Keep raw source acquisition separate from analysis-ready artifacts
- Keep alignment and weighting policy explicit in code paths and metadata
- Prefer deterministic naming + manifests over implicit discovery

---

**Previous:** [[02-Installation]] | **Next:** [[04-CLI-Reference]]
