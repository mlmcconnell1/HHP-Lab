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
        RAW[data/raw]
        CURATED[data/curated]
    end

    subgraph BuildLayer[Build Layer]
        BUILD[builds/<name>]
        XWALK[coclab generate xwalks]
        AGG[coclab aggregate {acs,zori,pep,pit}]
    end

    subgraph RecipeLayer[Recipe Layer]
        RECIPE[coclab build recipe]
        PLAN[planner + executor]
        PANEL[data/curated/panel]
        RMAN[.manifest.json sidecar]
    end

    subgraph Export[Export]
        BUNDLE[coclab build export]
        MANIFEST[MANIFEST.json]
    end

    Sources --> Ingest
    CURATED --> BuildLayer
    BuildLayer --> RecipeLayer
    RecipeLayer --> Export
```

## Major Subsystems

- `coclab/cli/`: Typer CLI command groups
- `coclab/recipe/`: schema, adapters, planner, executor, recipe manifests
- `coclab/builds.py`: build scaffolding, base-asset pinning, build manifests
- `coclab/xwalks/`: tract/county crosswalk generation
- `coclab/measures/`, `coclab/rents/`, `coclab/pep/`, `coclab/pit/`: dataset-specific ingestion and aggregation
- `coclab/export/`: artifact selection, copying, bundle `MANIFEST.json`

## Storage Model

- Global curated assets live under `data/curated/`
- Build-scoped artifacts live under `builds/<name>/data/curated/`
- Recipe panel outputs persist to canonical `data/curated/panel/` when target outputs include `panel`

## Architectural Intent

- Keep raw source acquisition separate from analysis-ready artifacts
- Keep alignment and weighting policy explicit in code paths and metadata
- Prefer deterministic naming + manifests over implicit discovery

---

**Previous:** [[02-Installation]] | **Next:** [[04-CLI-Reference]]
