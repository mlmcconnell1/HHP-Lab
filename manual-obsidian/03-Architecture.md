# Architecture

## System Overview

```mermaid
flowchart TB
    subgraph Sources["Data Sources"]
        HUD_EX[HUD Exchange GIS Tools]
        HUD_OD[HUD Open Data ArcGIS]
    end

    subgraph Ingest["Ingestion Layer"]
        ING_EX[hud_exchange_gis.py]
        ING_OD[hud_opendata_arcgis.py]
    end

    subgraph Processing["Processing Layer"]
        NORM[normalize.py]
        VAL[validate.py]
    end

    subgraph Storage["Storage Layer"]
        RAW[(data/raw/)]
        CURATED[(data/curated/)]
        REG[(boundary_registry.parquet)]
        SRC[(source_registry.parquet)]
    end

    subgraph Output["Output Layer"]
        VIZ[map_folium.py]
        HTML[Interactive HTML Maps]
    end

    HUD_EX --> ING_EX
    HUD_OD --> ING_OD
    ING_EX --> RAW
    ING_OD --> RAW
    RAW --> NORM
    NORM --> VAL
    VAL --> CURATED
    CURATED --> REG
    CURATED --> SRC
    CURATED --> VIZ
    VIZ --> HTML
```

## Module Structure

```mermaid
graph LR
    subgraph coclab
        CLI[cli/]
        ING[ingest/]
        GEO[geo/]
        REG[registry/]
        SOURCE[source_registry.py]
        VIZ[viz/]
        CENSUS[census/]
        XWALK[xwalks/]
        MEASURES[measures/]
        RENTS[rents/]
        PEP[pep/]
        PIT[pit/]
        PANEL[panel/]
        EXPORT[export/]
        BUILDS[builds.py]
    end

    CLI --> ING
    CLI --> REG
    CLI --> SOURCE
    CLI --> VIZ
    CLI --> XWALK
    CLI --> MEASURES
    CLI --> RENTS
    CLI --> PEP
    CLI --> PIT
    CLI --> PANEL
    CLI --> EXPORT
    CLI --> BUILDS
    ING --> GEO
    VIZ --> REG
    VIZ --> GEO
    XWALK --> CENSUS
    MEASURES --> XWALK
    RENTS --> XWALK
    PANEL --> MEASURES
    PANEL --> PIT
```

## Directory Layout

```
coclab/
  cli/          # CLI commands (Typer)
  geo/          # Geometry normalization and validation
  ingest/       # Data source ingesters
  registry/     # Vintage tracking and version selection
  source_registry.py  # Source hash tracking and change detection
  viz/          # Map rendering (Folium)
  census/       # Census geometry ingestion (TIGER/Line)
    ingest/     # Tract and county downloaders
  xwalks/       # CoC-to-census crosswalk builders
  measures/     # ACS measure aggregation and diagnostics
  acs/          # ACS population ingest, rollup, and cross-check
    ingest/     # Tract population fetcher
  rents/        # ZORI rent data ingestion and aggregation
  pep/          # PEP ingest and aggregation
  pit/          # PIT count ingestion and QA (Phase 3)
    ingest/     # HUD Exchange PIT downloaders and parsers
  panel/        # CoC × year panel assembly (Phase 3)
  export/       # Bundle export and MANIFEST generation
  builds.py     # Named build scaffolds and manifests
  naming.py     # Filename conventions and temporal shorthand
  provenance.py # Parquet provenance helpers
data/
  raw/          # Downloaded source files
  curated/      # Processed GeoParquet files
    census/     # TIGER tract/county geometries
    xwalks/     # CoC-tract and CoC-county crosswalks
    measures/   # CoC-level demographic measures
    acs/        # ACS tract population, rollups, and county weights
    zori/       # ZORI rent data (county and CoC-level)
    pep/        # PEP county and CoC-level data
    pit/        # Canonical PIT count files
    panel/      # CoC × year analysis panels
    source_registry.parquet  # Source ingestion registry
builds/         # Named build scaffolds (each with base/ and data/)
exports/        # Export bundles (export-1, export-2, ...)
tests/          # Test suite including smoke tests
```

---

**Previous:** [[02-Installation]] | **Next:** [[04-CLI-Reference]]
