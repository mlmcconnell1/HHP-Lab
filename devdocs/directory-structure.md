# Directory Structure: ingest, aggregate, build, export

Derived from code review of `coclab/naming.py`, `coclab/builds.py`,
`coclab/raw_snapshot.py`, `coclab/export/copy.py`, and all CLI modules.

All paths are relative to the project root (enforced by `_check_working_directory()`
in `coclab/cli/main.py`).

## Notation key

Filenames use temporal shorthand (see `temporal-terminology.md`):

    B = boundary vintage    T = tract vintage     C = county vintage
    A = ACS end-year        P = PIT year           Z = ZORI year
    Y = panel year range    @ = "analyzed using"   x = crosswalk join
    w = weighting method    m = yearly collapse    v = release vintage

## Full tree

```
project-root/
│
├── data/
│   ├── raw/                                         ── written by: ingest ──
│   │   │
│   │   ├── hud_exchange/
│   │   │   └── B{boundary_vintage}_{date}/
│   │   │       ├── response.ndjson                        ArcGIS API responses
│   │   │       ├── request.json                           request metadata
│   │   │       └── manifest.json                          pagination & hash
│   │   │
│   │   ├── hud_opendata/
│   │   │   └── {boundary_vintage}/
│   │   │       ├── response.ndjson                        ArcGIS API responses
│   │   │       ├── request.json                           request metadata
│   │   │       └── manifest.json                          pagination & hash
│   │   │
│   │   ├── pit/
│   │   │   └── {year}/
│   │   │       ├── {filename}.xlsx | {filename}.xlsb      PIT spreadsheet
│   │   │       └── {filename}.xlsx.meta.json | {filename}.xlsb.meta.json
│   │   │                                                   download sidecar
│   │   │
│   │   ├── census/
│   │   │   ├── {year}/
│   │   │   │   ├── tracts/
│   │   │   │   │   └── tl_{year}_{fips}_tract.zip         per-state TIGER
│   │   │   │   └── counties/
│   │   │   │       └── tl_{year}_us_county.zip            national TIGER
│   │   │   │           (2010 exception: tl_2010_us_county10.zip)
│   │   │   └── tract_relationship/
│   │   │       └── tab20_tract20_tract10_natl.txt         Census tract xwalk source
│   │   │
│   │   ├── nhgis/
│   │   │   └── {year}/
│   │   │       ├── tracts/
│   │   │       │   └── {shapefile}.zip                    NHGIS extract ZIP
│   │   │       └── counties/
│   │   │           └── {shapefile}.zip                    NHGIS extract ZIP
│   │   │
│   │   ├── zori/
│   │   │   └── zori__{geo}__{date}.csv                    Zillow ZORI CSV
│   │   │
│   │   ├── pep/
│   │   │   └── pep_county__v{vintage}__{date}.csv         Census PEP CSV
│   │   │
│   │   └── acs_tract/
│   │       └── {snapshot_id}/
│   │           ├── response.ndjson                        Census API responses
│   │           ├── request.json                           request metadata
│   │           └── manifest.json                          pagination & hash
│   │
│   └── curated/                          ── written by: ingest & build ──
│       │
│       ├── coc_boundaries/
│       │   └── coc__B{year}.parquet                       CoC boundary geometries
│       │
│       ├── census/
│       │   ├── tracts__T{year}.parquet                    TIGER tract geometries
│       │   ├── counties__C{year}.parquet                  TIGER county geometries
│       │   └── tract_relationship__T{from}xT{to}.parquet  Census tract xwalk
│       │
│       ├── pit/
│       │   ├── pit__P{year}.parquet                       PIT counts (ingest)
│       │   └── pit_vintage__P{year}.parquet               PIT vintage file (ingest)
│       │
│       ├── acs/
│       │   ├── acs_tracts__A{acs}xT{tract}.parquet        ACS tract pop (ingest)
│       │   └── county_weights__A{acs}__w{wt}.parquet      county weights (build)
│       │
│       ├── xwalks/                                        ── build xwalks ──
│       │   ├── xwalk__B{b}xT{t}.parquet                  boundary-to-tract
│       │   └── xwalk__B{b}xC{c}.parquet                  boundary-to-county
│       │
│       ├── measures/                                      ── build measures ──
│       │   └── measures__A{acs}@B{b}.parquet              CoC-level ACS measures
│       │
│       ├── zori/                                          ── ingest & build ──
│       │   ├── zori__{geo}__Z{year}.parquet               ingested ZORI (ingest)
│       │   ├── zori__A{a}@B{b}xC{c}__w{wt}.parquet       CoC monthly (build)
│       │   └── zori_yearly__...parquet                    CoC yearly (build)
│       │
│       ├── pep/                                           ── ingest & build ──
│       │   ├── pep_county__v{vintage}.parquet             single vintage (ingest)
│       │   ├── pep_county__combined.parquet               multi-vintage (ingest)
│       │   └── coc_pep__B{b}xC{c}__w{wt}__{s}_{e}.parquet  CoC-agg (build)
│       │
│       ├── panel/                                         ── build panel ──
│       │   └── panel__Y{start}-{end}@B{b}.parquet         merged panel
│       │
│       ├── source_registry.parquet                        SHA-256 provenance log
│       └── boundary_registry.parquet                      boundary vintage log
│
├── builds/                                ── written by: build & aggregate ──
│   └── {name}/
│       ├── manifest.json                                  build manifest (v1)
│       ├── base/
│       │   └── coc__B{year}.parquet                       pinned boundary copy
│       └── data/
│           ├── raw/                                       (build-scoped raw)
│           └── curated/
│               ├── xwalks/
│               │   ├── xwalk__B{b}xT{t}.parquet
│               │   └── xwalk__B{b}xC{c}.parquet
│               ├── measures/
│               │   └── measures__A{acs}@B{b}.parquet
│               ├── zori/
│               │   ├── zori__A{a}@B{b}xC{c}__w{wt}.parquet
│               │   └── zori_yearly__...parquet
│               ├── pep/
│               │   └── coc_pep__B{b}xC{c}__w{wt}__{s}_{e}.parquet
│               ├── pit/
│               │   └── pit__P{year}@B{year}.parquet
│               └── panel/
│                   └── panel__Y{start}-{end}@B{b}.parquet
│
└── exports/                                      ── written by: export ──
    ├── export-{N}/
    │   ├── MANIFEST.json                                  export metadata
    │   ├── README.md                                      human-readable summary
    │   ├── data/
    │   │   ├── panels/
    │   │   │   └── panel__Y{s}-{e}@B{b}.parquet
    │   │   └── inputs/
    │   │       ├── boundaries/
    │   │       │   └── coc__B{year}.parquet
    │   │       ├── xwalks/
    │   │       │   └── xwalk__B{b}xC{c}.parquet
    │   │       ├── pit/
    │   │       │   └── pit__P{year}.parquet
    │   │       ├── rents/
    │   │       │   └── zori__*.parquet
    │   │       └── acs/
    │   │           └── measures__*.parquet
    │   ├── diagnostics/                                   validation outputs
    │   └── codebook/                                      column documentation
    │
    └── export-{N}.tar.gz                                  optional archive
```

## Data-flow summary

```
                 INGEST                    BUILD / AGGREGATE            EXPORT
           ┌───────────────┐           ┌──────────────────────┐    ┌───────────┐
           │               │           │                      │    │           │
internet ──┤► data/raw/    │           │  data/curated/       │    │ exports/  │
           │               ├──►parquet─┤  (xwalks, measures,  ├───►│ export-N/ │
           │► data/curated/│    files  │   zori, pep, panel)  │    │           │
           │  (boundaries, │           │                      │    └───────────┘
           │   census, pit,│           │  builds/{name}/      │
           │   acs, zori,  │           │  data/curated/       │
           │   pep)        │           │  (build-scoped copy) │
           └───────────────┘           └──────────────────────┘
```

## Command-to-directory mapping

| Command                          | Reads from              | Writes to                     |
|----------------------------------|-------------------------|-------------------------------|
| `coclab ingest boundaries`       | internet                | `data/raw/hud_exchange/B{vintage}_{date}/` or `data/raw/hud_opendata/{boundary_vintage}/`, `data/curated/coc_boundaries/` |
| `coclab ingest pit`              | internet                | `data/raw/pit/`, `data/curated/pit/`              |
| `coclab ingest pit-vintage`      | internet                | `data/raw/pit/`, `data/curated/pit/`              |
| `coclab ingest census`           | internet                | `data/raw/census/`, `data/curated/census/`        |
| `coclab ingest nhgis`            | internet (IPUMS API)    | `data/raw/nhgis/`, `data/curated/census/`         |
| `coclab ingest tract-relationship`| internet               | `data/raw/census/`, `data/curated/census/`        |
| `coclab ingest acs-population`   | internet                | `data/raw/acs_tract/`, `data/curated/acs/`        |
| `coclab ingest zori`             | internet                | `data/raw/zori/`, `data/curated/zori/`            |
| `coclab ingest pep`              | internet                | `data/raw/pep/`, `data/curated/pep/`              |
| `coclab build create`            | `data/curated/coc_boundaries/` | `builds/{name}/`                           |
| `coclab build xwalks`            | `curated/census/`, `curated/coc_boundaries/` | `curated/xwalks/` (or build) |
| `coclab build measures`          | `curated/xwalks/`, internet (Census ACS API) | `curated/measures/` (or build)            |
| `coclab build zori`              | `curated/zori/`, `curated/xwalks/` | `curated/zori/` (or build)               |
| `coclab build pep`               | `curated/pep/`, `curated/xwalks/` | `curated/pep/` (or build)                |
| `coclab build panel`             | `curated/` (multiple)   | `curated/panel/` (or build)                       |
| `coclab aggregate pep`           | `curated/pep/`, `curated/xwalks/` | `builds/{name}/data/curated/pep/`        |
| `coclab aggregate pit`           | `curated/pit/`, `curated/xwalks/` | `builds/{name}/data/curated/pit/`        |
| `coclab aggregate acs`           | `curated/xwalks/`, `curated/acs/` | `builds/{name}/data/curated/measures/`   |
| `coclab aggregate zori`          | `curated/zori/`, `curated/xwalks/` | `builds/{name}/data/curated/zori/`      |
| `coclab export bundle`           | `data/curated/` or `builds/` | `exports/export-{N}/`                        |
