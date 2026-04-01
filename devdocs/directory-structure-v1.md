# Directory Structure: ingest, aggregate, build, export

Derived from code review of `coclab/naming.py`, `coclab/builds.py`,
`coclab/raw_snapshot.py`, `coclab/export/copy.py`, and all CLI modules.

This document shows the built-in default layout. The current runtime resolves
canonical locations from storage roots:

- `asset_store_root = <project_root>/data` by default
- `output_root = <project_root>/data/curated/panel` by default

So `data/raw/...` and `data/curated/...` below should be read as
`asset_store_root/raw/...` and `asset_store_root/curated/...`, while recipe
panel outputs resolve from `output_root/`.

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
│   │   ├── <ingest_type>/                                canonical first segment
│   │   │   └── <year>/                                   canonical second segment
│   │   │       ├── <variant_or_run_id>/                  required when collisions possible
│   │   │       │   ├── response.ndjson
│   │   │       │   ├── request.json
│   │   │       │   └── manifest.json
│   │   │       └── <artifact_filename>                   allowed when filenames are unique
│   │   │
│   │   ├── pit/2024/2007-2024-PIT-Counts-by-CoC.xlsb    example file ingest
│   │   ├── tiger/2017/tracts/tl_2017_06_tract.zip        example multi-file ingest
│   │   ├── nhgis/2010/tracts/us_tract_2010_tl2010.zip    example extract ingest
│   │   ├── acs5_tract/2023/full/response.ndjson          example API ingest
│   │   ├── acs5_county/2023/B25003__renter_households/response.ndjson
│   │   ├── hud_exchange/2025/2026-02-07/response.ndjson
│   │   ├── hud_opendata/2026/2026-02-12/response.ndjson
│   │   ├── zori/2026/zori__county__2026-02-07.csv
│   │   ├── pep/2024/pep_county__v2024__2026-02-02.csv
│   │   └── tiger/2020/tract_relationship/tab20_tract20_tract10_natl.txt
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
│       │   ├── acs5_tracts__A{acs}xT{tract}.parquet       ACS 5yr tract pop (ingest)
│       │   └── county_weights__A{acs}__w{wt}.parquet      county weights (build)
│       │
│       ├── xwalks/                                        ── build xwalks ──
│       │   ├── xwalk__B{b}xT{t}.parquet                  boundary-to-tract
│       │   └── xwalk__B{b}xC{c}.parquet                  boundary-to-county
│       │
│       ├── measures/                                      ── build measures ──
│       │   └── measures__A{acs}@B{b}xT{t}.parquet         CoC-level ACS measures
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
│               │   └── measures__A{acs}@B{b}xT{t}.parquet
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
| `coclab ingest boundaries`       | internet                | `data/raw/hud_exchange/<year>/<run_id>/` or `data/raw/hud_opendata/<year>/<run_id>/`, `data/curated/coc_boundaries/` |
| `coclab ingest pit`              | internet                | `data/raw/pit/<year>/...`, `data/curated/pit/`              |
| `coclab ingest pit-vintage`      | internet                | `data/raw/pit/<year>/...`, `data/curated/pit/`              |
| `coclab ingest tiger`            | internet                | `data/raw/tiger/<year>/...`, `data/curated/tiger/`          |
| `coclab ingest nhgis`            | internet (IPUMS API)    | `data/raw/nhgis/<year>/...`, `data/curated/tiger/`          |
| `coclab ingest tract-relationship`| internet               | `data/raw/tiger/<year>/tract_relationship/`, `data/curated/tiger/`          |
| `coclab ingest acs5-tract`       | internet                | `data/raw/acs5_tract/<year>/<variant>/...`, `data/curated/acs/`       |
| `coclab ingest zori`             | internet                | `data/raw/zori/<year>/...`, `data/curated/zori/`            |
| `coclab ingest pep`              | internet                | `data/raw/pep/<year>/...`, `data/curated/pep/`              |
| `coclab generate xwalks`         | `curated/census/`, `curated/coc_boundaries/` | `curated/xwalks/` (or build) |
| `coclab aggregate acs`           | `curated/xwalks/`, `curated/acs/` | `data/curated/measures/` (or `builds/{name}/...`) |
| `coclab aggregate zori`          | `curated/zori/`, `curated/xwalks/` | `data/curated/zori/` (or `builds/{name}/...`) |
| `coclab aggregate pep`           | `curated/pep/`, `curated/xwalks/` | `data/curated/pep/` (or `builds/{name}/...`) |
| `coclab aggregate pit`           | `curated/pit/`, `curated/xwalks/` | `data/curated/pit/` (or `builds/{name}/...`) |
| `coclab build recipe`            | `data/curated/` (multiple) | `data/curated/panel/`                     |
| `coclab build recipe-export`     | `data/curated/` or `builds/` | `exports/export-{N}/`                     |
