# Module Reference

This chapter is an orientation map of active modules in the current codebase.

## CLI Layer

- `hhplab/cli/main.py`: top-level Typer app and command registration
- `hhplab/cli/build_xwalks.py`: crosswalk generation
- `hhplab/cli/aggregate.py`: dataset aggregation commands (`acs`, `zori`, `pep`, `pit`)
- `hhplab/cli/status.py`: one-shot environment readiness report (`hhplab status`)
- `hhplab/cli/list_curated.py`: curated dataset discovery with metadata (row counts, columns, sizes)
- `hhplab/cli/list_*.py`: dataset discovery commands with optional JSON output
- `hhplab/cli/ingest_acs1_metro.py`: ACS 1-year CBSA-level metro unemployment ingestion
- `hhplab/cli/diagnostics.py` and `hhplab/cli/panel_diagnostics.py`: diagnostics commands with optional JSON output
- `hhplab/cli/recipe.py`: recipe execution/provenance/export commands
- `hhplab/cli/migrate_curated.py`: curated data migration utilities

## Recipe System

- `hhplab/recipe/recipe_schema.py`: versioned recipe schema models (including `CohortSelector`, `TemporalFilter`)
- `hhplab/recipe/loader.py`: YAML loading + schema dispatch
- `hhplab/recipe/adapters.py`: semantic validation registries
- `hhplab/recipe/default_*.py`: built-in adapter registration (including `default_dataset_adapters.py`)
- `hhplab/recipe/planner.py`: deterministic task planning
- `hhplab/recipe/executor.py`: runtime execution engine
- `hhplab/recipe/preflight.py`: no-execute readiness validation (plan-scoped path checks, support-dataset probes)
- `hhplab/recipe/probes.py`: dataset probe helpers for preflight validation
- `hhplab/recipe/manifest.py`: consumed-asset manifests and recipe bundle export

## Analysis Geography

- `hhplab/analysis_geo.py`: canonical `geo_type`/`geo_id` abstraction, `AnalysisGeometryRef` dataclass, DataFrame helpers (`resolve_geo_col`, `infer_geo_type`, `ensure_canonical_geo_columns`)

## Build and Provenance

- `hhplab/builds.py`: build directory and manifest helpers
- `hhplab/provenance.py`: Parquet metadata embedding/reading
- `hhplab/naming.py`: canonical filename/path conventions (including metro-specific naming functions)

## Data-Domain Modules

- `hhplab/hud/`: HUD boundary ingesters
- `hhplab/bls/`: BLS LAUS helpers and metro-native ingest
- `hhplab/census/ingest/`: TIGER and tract-relationship ingestion
- `hhplab/xwalks/`: tract and county crosswalk construction
- `hhplab/measures/`: ACS aggregation + diagnostics
- `hhplab/acs/`: ACS ingest/translation support (including `ingest/metro_acs1.py` for ACS 1-year CBSA data, `variables_acs1.py` for B23025 variable definitions)
- `hhplab/pit/`: PIT ingest, registry, QA
- `hhplab/pep/`: PEP ingest, aggregation, and diagnostics
- `hhplab/rents/`: ZORI ingest, weighting, aggregation, diagnostics
- `hhplab/panel/`: panel diagnostics and assembly internals
- `hhplab/metro/`: metro analysis geography module
  - `hhplab/metro/definitions.py`: canonical metro-universe definitions plus the Glynn/Fox subset profile
  - `hhplab/metro/pit.py`: PIT aggregation from CoC to metro via CoC membership
  - `hhplab/metro/acs.py`: ACS aggregation from tracts to metro via county membership
  - `hhplab/metro/pep.py`: PEP aggregation from counties to metro via county membership
  - `hhplab/metro/zori.py`: ZORI aggregation from counties to metro via county membership
  - `hhplab/metro/validate.py`: metro artifact validation (ID formats, referential integrity, counts)
  - `hhplab/metro/io.py`: read/write curated metro definition artifacts

## Utilities and Supporting Modules

- `hhplab/geo/`: GeoParquet I/O (`read_geoparquet`, `write_geoparquet`), boundary validation, CRS normalization, geometry hashing, CT planning regions
- `hhplab/viz/`: Folium-based interactive map rendering (`render_coc_map`)
- `hhplab/nhgis/`: NHGIS extraction support for pre-2020 tract data
- `hhplab/source_registry.py`: external source tracking with SHA-256 hashes (`register_source`, `check_source_changed`, `list_sources`)
- `hhplab/year_spec.py`: year-spec parser for ranges/lists used throughout CLI (e.g., `2018-2024`, `2018,2020,2022`)
- `hhplab/raw_snapshot.py`: raw data snapshot retention utilities
- `hhplab/curated_policy.py`: curated layout policy enforcement and validation
- `hhplab/curated_migrate.py`: curated data migration utilities
- `hhplab/audit_panels.py`: metro audit panel utilities

## Guidance

- Prefer CLI or recipe interfaces for end-to-end workflows.
- Treat module internals as implementation details unless explicitly exported via package `__init__.py`.

---

**Previous:** [[13-Bundle-Layout]] | **Next:** [[15-Development]]
