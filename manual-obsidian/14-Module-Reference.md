# Module Reference

This chapter is an orientation map of active modules in the current codebase.

## CLI Layer

- `coclab/cli/main.py`: top-level Typer app and command registration
- `coclab/cli/build_xwalks.py`: crosswalk generation
- `coclab/cli/aggregate.py`: dataset aggregation commands (`acs`, `zori`, `pep`, `pit`)
- `coclab/cli/status.py`: one-shot environment readiness report (`coclab status`)
- `coclab/cli/list_curated.py`: curated dataset discovery with metadata (row counts, columns, sizes)
- `coclab/cli/list_*.py`: dataset discovery commands with optional JSON output
- `coclab/cli/ingest_acs1_metro.py`: ACS 1-year CBSA-level metro unemployment ingestion
- `coclab/cli/diagnostics.py` and `coclab/cli/panel_diagnostics.py`: diagnostics commands with optional JSON output
- `coclab/cli/recipe.py`: recipe execution/provenance/export commands
- `coclab/cli/migrate_curated.py`: curated data migration utilities

## Recipe System

- `coclab/recipe/recipe_schema.py`: versioned recipe schema models (including `CohortSelector`, `TemporalFilter`)
- `coclab/recipe/loader.py`: YAML loading + schema dispatch
- `coclab/recipe/adapters.py`: semantic validation registries
- `coclab/recipe/default_*.py`: built-in adapter registration (including `default_dataset_adapters.py`)
- `coclab/recipe/planner.py`: deterministic task planning
- `coclab/recipe/executor.py`: runtime execution engine
- `coclab/recipe/preflight.py`: no-execute readiness validation (plan-scoped path checks, support-dataset probes)
- `coclab/recipe/probes.py`: dataset probe helpers for preflight validation
- `coclab/recipe/manifest.py`: consumed-asset manifests and recipe bundle export

## Analysis Geography

- `coclab/analysis_geo.py`: canonical `geo_type`/`geo_id` abstraction, `AnalysisGeometryRef` dataclass, DataFrame helpers (`resolve_geo_col`, `infer_geo_type`, `ensure_canonical_geo_columns`)

## Build and Provenance

- `coclab/builds.py`: build directory and manifest helpers
- `coclab/provenance.py`: Parquet metadata embedding/reading
- `coclab/naming.py`: canonical filename/path conventions (including metro-specific naming functions)

## Data-Domain Modules

- `coclab/ingest/`: HUD boundary ingesters
- `coclab/census/ingest/`: TIGER and tract-relationship ingestion
- `coclab/xwalks/`: tract and county crosswalk construction
- `coclab/measures/`: ACS aggregation + diagnostics
- `coclab/acs/`: ACS ingest/translation support (including `ingest/metro_acs1.py` for ACS 1-year CBSA data, `variables_acs1.py` for B23025 variable definitions)
- `coclab/pit/`: PIT ingest, registry, QA
- `coclab/pep/`: PEP ingest, aggregation, and diagnostics
- `coclab/rents/`: ZORI ingest, weighting, aggregation, diagnostics
- `coclab/panel/`: panel diagnostics and assembly internals
- `coclab/metro/`: metro analysis geography module
  - `coclab/metro/definitions.py`: Glynn/Fox metro definitions (25 metros, membership tables)
  - `coclab/metro/pit.py`: PIT aggregation from CoC to metro via CoC membership
  - `coclab/metro/acs.py`: ACS aggregation from tracts to metro via county membership
  - `coclab/metro/pep.py`: PEP aggregation from counties to metro via county membership
  - `coclab/metro/zori.py`: ZORI aggregation from counties to metro via county membership
  - `coclab/metro/validate.py`: metro artifact validation (ID formats, referential integrity, counts)
  - `coclab/metro/io.py`: read/write curated metro definition artifacts

## Utilities and Supporting Modules

- `coclab/geo/`: GeoParquet I/O (`read_geoparquet`, `write_geoparquet`), boundary validation, CRS normalization, geometry hashing, CT planning regions
- `coclab/viz/`: Folium-based interactive map rendering (`render_coc_map`)
- `coclab/nhgis/`: NHGIS extraction support for pre-2020 tract data
- `coclab/source_registry.py`: external source tracking with SHA-256 hashes (`register_source`, `check_source_changed`, `list_sources`)
- `coclab/year_spec.py`: year-spec parser for ranges/lists used throughout CLI (e.g., `2018-2024`, `2018,2020,2022`)
- `coclab/raw_snapshot.py`: raw data snapshot retention utilities
- `coclab/curated_policy.py`: curated layout policy enforcement and validation
- `coclab/curated_migrate.py`: curated data migration utilities
- `coclab/audit_panels.py`: metro audit panel utilities

## Guidance

- Prefer CLI or recipe interfaces for end-to-end workflows.
- Treat module internals as implementation details unless explicitly exported via package `__init__.py`.

---

**Previous:** [[13-Bundle-Layout]] | **Next:** [[15-Development]]
