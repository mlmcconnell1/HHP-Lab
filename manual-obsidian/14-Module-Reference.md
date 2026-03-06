# Module Reference

This chapter is an orientation map of active modules in the current codebase.

## CLI Layer

- `coclab/cli/main.py`: top-level Typer app and command registration
- `coclab/cli/builds.py`: build scaffold/list/catalog commands
- `coclab/cli/build_xwalks.py`: crosswalk generation into build scope
- `coclab/cli/aggregate.py`: dataset aggregation commands (`acs`, `zori`, `pep`, `pit`)
- `coclab/cli/build_panel.py`: imperative panel assembly command
- `coclab/cli/status.py`: one-shot environment readiness report (`coclab status`)
- `coclab/cli/list_artifacts.py`: build/global artifact inventory for automation
- `coclab/cli/list_*.py`: dataset discovery commands with optional JSON output
- `coclab/cli/diagnostics.py` and `coclab/cli/panel_diagnostics.py`: diagnostics commands with optional JSON output
- `coclab/cli/recipe.py`: recipe execution/provenance/export commands
- `coclab/cli/export_bundle.py`: analysis bundle export command

## Recipe System

- `coclab/recipe/recipe_schema.py`: versioned recipe schema models
- `coclab/recipe/loader.py`: YAML loading + schema dispatch
- `coclab/recipe/adapters.py`: semantic validation registries
- `coclab/recipe/default_*.py`: built-in adapter registration
- `coclab/recipe/planner.py`: deterministic task planning
- `coclab/recipe/executor.py`: runtime execution engine
- `coclab/recipe/manifest.py`: consumed-asset manifests and recipe bundle export

## Build and Provenance

- `coclab/builds.py`: build directory and manifest helpers
- `coclab/provenance.py`: Parquet metadata embedding/reading
- `coclab/naming.py`: canonical filename/path conventions

## Data-Domain Modules

- `coclab/ingest/`: HUD boundary ingesters
- `coclab/census/ingest/`: TIGER and tract-relationship ingestion
- `coclab/xwalks/`: tract and county crosswalk construction
- `coclab/measures/`: ACS aggregation + diagnostics
- `coclab/acs/`: ACS ingest/translation support
- `coclab/pit/`: PIT ingest, registry, QA
- `coclab/pep/`: PEP ingest and aggregation
- `coclab/rents/`: ZORI ingest, weighting, aggregation, diagnostics
- `coclab/panel/`: imperative panel builder + diagnostics
- `coclab/export/`: bundle selection/copy/manifest/readme generation

## Guidance

- Prefer CLI or recipe interfaces for end-to-end workflows.
- Treat module internals as implementation details unless explicitly exported via package `__init__.py`.

---

**Previous:** [[13-Bundle-Layout]] | **Next:** [[15-Development]]
