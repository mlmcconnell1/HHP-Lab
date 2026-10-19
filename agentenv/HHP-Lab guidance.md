## HHP-Lab Agent-Friendly CLI

Prefer these HHP-Lab runtime features when automating:

- Use machine-readable output whenever available (`--json`) to avoid parsing human text.
- Run non-interactively for CI/agents: pass `--non-interactive` or set `HHPLAB_NON_INTERACTIVE=1`.
- Use `hhplab agents` for built-in geography/year matching rules and operational guidance.
- Preflight environment and prerequisites with:
  - `hhplab status --json`
- Validate curated naming/layout policy before and after writes:
  - `hhplab validate curated-layout`
- For curated filename migrations, default to dry-run first:
  - `hhplab migrate curated-layout`
  - `hhplab migrate curated-layout --apply`
- For recipe workflows, validate/plan before execute:
  - `hhplab build recipe-preflight --recipe <file> --json`
  - `hhplab build recipe --recipe <file> --json`
  - Use `hhplab build recipe-plan --recipe <file> --json` when you need the resolved task graph while authoring/debugging a recipe

## Measure Discovery

Before calling external APIs or writing one-off scripts, determine whether the package already supports the measure:

1. Check the package registries first:
   - ACS5 tract-derived measures: `hhplab/sources/census/acs/variables.py` (`ACS5_COVARIATE_REGISTRY`), or `hhplab list acs-variables`.
   - ACS1 metro/county-native measures: `hhplab/sources/census/acs/variables_acs1.py` (`DERIVED_ACS1_MEASURES` and `ACS1_*_MEASURE_COLUMNS`), or `hhplab list acs-variables`.
   - External covariate sources: `hhplab list covariates`, backed by `hhplab/covariates/catalog.py`.
2. Treat curated files on disk as possibly stale. Absence of a column in an existing parquet does **not** mean the package lacks support; re-ingest with `--force` to refresh the schema before concluding support is absent.
3. If a measure exists in a registry, use the package ingest, aggregate, and analyze pipeline. Only fall back to external fetching when the measure is genuinely absent, and then file a bead to add registry support rather than leaving a one-off script.

## Code Style: Human and Agent Readable

All code generated for this project must be easily usable by both humans and AI agents. Apply these principles when writing code, tests, CLI commands, and data pipelines:

### Tests and fixtures
- **Declarative over implicit.** Define fixture data and expected outcomes as named constants, not as magic numbers buried in logic. An agent (or human) modifying a fixture value should not need to hand-recompute downstream expectations.
- **Parametrize over loops.** Use `@pytest.mark.parametrize` instead of for-loops inside test bodies. Failures should name the exact case in the test ID (e.g., `test_dtype[year-int64]`), not require reading assertion messages.
- **Truth tables over prose.** When a fixture has designed-in outcomes (e.g., which rows pass a threshold), document the full truth table in a visible location (module docstring or a constant), not scattered across individual test docstrings.
- **Allowlists and exceptions at module level.** If something is a known special case (e.g., columns with expected dtype normalization), declare it as a named module-level constant with a comment explaining why — not as a local variable inside a function.
- **Derive, don't duplicate.** Golden-value tests should compute expectations from the same constants that build the fixtures. Changing a fixture value should automatically update the expected outcome.

### CLI and output
- **Always provide `--json`.** Every CLI command that produces output should support a `--json` flag emitting structured, machine-parseable JSON. Agents should never need to scrape human-formatted tables or prose to extract results.
- **Actionable error messages.** Errors should state what went wrong AND what to do about it (e.g., "No ACS measures found — run `hhplab aggregate acs` first"). An agent that encounters an error should be able to act on it without searching the codebase.
- **Deterministic, parseable file names.** Output artifacts should use the canonical naming from `hhplab/naming.py` so agents can discover and reference them programmatically without globbing.

### Schemas and data contracts
- **Canonical column lists as code.** Output schemas (e.g., `PANEL_COLUMNS`, `ZORI_COLUMNS`) must be defined as module-level constants. When a schema changes, update the constant — never add columns silently.
- **Provenance in every artifact.** Parquet outputs must embed provenance metadata via `write_parquet_with_provenance` so downstream agents can inspect lineage without external tracking.

## Dataset Availability & Geometry Rules

When authoring recipes, selecting year ranges, or debugging missing data, use these constraints. A year outside a source's coverage window means the data **does not exist** — it is not a build failure.

### Temporal coverage

Use `hhplab list sources --json` for machine-readable temporal coverage of core panel sources (PIT, HIC, ACS5, ACS1, PEP, ZORI). Use `hhplab list covariates --json` for external covariate source coverage such as IRS SOI migration.

- **ACS lag rule:** ACS vintage for PIT year Y is Y−1 (released ~Dec of year Y−1).
- **PEP coverage:** Postcensal vintage 2020 covers 2010-2020; vintage 2024 covers 2020-2024. Combined/intercensal fills the full 2010-2020 range.

### Census tract geometry eras

Tract-based data (ACS, crosswalks) must reference the correct decennial tract vintage. Tracts are redefined each decennial census:

| Data years | Tract vintage | Example |
|-----------|---------------|---------|
| 2000–2009 | 2000 | ACS 2009 uses 2000-era tracts |
| 2010–2019 | 2010 | ACS 2018 uses 2010-era tracts |
| 2020–2029 | 2020 | ACS 2023 uses 2020-era tracts |

**Rule:** use the most recent decennial ≤ the data year. In recipes, this drives the `segments` section of `file_set` specs — each segment maps a year range to its tract vintage.

Cross-era analysis (e.g., a 2015-2024 panel) requires a tract relationship file (2010↔2020) and separate crosswalk builds per era.

### Measure columns by data source

Different data sources produce different demographic columns. Conformance checks use `PanelRequest.measure_columns` to validate the right set:

| Source | Measure columns in panel | Notes |
|--------|-------------------------|-------|
| ACS | `total_population`, `adult_population`, `population_below_poverty`, `median_household_income`, `median_gross_rent` | Tract-level, apportioned via crosswalk |
| PEP | `population` | County-level, aggregated to target geography |

When building non-ACS panels (e.g., PEP-based metro), set `measure_columns` on `PanelRequest` so conformance checks validate the correct columns instead of defaulting to the ACS set.

## Adding Beads (Problem Noticed)

If you identify a problem in the code, even incidentally while working on something else, add a bead to make sure it is addressed later.