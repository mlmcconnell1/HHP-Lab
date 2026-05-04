# Metro ACS 1-Year + 5-Year Dual-Product Plan

> Note: the repository/runtime is now HHP-Lab/`hhplab`. This file is kept in
> its historical form, so older CoC-Lab references may remain intentionally.

Status: Proposed
Version: 0.1
Scope: Metro analysis geography only

## Goal

Add ACS 1-year support for metro panels while preserving the existing ACS 5-year tract-derived pipeline.

This plan explicitly does **not** attempt to add ACS 1-year support for CoCs. CoCs remain ACS 5-year only because the current CoC ACS workflow depends on tract-native ACS inputs, and ACS 1-year estimates are not published at tract geography.

## Why This Should Be a Dual-Product Design

Metro ACS 5-year and metro ACS 1-year are different products with different strengths:

- ACS 5-year:
  - available through the existing tract-derived pipeline;
  - supports continuity with current panel outputs;
  - better for small-area stability.
- ACS 1-year:
  - available natively for metropolitan statistical areas;
  - better for annual movement in fast-changing measures such as unemployment;
  - should not replace ACS 5-year silently under the same column names.

The system should therefore support both products side by side rather than treating ACS 1-year as a drop-in override.

## Product Model

Introduce two metro ACS product families:

1. `acs5_tract_derived`
   - current path;
   - source geography: tract;
   - target geography: metro;
   - aggregation path: tract -> metro.

2. `acs1_metro_native`
   - new path;
   - source geography: metro/CBSA;
   - target geography: metro;
   - aggregation path: none when the metro definition matches a Census metro exactly.

## Non-Goals

- Adding ACS 1-year support for CoC panels.
- Inferring CoC annual values from metro annual values.
- Hiding product differences behind shared unsuffixed column names.
- Solving all possible ACS measure additions in the first implementation.

## First Implementation Slice

Implement the smallest useful version first:

1. Add metro-native ACS 1-year unemployment only.
2. Preserve all current ACS 5-year metro outputs unchanged.
3. Add one new metro panel column:
   - `unemployment_rate_acs1`
4. Limit ACS 1-year support to metros that can be mapped exactly to a Census metro.
5. Emit nulls for unsupported metros rather than approximating.

This first slice delivers the primary analytical benefit, annual labor-market movement, with limited schema and pipeline risk.

## Canonical Schema Direction

Do not overload the existing ACS 5-year columns.

Recommended column naming:

- existing ACS 5-year columns remain unchanged for now:
  - `total_population`
  - `adult_population`
  - `population_below_poverty`
  - `median_household_income`
  - `median_gross_rent`
- new ACS 1-year columns are explicitly product-scoped:
  - `unemployment_rate_acs1`

If future ACS 1-year measures are added, use the same pattern:

- `median_household_income_acs1`
- `median_gross_rent_acs1`
- `total_population_acs1`

Longer term, if the project wants every ACS field to be product-scoped, a broader migration can rename the existing 5-year columns to `_acs5` variants. That migration is **not** required for the first implementation.

## Required Architecture Changes

## 1. Add a Metro-Native ACS 1-Year Ingest/Aggregation Path

Create a dedicated metro ACS 1-year module rather than trying to force annual data through the tract pipeline.

Recommended new module:

- `hhplab/metro/metro_acs1.py`

Responsibilities:

- fetch ACS 1-year data from the Census API at metro geography;
- map Census metro identifiers to project `metro_id`;
- derive measure columns such as `unemployment_rate_acs1`;
- write curated metro ACS 1-year artifacts with provenance.

Rationale:

- the current ACS ingest code in `hhplab/acs/ingest/tract_population.py` is explicitly tract-native;
- reusing it for ACS 1-year would hard-code false assumptions about tract availability.

## 2. Add a Metro-to-Census-Metro Mapping Layer

ACS 1-year metro estimates are native to Census metropolitan statistical areas, while this project uses Glynn/Fox metro definitions.

Add a curated or code-generated mapping layer that records whether a project metro has an exact Census metro counterpart.

Recommended artifact:

- `data/curated/metro/metro_cbsa_mapping__<definition_version>.parquet`

Recommended columns:

- `metro_id`
- `definition_version`
- `cbsa_code`
- `cbsa_title`
- `match_status`
  - `exact`
  - `unsupported`
- `match_notes`

Rules:

- only `exact` rows are eligible for ACS 1-year ingestion;
- unsupported rows remain null in ACS 1-year outputs;
- no approximate or partial mappings in phase 1.

## 3. Add First-Class Metro ACS Artifacts

The current operational ACS aggregation path is CoC-centric. Metro ACS 1-year needs a normal artifact path that panel assembly can discover reliably.

Recommended naming direction:

- `measures__metro__acs1__A2024@Dglynnfoxv1.parquet`
- `measures__metro__acs1__A2024-2024@Dglynnfoxv1.parquet`

If naming should stay closer to existing helpers, add dedicated helpers in `hhplab/naming.py`:

- `metro_measures_acs1_filename(...)`
- `metro_measures_acs5_filename(...)`

The important requirement is that the filename encode:

- target geography family: metro
- ACS product: `acs1` vs `acs5`
- ACS vintage
- definition version

## 4. Make Panel Assembly Product-Aware

Metro panel assembly in `hhplab/panel/assemble.py` currently assumes a single ACS input family.

Add a second optional metro ACS load step:

- keep existing metro ACS 5-year load behavior;
- load metro ACS 1-year artifacts separately when available;
- merge ACS 1-year columns into the metro panel by `metro_id` and `year`.

Recommended metro panel additions:

- `unemployment_rate_acs1`
- optionally `acs1_vintage_used`

The panel provenance block should also record which ACS products were used for the build.

Recommended provenance additions:

- `acs_products_used`: `["acs5"]` or `["acs5", "acs1"]`
- `acs1_vintage_used` when ACS 1-year is present

## 5. Generalize Conformance to Product-Specific Measure Sets

`hhplab/panel/conformance.py` currently uses a single `ACS_MEASURE_COLUMNS` list.

Replace or extend this with product-aware constants, for example:

- `ACS5_COC_MEASURE_COLUMNS`
- `ACS5_METRO_MEASURE_COLUMNS`
- `ACS1_METRO_MEASURE_COLUMNS`

For the first slice:

- CoC panel request defaults remain ACS 5-year only;
- metro panel request can validate:
  - core ACS 5-year metro columns;
  - optional ACS 1-year columns when requested.

Recommended rule:

- absence of ACS 1-year columns should not fail a metro panel unless the build explicitly requested them.

## 6. Update Recipe and Export Logic

Recipe execution currently infers measure columns from a small known set in `hhplab/recipe/executor.py`.

Update recipe-aware detection so metro ACS 1-year measures are included when present:

- `unemployment_rate_acs1`

Also update export codebook metadata in `hhplab/export/codebook.py` so:

- `unemployment_rate_acs1` is documented explicitly;
- unsuffixed `unemployment_rate` is avoided unless the project chooses a deliberate aliasing strategy.

## CLI Changes

## 1. New Aggregate Command Path

Add a dedicated aggregate command for metro ACS 1-year.

Recommended options:

```bash
coclab aggregate metro-acs1 --build <build> [--years ...] [--measures unemployment]
```

Alternative:

```bash
coclab aggregate acs --build <build> --geo-type metro --product acs1
```

Recommendation:

- prefer a dedicated command only if the current CLI structure would otherwise become awkward;
- otherwise extend `aggregate acs` with explicit `--geo-type` and `--product` flags.

Requirements:

- machine-readable `--json` output support;
- deterministic output path discovery;
- actionable errors when a metro has no exact CBSA mapping.

## 2. Build Panel CLI

Extend `coclab build panel` for metro builds with explicit ACS 1-year control.

Recommended flags:

- `--include-acs1/--no-include-acs1`
- `--acs1-measures unemployment`

Behavior:

- default metro build can continue to use ACS 5-year only unless ACS 1-year is requested;
- when requested, the command loads ACS 1-year metro artifacts and merges those columns;
- CoC builds ignore ACS 1-year flags with a clear error or warning.

## Data Model and Provenance

Every ACS 1-year metro artifact should include provenance fields sufficient to answer:

- what ACS product was used?
- what vintage was used?
- what Census geography was queried?
- how was `metro_id` mapped?

Recommended provenance fields:

- `geo_type = "metro"`
- `definition_version`
- `acs_product = "acs1"`
- `acs_vintage`
- `dataset_type = "metro_measures_acs1"`
- `cbsa_mapping_version`
- `measure_set`

## Measure Methodology

For unemployment:

- fetch Census ACS 1-year employment-status inputs from table `B23025`;
- use:
  - civilian labor force
  - unemployed
- derive:
  - `unemployment_rate_acs1 = unemployed / civilian_labor_force`

Store numerator and denominator in intermediate metro ACS 1-year artifacts when feasible:

- `civilian_labor_force_acs1`
- `unemployed_acs1`

This makes the rate auditable and supports future QA checks.

## Testing Plan

Add tests in five layers.

1. Mapping tests
   - verify `metro_id` to CBSA mapping coverage and exact-match rules.

2. ACS 1-year ingest tests
   - verify Census responses are normalized correctly;
   - verify unemployment numerator/denominator derivation.

3. Metro artifact tests
   - verify output schema, filenames, and provenance.

4. Panel assembly tests
   - verify metro panels can include both ACS 5-year and ACS 1-year columns;
   - verify ACS 1-year columns join by `metro_id` and `year`.

5. Conformance/export tests
   - verify requested ACS 1-year columns are checked correctly;
   - verify codebook and export metadata reflect the new schema.

## Rollout Plan

## Phase 1. Metro ACS 1-Year Unemployment

Deliverables:

- exact metro-to-CBSA mapping
- metro ACS 1-year ingest/aggregate path
- metro artifact writing and discovery
- metro panel support for `unemployment_rate_acs1`
- tests and export metadata

Acceptance:

- a metro panel can be built with existing ACS 5-year columns plus annual unemployment.

## Phase 2. Additional Metro ACS 1-Year Measures

Potential measures:

- labor-force counts
- total population
- median household income
- poverty-related annual metrics

Decision rule:

- only add measures with clear native ACS 1-year definitions and acceptable coverage across supported metros.

## Phase 3. Optional Product Generalization

If the project wants a fully symmetric dual-product ACS model, consider:

- renaming existing metro ACS 5-year columns to `_acs5` suffixes;
- adding explicit panel schema families by geography and ACS product;
- making recipe dataset specs declare `product: acs1` or `product: acs5`.

This phase should only happen if the added clarity is worth the migration cost.

## Risks and Tradeoffs

1. Exact CBSA mapping may not cover every project metro.
   - Mitigation: support only exact matches in phase 1.

2. Mixed ACS products can confuse users.
   - Mitigation: explicit column names and provenance.

3. Hard-coded ACS schema constants already exist in multiple modules.
   - Mitigation: centralize product-specific measure constants before adding many more ACS 1-year fields.

4. CLI sprawl is possible.
   - Mitigation: prefer extending existing aggregate/build commands unless the interface becomes unclear.

## Summary Recommendation

Implement metro ACS 1-year as a parallel, metro-native product beginning with unemployment only.

Keep CoCs on ACS 5-year.

Use explicit product-scoped schema and provenance so annual metro measures can coexist cleanly with the existing tract-derived ACS 5-year panel columns.
