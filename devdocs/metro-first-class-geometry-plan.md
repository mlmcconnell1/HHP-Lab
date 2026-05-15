# Metro Areas as a First-Class Analysis Geography

> Note: the repository/runtime is now HHP-Lab/`hhplab`. This file is kept in
> its historical form, so older CoC-Lab references may remain intentionally.

Status: Implemented
Version: 1.0
Primary target: Glynn/Fox metro definitions
Future target considered in design: county analysis geography

## Goal

Add metro areas as a first-class analysis geography without duplicating the current CoC-first architecture. The immediate implementation target is the metro definition used in `background/Dynamics-of-Homelessness-GlynnFox.pdf`, but the architectural changes should also preserve a clean path to future county-level analysis.

This is not a request to make all geometry handling fully generic in one phase. It is a request to introduce the minimum durable abstraction that:

1. Keeps CoC as a supported first-class geography.
2. Adds metro as a supported first-class geography.
3. Does not block a later county geography.

## Alignment With Current Manual

This plan is intended to extend the architecture described in `HHP-Lab-manual/`, not replace it wholesale.

The current manual has two simultaneously true statements:

1. the implemented system is explicitly CoC-centered;
2. the recipe/schema layer already treats geometry types as an open set and uses `geo_id`/`year` as the conceptual join contract.

This plan follows that split:

- it preserves the current CoC-first operational path as the default user experience;
- it extends the operational pipeline toward the geometry-open direction already described in the recipe and panel methodology chapters;
- it does not assume the entire manual should become geometry-neutral in one phase.

## Why This Needs More Than a New Rollup

The Glynn/Fox metros are not just a second boundary file. They are synthetic analysis units built from mixed rules:

- some metros map to one CoC;
- some require aggregating multiple CoCs within a county;
- some require combining county population/ZRI inputs across a multi-county CoC;
- the paper's unit is an analysis construct, not a single federal boundary source.

That means "metro support" should be treated as a new analysis geography family with explicit identity and mapping data, not as a thin alias for CoC polygons.

## Scope

## In scope for this phase

- Add a first-class analysis geography abstraction covering `coc` and `metro`.
- Add curated metro definition data for the Glynn/Fox metros.
- Update aggregation and panel plumbing so a build can target either CoC or metro.
- Generalize derived artifacts to use canonical geography fields instead of implicit `coc_id`.
- Preserve existing CoC workflows and outputs.

## Out of scope for this phase

- County as a user-visible analysis target.
- A complete plugin system for arbitrary geometries.
- Rebuilding every CLI around a totally geometry-generic recipe compiler.
- Full migration of every historical artifact to new names in one step.

## Design Principles

1. Separate `analysis geography` from `source geometry`.
2. Use canonical identifiers in derived tables:
   - `geo_type`
   - `geo_id`
3. Keep geometry-specific columns only where required for backward compatibility.
4. Treat metro definitions as curated input data with provenance, not hard-coded logic.
5. Make build manifests and artifact naming geography-aware now so county can slot in later.

## Manual-consistent terminology

To stay aligned with the current manual:

- use "CoC-centered" to describe the current implemented system;
- use "analysis geography" to describe the new abstraction introduced in this plan;
- keep recipe-level `GeometryRef` and `join_on: [geo_id, year]` semantics as compatible foundations rather than inventing a parallel vocabulary.

## Proposed Architecture

## 1. Introduce an analysis geography model

Create a small internal model used by build, aggregate, and panel layers:

- `geo_type`: `coc` or `metro`
- `geo_id`: canonical identifier within the geography family
- `boundary_vintage`: only for polygonal geometry families that have vintages
- `definition_version`: optional version for synthetic geographies such as metros

Recommended helper concepts:

- `AnalysisGeometryRef`
- `AnalysisGeometryCatalog`
- `AnalysisGeometryDefinition`

This does not need to replace the recipe schema's existing `GeometryRef`. It can sit in the operational pipeline first.

## 2. Add a curated metro definition package

Create curated inputs that define the Glynn/Fox metros explicitly.

Recommended artifacts:

- `data/curated/metro/metro_definitions__glynn_fox_v1.parquet`
- `data/curated/metro/metro_coc_membership__glynn_fox_v1.parquet`
- `data/curated/metro/metro_county_membership__glynn_fox_v1.parquet`

Suggested columns:

### `metro_definitions`

- `metro_id`
- `metro_name`
- `definition_version`
- `source`
- `source_ref`
- `notes`

### `metro_coc_membership`

- `metro_id`
- `coc_id`
- `membership_type`
- `weighting_note`
- `definition_version`

### `metro_county_membership`

- `metro_id`
- `county_fips`
- `membership_type`
- `weighting_note`
- `definition_version`

These tables should encode the paper's logic directly. They should not be reverse-engineered at runtime from prose.

## 3. Standardize derived datasets on `geo_id`

Derived non-native datasets should move toward:

- `geo_type`
- `geo_id`

Examples:

- ACS measures
- aggregated PEP
- aggregated ZORI
- aggregated PIT
- panels
- diagnostics and conformance summaries

Backward-compatibility rule:

- keep `coc_id` in CoC outputs during transition;
- metro outputs should not invent a fake `coc_id`;
- shared logic should prefer `geo_id`.

## 4. Make builds geography-scoped

Current builds assume a single privileged `coc_boundary` asset class. Update the manifest shape so base assets can be geometry-scoped.

Recommended manifest direction:

- `asset_type`: `coc_boundary`, `metro_definition`, `county_boundary`, etc.
- `geo_type`: optional but preferred on new entries
- `year` or `definition_version` depending on asset family

This allows a metro-targeted build to pin:

- CoC boundaries if needed for upstream translation;
- metro definition tables;
- county geometries if a later county target is added.

## 5. Split source-native aggregation from analysis-geometry resampling

There are two different operations in the codebase today:

1. read a native dataset at its own geography;
2. resample or aggregate that dataset to the analysis geography.

That distinction should become explicit.

Examples:

- PIT is native at CoC, then resampled to metro by CoC membership.
- PEP is native at county, then aggregated to CoC or metro by county membership/crosswalk.
- ZORI is native at county, then aggregated to CoC or metro by county weighting.
- ACS is native at tract, then aggregated to CoC or metro through tract crosswalks.

The implementation should avoid burying this distinction inside `coc_*` helpers.

## Immediate Implementation Plan

## Phase 0. Define canonical IDs and definitions

Deliverables:

- Metro definition spec in code comments or module docs.
- Curated metro definition artifacts.
- Canonical `metro_id` format.
- Rules for whether metros are versioned by `definition_version`, `boundary_vintage`, or both.

Decision:

- For this phase, use a fixed `definition_version` for Glynn/Fox metros.
- Do not require metro polygons up front.

Acceptance:

- A developer can inspect one curated table and determine how a metro is constructed.

## Phase 1. Add a geometry-awareness layer to core operational code

Target modules:

- build manifest helpers
- naming helpers
- artifact listing/discovery
- provenance helpers where needed

Deliverables:

- a central helper for deriving canonical ID column names;
- shared constants for `geo_type` and `geo_id`;
- geography-aware manifest/base-asset handling;
- naming rules for metro-derived outputs.

Recommended naming direction:

- keep existing CoC names unchanged for compatibility;
- add geography-explicit forms for new metro outputs;
- avoid a second legacy trap where metro outputs are named like CoC outputs.

Examples:

- `measures__metro__A2023@Dglynnfoxv1xT2020.parquet`
- `panel__metro__Y2011-2016@Dglynnfoxv1.parquet`

The exact token syntax can be finalized in implementation, but the filename must encode the target geography family.

Important manual alignment note:

- the current temporal terminology document defines `B/T/C/A/P/Z/Y`, not a synthetic-geography token such as `D`;
- if a `D{definition}` or equivalent notation is adopted for metro outputs, that must be added as an explicit extension to the terminology docs rather than introduced implicitly in code only.

Acceptance:

- New metro outputs can coexist with CoC outputs in the same curated folders without ambiguity.

## Phase 2. Generalize resampling primitives

Target modules:

- tract crosswalk generation
- county crosswalk generation
- ACS aggregation
- PEP aggregation
- ZORI aggregation

Deliverables:

- replace CoC-specific helper names or wrap them in geometry-neutral interfaces;
- allow target key columns other than `coc_id`;
- preserve current CoC behavior through compatibility wrappers.

Recommended pattern:

- low-level geometry-neutral functions:
  - `build_tract_crosswalk(target_gdf, target_id_col, ...)`
  - `build_county_crosswalk(target_gdf, target_id_col, ...)`
  - `aggregate_acs_to_geo(..., geo_id_col=...)`
  - `aggregate_pep_to_geo(...)`
  - `aggregate_zori_to_geo(...)`
- compatibility wrappers:
  - `build_coc_tract_crosswalk(...)`
  - `aggregate_pep_to_coc(...)`

Acceptance:

- CoC codepaths still pass unchanged tests.
- The same aggregation engine can target metro with explicit membership/crosswalk inputs.

## Phase 3. Add PIT-to-metro aggregation

This is the most metro-specific part, because PIT is native to CoC while the metro definitions are synthetic.

Deliverables:

- explicit metro PIT aggregation logic using curated metro-CoC membership;
- documented handling for:
  - one CoC to one metro;
  - multiple CoCs to one metro;
  - no fake boundary interpolation when a simple additive rollup is the right model.

Expected behavior:

- `pit_total`, `pit_sheltered`, and `pit_unsheltered` aggregate by sum over member CoCs.
- provenance records the metro definition version used.

Acceptance:

- a metro PIT table can be produced for all years supported by the underlying CoC PIT inputs.

## Phase 4. Add tract/county pathways for metro ACS, PEP, and ZORI

Deliverables:

- metro ACS measures from tract-native ACS through a metro tract/county relation;
- metro PEP from county-native source data;
- metro ZORI from county-native source data.

Important:

- do not force metro to depend on synthetic polygons if county membership is already the authoritative definition for the paper's logic.
- if a metro polygon is later added, it should support visualization and spatial QA, not redefine the analysis semantics silently.

Acceptance:

- metro measures and rent/population series can be built with the same CLI family used for CoC.

## Phase 5. Generalize panel assembly

Target modules:

- panel assembly
- panel diagnostics
- conformance
- export/codebook/selection

Deliverables:

- canonical panel schema based on `geo_id`;
- geography-specific metadata in provenance and export manifests;
- conformance requests that no longer assume "expected CoC count" only.

Recommended changes:

- replace `expected_coc_count` with `expected_geo_count`;
- rename or wrap CoC-specific checks so they are geography-neutral internally;
- preserve legacy CLI wording where needed for CoC-only workflows.

Acceptance:

- one panel builder can emit a CoC panel or a metro panel without fake columns.

## Phase 6. CLI surface

Target commands:

- `coclab aggregate acs`
- `coclab aggregate pep`
- `coclab aggregate zori`
- `coclab aggregate pit`
- `coclab build recipe`
- `coclab list curated`
- possibly `coclab generate xwalks`

Recommended CLI direction:

- add `--geo-type coc|metro`
- add `--definition-version` for metro where required
- keep CoC defaults so existing workflows do not break

Example:

```bash
coclab aggregate pit --build metro-gf --geo-type metro --definition-version glynn_fox_v1
coclab aggregate pep --build metro-gf --geo-type metro --definition-version glynn_fox_v1
coclab build panel --build metro-gf --geo-type metro --definition-version glynn_fox_v1 --start 2011 --end 2016
```

Acceptance:

- metro workflows are explicit and machine-readable;
- CoC workflows remain simple and backward compatible.

## County Future-Proofing

County is out of scope as a shipped analysis geography in this phase, but it must be considered in the architecture.

## Requirements for future county support

1. `geo_type=county` must fit into the same `geo_id` contract.
2. Build manifests must be able to pin county geometry assets without special cases.
3. Aggregation code must not assume that the target geography always differs from the source geography.
4. Panel code must not assume boundary vintages exist for every geography family.
5. Naming must handle county outputs without forcing fake `B{year}` boundary tokens where counties are the native geography.

## Specific design choices to keep county viable later

- Do not encode "target geography implies polygon boundary vintage" as a universal rule.
- Do not make `coc_id` the fallback canonical key.
- Do not hard-code `coc_boundary` as the only build base asset type.
- Do not make PIT-specific alignment assumptions global to all geographies.
- Keep source-native and analysis-target concepts separate, because county may be both.

## Documentation follow-up implied by this plan

If metro support is implemented, the manual should be updated in a targeted way:

- `01-Overview.md`: explain that CoC remains the default implemented hub, with metro now also supported as an analysis geography.
- `07-Data-Model.md`: add metro definition and metro-derived dataset schemas, while preserving existing CoC schemas.
- `08-Temporal-Terminology.md`: document any new notation used for synthetic geography definitions.
- `12-Methodology-Panel-Assembly.md`: explain how the imperative and recipe paths behave when the target geometry is metro rather than CoC.

Those documentation updates are downstream of implementation. They are not required before coding begins, but the architecture should be built so the docs can describe it cleanly.

## Data Model Recommendations

## Canonical column policy

For new derived outputs:

- required:
  - `geo_type`
  - `geo_id`
  - `year`
- optional, geometry-family specific:
  - `boundary_vintage`
  - `definition_version`
  - `source_geo_type`

For compatibility:

- CoC outputs may continue to carry `coc_id`.
- county outputs in a future phase may carry `county_fips`.
- these family-specific columns should be aliases, not the canonical join contract.

## Provenance policy

Provenance blocks for new geography-aware outputs should include:

- `geo_type`
- `geo_id_schema` or identifier note if useful
- `definition_version` for synthetic families
- `boundary_vintage` where applicable
- `source_geometry_types`

## Testing Plan

## Unit tests

- canonical `geo_id` helper behavior
- metro definition loading and validation
- PIT CoC-to-metro rollup truth tables
- metro PEP/ZORI aggregation with named expected constants
- panel joins using `geo_id`

## Integration tests

- CoC regression path remains green
- metro build using a minimal fixture definition
- artifact listing and export selection distinguish CoC vs metro outputs

## Fixture guidance

- define metros with small named truth-table fixtures
- avoid magic numbers in expected outputs
- derive expected aggregates from fixture constants

## Main Risks

1. Half-generic refactor risk: renaming a few columns without introducing a real geometry model will create a more confusing CoC-first system.
2. Metro definition ambiguity: if the Glynn/Fox mapping is not encoded explicitly, future outputs will be irreproducible.
3. Filename ambiguity: metro outputs must not collide with CoC outputs.
4. Panel churn: panel diagnostics and export code assume CoC semantics in many places.
5. Over-generalization risk: trying to solve arbitrary geometry support in this phase could slow delivery substantially.

## Recommended Delivery Slice

If implementation should be staged for fastest value:

1. Add metro definition artifacts and validation.
2. Generalize core keys to `geo_id` in derived outputs.
3. Implement PIT-to-metro.
4. Implement county-native PEP and ZORI to metro.
5. Implement ACS to metro.
6. Generalize panel assembly and export.

This produces usable metro panels early while leaving the hardest refactors for the layers that actually need them.

## Acceptance Criteria

1. CoC remains a fully supported first-class analysis geography.
2. Metro becomes a fully supported first-class analysis geography for the Glynn/Fox definition set.
3. Derived outputs use a canonical geography contract that does not depend on `coc_id`.
4. Build manifests and artifact names can represent more than one target geography family.
5. The resulting architecture keeps a clean path open for a future county analysis geography without a second structural refactor.
