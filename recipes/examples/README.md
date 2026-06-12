# Example Recipes

These recipes are intended to be runnable, high-signal examples for future
users. Together they cover the current recipe surface:

- CoC, metro, and MSA targets
- PIT, ACS5 tract inputs, PEP county inputs, ZORI county inputs, and ACS1 metro inputs
- Identity and aggregate resampling
- CoC crosswalks and generated metro/MSA transforms
- Recipe-native map outputs with layered CoC / MSA / metro overlays
- Recipe-native containment outputs for MSA-to-CoC and CoC-to-county candidate lists
- MSA-CoC containment panels at `msa_id x coc_id x year` grain
- `file_set`-driven year/geometry switching
- Point-in-time and calendar-mean temporal filters
- ACS1/ACS5 small-area estimation (SAE) recipe planning and preflight

## Canonical Urban-Fraction Recipes

The repository root includes two canonical selector recipes:

- `recipes/coc-urban-fraction-gte-95-2020.yaml`
- `recipes/coc-urban-fraction-gte-99-2020.yaml`

Both recipes build one 2020 row per selected CoC and write to the configured
output root, which defaults to `../HHP-Data` in `hhplab.yaml`. They request
recipe-native primary MSA annotations through `panel_policy.primary_msa`, so
no post-run MSA join is needed.

Required curated inputs:

- 2020 CoC boundaries:
  `hhplab ingest boundaries --source hud_exchange --vintage 2020`
- 2023 county geometry:
  `hhplab ingest tiger --year 2023 --type counties`
- Census MSA 2023 definitions and county membership:
  `hhplab generate msa --definition-version census_msa_2023`
- 2020 CoC-to-MSA area crosswalk:
  `hhplab generate msa-xwalk --boundary 2020 --definition-version census_msa_2023 --counties 2023`
- 2020 decennial tract population denominators for population-basis primary MSA selection:
  `hhplab ingest decennial-tracts --decennial 2020 --tracts 2020`
- 2020 CoC urban-fraction measure:
  `hhplab build urban-fraction --boundary 2020 --decennial 2020 --urban-area-vintage 2020 --block-vintage 2020`

Run preflight before execution:

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-preflight \
  --recipe recipes/coc-urban-fraction-gte-95-2020.yaml --json

HHPLAB_NON_INTERACTIVE=1 hhplab build recipe \
  --recipe recipes/coc-urban-fraction-gte-95-2020.yaml --json
```

Primary MSA output fields:

| Column | Meaning |
|--------|---------|
| `primary_msa_id` | Selected Census MSA / CBSA code, or null when no MSA overlaps the CoC. |
| `primary_msa_name` | Display name from the requested MSA definition version. |
| `primary_msa_population` | Selected MSA population when population-basis overlap is used; null for area-basis selection. |
| `primary_msa_overlap_basis` | Basis used to select the MSA; `gte_95` uses 2020 decennial tract population and `gte_99` uses area. |
| `primary_msa_coc_contained_percent` | Percent of the CoC overlap basis contained by the selected MSA. |
| `primary_msa_covered_by_coc_percent` | Percent of the selected MSA overlap basis covered by the CoC. |

## Cohort-Style Examples

The current recipe DSL builds a target geography and time span, but it does not
yet have a first-class selector for ranked cohorts like "top 50 CoCs by 2021
population." Use the base recipes below to build the panel, then rank and slice
the output downstream.

- Requested idea: top 50 CoCs by population with ACS income and ZORI, 2015-2021
  Use `coc-base-pit-acs-zori-2016-2021.yaml`, then filter the built panel on
  `year == 2021` and keep the 50 largest `population` values.
- Requested idea: 25 smallest metros with at least 1M population and ACS income, 2019-2025
  Use `metro-glynnfox-acs-income-2019-2025.yaml`. That example still uses the
  historical Glynn/Fox cohort, which is now a subset profile over the canonical
  metro universe, so downstream ranking happens after the panel is built.

## Recipes

- `coc-base-pit-acs-zori-2016-2021.yaml`
  National CoC base panel for downstream top-N slicing. Includes PIT, PEP,
  lagged ACS5 demographics, and January ZORI.
- `coc-pit-density-2015-2024.yaml`
  National CoC PIT panel with CoC names plus density derived from lagged ACS5
  total population and curated CoC boundaries.
- `coc-sae-acs1-2023.yaml`
  One-year CoC SAE example that allocates ACS1 county components through ACS5
  tract distribution supports. It requests labor-force, rent-burden, and
  distribution-derived median/quintile outputs with diagnostics.
- `coc-pep-zori-calendar-2020-2024.yaml`
  County-driven CoC panel using PEP population plus calendar-mean ZORI.
- `coc-county-containment-los-angeles-2025.yaml`
  Containment-only recipe that writes counties whose 2023 geometry is at least
  50 percent contained by the selected 2025 Los Angeles CoC boundary.
- `metro-glynnfox-acs-income-2019-2025.yaml`
  ACS-only metro panel for the 25 Glynn/Fox metros. Treat this as a
  compatibility-profile example over the canonical metro universe. Good for
  long ACS-only spans that extend beyond PIT coverage.
- `metro-glynnfox-pit-acs-pep-zori-2016-2024.yaml`
  Full-feature metro panel that combines all major crosswalk-based inputs.
- `metro-glynnfox-pit-pep-2011-2014.yaml`
  Early-year metro panel focused on PIT + PEP before ZORI availability.
- `metro-glynnfox-pit-pep-acs1-2023.yaml`
  One-year metro panel showing ACS1 metro-native identity resampling alongside
  PIT and PEP.
- `msa-census-pit-acs-pep-2020-2021.yaml`
  Census MSA panel that uses CoC-native PIT allocated through the generated
  CoC-to-MSA crosswalk, plus county PEP and lagged ACS5 tract measures.
- `msa-coc-containment-denver-2025.yaml`
  Containment-only recipe that writes 2025 CoCs whose area overlaps the Denver
  Census MSA above the configured threshold.
- `top100-msa-coc-panel-2010-2019.yaml`
  MSA-CoC panel recipe for the top 100 Census MSAs, 2010-2019. It keeps CoCs
  with at least 99 percent containment, emits MSA-year ACS5 covariates plus
  CoC-year PIT/population measures, and documents how to switch population and
  unemployment sources.

## Geography Notes

- Use `metro` for the project's metro analysis surface.
- For new recipes, use `geometry: { type: metro, source: census_msa_2023 }`
  when you want the full canonical metro universe.
- Add `subset_profile: glynn_fox` and
  `subset_profile_definition_version: glynn_fox_v1` when you want the
  historical 25-metro Glynn/Fox subset over that universe.
- Existing examples that still say `source: glynn_fox_v1` are compatibility
  examples. Runtime execution resolves them through the canonical metro
  universe plus the Glynn/Fox subset profile.
- Use `msa` for official Census delineations keyed by `msa_id` / CBSA code.
- Map targets use the same geometry ids plus a `map_spec.layers[*].selector_ids`
  list. CoC layers need curated CoC boundaries, MSA layers need
  `hhplab ingest msa-boundaries`, and metro layers need
  `hhplab generate metro-boundaries`.
- MSA PIT values are derived from the stored CoC-to-MSA crosswalk rather than
  published natively by HUD. See [background/msa_geography.md](../../background/msa_geography.md)
  for the allocation rule and prerequisites.
- Containment recipes do not need datasets or transforms. They read curated
  geometry artifacts directly and should be checked with `hhplab build
  recipe-preflight --json` before running `hhplab build recipe --json`.
  MSA-to-CoC containment requires CoC boundaries, TIGER counties, and generated
  MSA definition/membership artifacts; CoC-to-county containment requires CoC
  boundaries and TIGER counties.
- MSA-CoC panel recipes use the containment artifacts plus panel source
  datasets. For 2010-2019 examples, ACS5 unemployment covers the full window;
  LAUS unemployment can be selected for 2015+ windows after ingesting
  `hhplab ingest laus-metro`.
- SAE recipes use `small_area_estimate` steps rather than generic
  `weighted_mean` aggregation. They require ACS1 county source components, ACS5
  tract support components, and a target tract crosswalk. Median-like SAE
  outputs must be distribution-derived; direct ACS median columns are context
  fields and must not be averaged.

## Suggested Commands

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-preflight \
  --recipe recipes/examples/coc-base-pit-acs-zori-2016-2021.yaml --json

HHPLAB_NON_INTERACTIVE=1 hhplab build recipe \
  --recipe recipes/examples/coc-base-pit-acs-zori-2016-2021.yaml --json

HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-preflight \
  --recipe recipes/examples/coc-sae-acs1-2023.yaml --json

HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-plan \
  --recipe recipes/examples/coc-sae-acs1-2023.yaml --json
```
