# Example Recipes

These recipes are intended to be runnable, high-signal examples for future
users. Together they cover the current recipe surface:

- CoC, metro, and MSA targets
- PIT, ACS5 tract inputs, PEP county inputs, ZORI county inputs, and ACS1 metro inputs
- Identity and aggregate resampling
- CoC crosswalks and generated metro/MSA transforms
- Recipe-native map outputs with layered CoC / MSA / metro overlays
- `file_set`-driven year/geometry switching
- Point-in-time and calendar-mean temporal filters

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
- `coc-pep-zori-calendar-2020-2024.yaml`
  County-driven CoC panel using PEP population plus calendar-mean ZORI.
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

## Suggested Commands

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-preflight \
  --recipe recipes/examples/coc-base-pit-acs-zori-2016-2021.yaml --json

HHPLAB_NON_INTERACTIVE=1 hhplab build recipe \
  --recipe recipes/examples/coc-base-pit-acs-zori-2016-2021.yaml --json
```
