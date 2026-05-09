# SAE Recipe Usage

Date: 2026-05-09
Bead: `coclab-uc9r.15`

Small-area estimation (SAE) recipes allocate ACS1 county aggregates to target
geographies through ACS5 tract distribution supports. Use SAE when direct
county mapping is too coarse for partial-county or multi-county CoCs and when
you need county ACS1 signal with tract-level spatial distribution.

## Example

See `recipes/examples/coc-sae-acs1-2023.yaml`.

Validate the recipe without executing:

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-preflight \
  --recipe recipes/examples/coc-sae-acs1-2023.yaml --json
```

Inspect the resolved task graph:

```bash
HHPLAB_NON_INTERACTIVE=1 hhplab build recipe-plan \
  --recipe recipes/examples/coc-sae-acs1-2023.yaml --json
```

The example expects these curated artifacts:

- `data/curated/acs/acs1_county_sae__A2023.parquet`
- `data/curated/acs/acs5_tract_sae_support__A2022xT2020.parquet`
- `data/curated/xwalks/xwalk__B2025xT2020.parquet`

## Measure Semantics

SAE v1 is component-based. It allocates ACS1 county counts and bins using
within-county ACS5 tract shares, rolls allocated components to the target
geography, and derives rates or distribution summaries after rollup.

Supported recipe measure families:

- `labor_force`
- `rent_burden`
- `owner_cost_burden`
- `household_income_bins`
- `gross_rent_bins`
- `tenure_income`

Do not request direct ACS median columns such as `median_household_income` or
`median_gross_rent` as SAE outputs. SAE medians and quintile cutoffs must be
derived from allocated distributions, for example
`sae_household_income_median`, `sae_household_income_quintile_cutoff_20`, and
`sae_gross_rent_median`.

## Direct County Comparison

Direct county comparison diagnostics are valid only when a target geography is
made entirely from whole counties. They are not valid for partial-county CoCs,
and mixed whole/partial county targets should be treated as non-comparable.

Use direct county diagnostics to check whether the SAE allocation preserves the
county aggregate in whole-county cases. Use SAE estimates for partial-county and
multi-county CoCs where direct county mapping would over- or under-include
county population.

## ACS1 Availability

ACS1 2020 is unavailable. Preflight reports that gap with no ingest command.
Use a different universe year, a different source such as BLS LAUS for labor
market measures, or an explicitly documented fallback policy.
