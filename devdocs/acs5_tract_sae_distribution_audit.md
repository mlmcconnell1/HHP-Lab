# ACS5 Tract SAE Distribution Support Audit

Date: 2026-05-09
Bead: `coclab-uc9r.3`

This audit covers Census ACS 5-year tract ingest support for first-version
small-area estimation (SAE). ACS5 tract distributions provide spatial weights
for allocating ACS1 county aggregates to tract components before downstream
rollup.

## Coverage Inventory

| SAE support family | ACS5 table | Canonical columns | Status |
| --- | --- | --- | --- |
| Core population weights | `B01003`, `B01001` | `total_population`, `adult_population`, `moe_total_population` | Covered |
| Household tenure denominators | `B25003` | `total_households`, `owner_households`, `renter_households` | Covered |
| Poverty supports | `C17002` | `poverty_universe`, poverty bins, `population_below_poverty` | Covered |
| Labor-force supports | `B23025` | `civilian_labor_force`, `unemployed_count` | Covered |
| Household income distribution | `B19001` | `household_income_total`, income bins through `household_income_200000_plus` | Covered; added in this audit |
| Gross rent distribution | `B25063` | `gross_rent_distribution_total`, cash-rent bins through `gross_rent_distribution_cash_rent_3500_plus`, `gross_rent_distribution_no_cash_rent` | Covered; added in this audit |
| Rent burden bins | `B25070` | `gross_rent_pct_income_total`, burden bins through `gross_rent_pct_income_50_plus`, not-computed | Covered; added in this audit |
| Owner cost burden bins | `B25091` | with- and without-mortgage burden totals and bins through `50%+` | Covered; added in this audit |
| Tenure by household income | `B25118` | owner/renter income-bin counts | Covered; added in this audit |
| Existing median context | `B19013`, `B25064` | `median_household_income`, `median_gross_rent` | Covered; not suitable as allocation denominators |

## Implementation Notes

ACS5 tract ingest previously requested one near-limit API variable set. SAE
distribution support increases the variable count beyond one Census API request,
so `fetch_state_tract_data` now chunks variables by state and inner-joins the
chunk responses on `NAME`, `state`, `county`, and `tract`.

The canonical schema additions live in `ACS5_SAE_COUNT_COLUMNS` and are included
in `ACS5_COUNT_COLUMNS` and `ACS_TRACT_OUTPUT_COLUMNS`. These columns are
nullable `Int64` in curated tract artifacts. Median columns remain nullable
`Float64`; SAE v1 should derive median-like outputs from distributions rather
than averaging tract medians.

Provenance continues to record requested tables, requested variables,
unavailable variables for older vintages, raw content hash, tract-vintage
translation metadata, and row count. The raw snapshot payload for each state is
now a JSON bundle of per-chunk Census responses so snapshot hashing remains
deterministic while preserving request chunk boundaries.

## Gaps Found And Resolution

The existing tract ingest had core counts and medians but lacked tract-level
distribution supports for ACS1 county SAE:

- `B19001` household income distribution.
- `B25063` gross rent distribution.
- `B25070` gross rent burden bins.
- `B25091` owner cost burden bins.
- `B25118` tenure by household income.

These tables are now registered in `hhplab/acs/variables.py`, exposed through
the canonical tract schema in `hhplab/schema/columns.py`, fetched through the
chunked tract API path, and covered in `tests/test_acs_tract_population.py`.
