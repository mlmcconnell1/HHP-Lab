# ACS1 County SAE Source Coverage Audit

Date: 2026-05-09
Bead: `coclab-uc9r.2`

This audit covers Census ACS 1-year county-native detailed-table ingest for the
first HHP-Lab small-area estimation (SAE) source aggregate pass. The county
artifact is written by `hhplab.acs.ingest.county_acs1.ingest_county_acs1` to
`data/curated/acs/acs1_county__A{vintage}.parquet`.

## Coverage Inventory

| SAE source family | ACS1 table | Canonical columns | Status |
| --- | --- | --- | --- |
| Labor force denominator and unemployment | `B23025` | `pop_16_plus`, `civilian_labor_force`, `unemployed_count`, `unemployment_rate_acs1` | Covered |
| Household income distribution | `B19001` | `household_income_total`, `household_income_*` bins through `household_income_200000_plus` | Covered; added in this audit |
| Household income quintile cutoffs | `B19080` | `household_income_quintile_cutoff_*`, `household_income_top_5pct_lower_limit` | Covered |
| Mean income by quintile | `B19081` | `mean_household_income_*_quintile`, `mean_household_income_top_5pct` | Covered |
| Aggregate income shares | `B19082` | `aggregate_income_share_*_quintile`, `aggregate_income_share_top_5pct` | Covered |
| Gross rent distribution | `B25063` | `gross_rent_distribution_total`, cash-rent bins through `gross_rent_distribution_cash_rent_3500_plus`, `gross_rent_distribution_no_cash_rent` | Covered; added in this audit |
| Median gross rent | `B25064` | `median_gross_rent` | Covered; downstream SAE should not average medians |
| Owner monthly costs | `B25088`, `B25089` | median and aggregate owner-cost columns by mortgage status | Covered |
| Gross rent burden bins | `B25070` | `gross_rent_pct_income_total`, bins from `<10%` through `50%+`, and not-computed | Covered |
| Owner cost burden bins | `B25091` | with- and without-mortgage totals, burden bins, and not-computed columns | Covered |
| Tenure by household income | `B25118` | owner/renter income-bin counts | Covered |
| Median household income by tenure | `B25119` | total, owner-occupied, renter-occupied medians | Covered; downstream SAE should not average medians |
| Utility cost supports | `B25132`, `B25133`, `B25134` | electricity, gas, and water/sewer cost bins | Covered for 2021+ only |
| Housing stock controls | `B25010`, `B25024`, `B25035`, `B25040`, `B25068` | household size, units in structure, structure age, heating fuel, rent by bedrooms | Covered |

## Availability And Vintage Handling

`ACS1_UNAVAILABLE_VINTAGES` marks 2020 as unavailable because Census did not
publish standard ACS 1-year estimates for that vintage. The shared ACS1 API
helper raises an actionable error before making API calls for 2020.

The registry uses `ACS1_TABLE_FIRST_YEAR` to preserve a stable output schema
while skipping tables unavailable for older vintages. Utility-cost tables
`B25132`, `B25133`, and `B25134` start in 2021; their columns are backfilled as
nullable values for earlier supported vintages.

## Schema, Types, And Provenance

`ACS1_COUNTY_OUTPUT_COLUMNS` is the canonical county artifact schema. Count-like
and median dollar columns are normalized to nullable `Int64`; ACS share and
average-size columns plus `unemployment_rate_acs1` are normalized to nullable
`Float64`.

Parquet outputs are written through `write_parquet_with_provenance`. Provenance
records `dataset_type=county_acs1`, `acs_product=acs1`, `tables_requested`,
`tables_fetched`, `tables_unavailable_for_vintage`, requested variables,
`api_year`, retrieval time, and row counts.

## Recipe Dataset Declarations

Recipe validation already accepts `provider: census`, `product: acs1`,
`version: 1` for `native_geometry.type` values `county` and `metro`.
Preflight remediation points county ACS1 datasets to:

```bash
hhplab ingest acs1-county --vintage <year>
```

County ACS1 is sparse by Census publication threshold; counties below ACS1
publication thresholds are absent by design and should be handled downstream as
coverage limitations, not ingest failures.

## Gaps Found And Resolution

Two source supports requested for SAE v1 were missing from the shared ACS1
registry before this audit:

- `B19001` household income distribution.
- `B25063` gross rent distribution.

Both tables are now registered in `hhplab/acs/variables_acs1.py`, included in
county and metro ACS1 fetches, exposed through `ACS1_COUNTY_OUTPUT_COLUMNS`, and
covered by `tests/test_ingest_county_acs1.py`.

No additional ingest gaps were found for the SAE source families listed above.
