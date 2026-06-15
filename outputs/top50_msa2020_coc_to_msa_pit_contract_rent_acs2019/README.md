# Top 50 MSA PIT and Contract Rent Panel

This output joins the 2020 CoC-to-MSA PIT rollup to ACS 2019 first-quartile
contract rent at MSA grain.

Files:

- `panel__top50_msa2020_coc_to_msa_pit_contract_rent_acs2019__Y2020@B2020_Dcensus_msa_2023_A2019.parquet`
- `panel__top50_msa2020_coc_to_msa_pit_contract_rent_acs2019__Y2020@B2020_Dcensus_msa_2023_A2019.csv`
- `panel__top50_msa2020_coc_to_msa_pit_contract_rent_acs2019__Y2020@B2020_Dcensus_msa_2023_A2019.preview.csv`
- `panel__top50_msa2020_coc_to_msa_pit_contract_rent_acs2019__Y2020@B2020_Dcensus_msa_2023_A2019.summary.json`

The panel grain is one row per `msa_id` and `year`.

PIT source:

- `outputs/top50_msa2020_census_coc2020_pit_population_rollup_pit2020_vintage/panel__top50_msa2020_census_coc2020_pit_population_rollup_pit2020_vintage__Y2020@B2020_Dcensus_msa_2023.parquet`
- PIT year: 2020
- PIT vintage: 2020
- CoC-to-MSA allocation: 2020 CoC boundaries and 2020 Census PL 94-171 block
  population, allocated to Census MSA 2023 county definitions.

Rent source:

- `data/curated/acs_contract_rent_cache/acs5_contract_rent_tracts__A2019.parquet`
- ACS vintage: 2019
- Measure: `first_quartile_contract_rent_acs2019_dollars`
- Method: sum ACS5 B25056 contract-rent distribution bins over Census MSA
  2023 county membership, then linearly interpolate the 25th percentile from
  the pooled cash-rent distribution.

Notes:

- Contract rent excludes utilities.
- ACS 2019 is used for PIT-year alignment under the project ACS lag rule.
- San Juan-Bayamon-Caguas, PR has null first-quartile contract rent because
  the local ACS 2019 tract contract-rent cache has no matching Puerto Rico
  tracts. The row is retained with diagnostics in
  `contract_rent_quantile_diagnostics`.
- The parquet panel includes embedded `hhplab_provenance` metadata.
