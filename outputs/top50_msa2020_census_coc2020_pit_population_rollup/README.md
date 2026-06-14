# Top 50 MSA 2020 PIT Population Rollup

This output rolls 2020 CoC PIT counts up to Census MSAs on a 2020 Census
block-population basis.

Files:

- `panel__top50_msa2020_census_coc2020_pit_population_rollup__Y2020@B2020_Dcensus_msa_2023.parquet`
- `panel__top50_msa2020_census_coc2020_pit_population_rollup__Y2020@B2020_Dcensus_msa_2023.csv`
- `panel__top50_msa2020_census_coc2020_pit_population_rollup__Y2020@B2020_Dcensus_msa_2023.preview.csv`
- `panel__top50_msa2020_census_coc2020_pit_population_rollup__Y2020@B2020_Dcensus_msa_2023.summary.json`
- `xwalk__B2020xMcensus_msa_2023xC2023xK2020xN2020__top50_population.parquet`

The panel grain is one row per `msa_id` and `year`. It includes the 50 largest
MSAs ranked by 2020 Census PL 94-171 block population aggregated over Census
MSA 2023 county membership.

PIT measures included:

- `pit_total`
- `pit_sheltered`
- `pit_unsheltered`

Allocation method:

- CoC boundaries: 2020 HUD Exchange CoC boundaries.
- MSA definitions: Census MSA 2023 county membership.
- Denominator: 2020 Census PL 94-171 block population.
- CoC PIT source: `data/curated/pit/pit_vintage__P2024.parquet`, filtered to
  `pit_year == 2020`.

The parquet panel and crosswalk include embedded `hhplab_provenance` metadata.
