# Top 50 MSA 2020 CoC PIT and Presidential Votes Panel

This output rolls 2020 CoC PIT counts up to Census MSAs and joins 2020 U.S.
presidential vote counts for the same top-50 MSA universe.

Files:

- `panel__top50_msa2020_coc_pit_president_votes_2020__Y2020@B2020_Dcensus_msa_2023.parquet`
- `panel__top50_msa2020_coc_pit_president_votes_2020__Y2020@B2020_Dcensus_msa_2023.csv`
- `panel__top50_msa2020_coc_pit_president_votes_2020__Y2020@B2020_Dcensus_msa_2023.preview.csv`
- `panel__top50_msa2020_coc_pit_president_votes_2020__Y2020@B2020_Dcensus_msa_2023.summary.json`

The panel grain is one row per `msa_id` and `year`. It includes the 50 largest
MSAs ranked by 2020 Census PL 94-171 block population aggregated over Census
MSA 2023 county membership.

PIT measures included:

- `pit_total`
- `pit_sheltered`
- `pit_unsheltered`

Vote measures included:

- `republican_votes`
- `democratic_votes`
- `two_party_votes`
- `totalvotes`
- `republican_vote_share`
- `democratic_vote_share`
- `democratic_margin_votes`
- `democratic_margin_share`
- `major_party_vote_share`

Inputs:

- PIT rollup:
  `outputs/top50_msa2020_census_coc2020_pit_population_rollup_pit2020_vintage/panel__top50_msa2020_census_coc2020_pit_population_rollup_pit2020_vintage__Y2020@B2020_Dcensus_msa_2023.parquet`
- MEDSL curated county presidential artifact:
  `data/curated/medsl/medsl_president_county__Y2000-2024@C2020.parquet`
- MEDSL raw county presidential file:
  `data/raw/medsl/countypres_2000-2024.tab`
- MSA county membership:
  `data/curated/msa/msa_county_membership__census_msa_2023.parquet`

Vote aggregation method:

- County Democratic and Republican presidential votes are summed over Census
  MSA 2023 county membership.
- For selected non-Puerto Rico counties absent from the curated MEDSL 2020
  artifact, raw 2020 MEDSL mode-level rows are summed by county and party.
  This fallback is tracked in bead `coclab-kb8j`.
- Puerto Rico MSA rows are retained in the top-50 population universe and
  marked `presidential_votes_applicable == false`; presidential vote columns
  are null for those rows.

The parquet panel includes embedded `hhplab_provenance` metadata.
