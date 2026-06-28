# Top 50 Non-PR MSA 2020 PIT and CDC Overdose Panel

This exploratory output builds one row per Census MSA for the 50 largest MSAs by
2020 county PEP population after excluding Puerto Rico counties from the ranking
universe.

Files:

- `panel__top50_msa2020_coc_pit_cdc_overdose_2020__Y2020@B2020_Mcensus_msa_2023.parquet`
- `panel__top50_msa2020_coc_pit_cdc_overdose_2020__Y2020@B2020_Mcensus_msa_2023.csv`
- `panel__top50_msa2020_coc_pit_cdc_overdose_2020__Y2020@B2020_Mcensus_msa_2023.preview.csv`
- `panel__top50_msa2020_coc_pit_cdc_overdose_2020__Y2020@B2020_Mcensus_msa_2023.summary.json`

Panel grain: `msa_id x year`, filtered to `year == 2020`.

Included measures:

- `pit_total`, `pit_sheltered`, `pit_unsheltered`: HUD PIT counts rolled from
  CoC geography to Census MSA geography with the stored B2020 CoC-to-MSA area
  allocation crosswalk.
- `overdose_deaths_12mo`: CDC VSRR provisional county overdose deaths already
  aggregated to MSA geography from January trailing-12-month county rows.

Selection and exclusion rules:

- MSA definition: `census_msa_2023`.
- Ranking source: `data/curated/pep/pep_county__v2025__y2020-2025.parquet`,
  filtered to 2020.
- Puerto Rico exclusion: counties whose FIPS starts with `72` are removed before
  MSA population ranking. No Puerto Rico MSA is included in the final top 50.

Input artifacts:

- PIT: `data/curated/pit/pit_vintage__P2025.parquet` filtered to `pit_year == 2020`.
- CoC-to-MSA crosswalk: `data/curated/xwalks/msa_coc_xwalk__B2020xMcensus_msa_2023xC2023.parquet`.
- CDC overdose MSA: `data/curated/cdc/cdc_overdose__msa__Y2020-2025@Mcensusmsa2023xC2023.parquet` filtered to `year == 2020`.
- MSA definitions: `data/curated/msa/msa_definitions__census_msa_2023.parquet`.
- MSA county membership: `data/curated/msa/msa_county_membership__census_msa_2023.parquet`.

Summary:

- Rows: 50
- PIT total sum across selected MSAs: 382078.533
- Overdose 12-month deaths sum across selected MSAs: 34646.000
- Minimum overdose county coverage ratio: 0.375
- Minimum PIT allocation coverage ratio: 0.860

The parquet file includes embedded `hhplab_provenance` metadata.
