# Top 50 Non-PR Census MSAs: 2020 CoC PIT Rollup with DOJ Sanctuary Status

This output is a 2020 MSA-year panel for the top 50 non-Puerto Rico Census MSAs
by 2020 Census PL 94-171 population. It starts from the existing non-PR top-50
CoC PIT rollup panel and joins DOJ sanctuary-jurisdiction MSA matches on
`cbsa_code`.

Files:

- `outputs/top50_msa_nonpr_coc_pit_sanctuary_2020/panel__top50_msa_nonpr_coc_pit_sanctuary_2020__Y2020@B2020_Dcensus_msa_2023_D20250805.parquet`
- `outputs/top50_msa_nonpr_coc_pit_sanctuary_2020/panel__top50_msa_nonpr_coc_pit_sanctuary_2020__Y2020@B2020_Dcensus_msa_2023_D20250805.csv`
- `outputs/top50_msa_nonpr_coc_pit_sanctuary_2020/panel__top50_msa_nonpr_coc_pit_sanctuary_2020__Y2020@B2020_Dcensus_msa_2023_D20250805.preview.csv`
- `outputs/top50_msa_nonpr_coc_pit_sanctuary_2020/panel__top50_msa_nonpr_coc_pit_sanctuary_2020__Y2020@B2020_Dcensus_msa_2023_D20250805.summary.json`

Panel grain: one row per `msa_id` and `year` for 2020.

Included PIT measures:

- `pit_total`
- `pit_sheltered`
- `pit_unsheltered`

Included sanctuary status columns:

- `sanctuary_match`
- `sanctuary_state_match`
- `sanctuary_county_match`
- `sanctuary_city_match`
- `sanctuary_matched_states`
- `sanctuary_matched_counties`
- `sanctuary_matched_cities`
- `match_basis`

Sanctuary matching source:

- `data/curated/sanctuary/sanctuary_msa_matches__D20250805xMcensus_msa_2023.parquet`
- DOJ source date: 2025-08-05

Notes:

- Puerto Rico MSAs are excluded before selecting the top 50 in the base panel.
- Non-matched MSAs are retained with `sanctuary_match=false` and
  `match_basis=none`.
- Sanctuary state matches are intentionally broad: if a DOJ-listed state touches
  an MSA through at least one component county, the MSA is marked as a state
  match and `sanctuary_matched_states` lists only the matched state components.
- PIT counts come from the existing tracked base panel
  `outputs/top50_msa_nonpr_coc_pit_contract_rent_2010_2020/panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020.parquet`.
