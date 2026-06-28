# Top 50 Non-PR Census MSAs: CoC PIT and ACS5 Contract Rent, 2010/2020

This requested output rolls HUD CoC PIT counts to the top 50 non-Puerto Rico
Census MSAs by 2020 PL 94-171 block population, using Census MSA 2023 county
membership for the population ranking.

Files:

- `outputs/top50_msa_nonpr_coc_pit_contract_rent_2010_2020/panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020.parquet`
- `outputs/top50_msa_nonpr_coc_pit_contract_rent_2010_2020/panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020.csv`
- `outputs/top50_msa_nonpr_coc_pit_contract_rent_2010_2020/panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020.preview.csv`
- `outputs/top50_msa_nonpr_coc_pit_contract_rent_2010_2020/panel__top50_msa_nonpr_coc_pit_contract_rent_2010_2020__Y2010-2020@B2020_Dcensus_msa_2023_A2010-2020.summary.json`

Panel grain: one row per `msa_id` and `year` for 2010 and 2020.

Included measures:

- CoC PIT rollup: `pit_total`, `pit_sheltered`, `pit_unsheltered`
- ACS5 contract-rent bins from terminal years 2010 and 2020
- `median_contract_rent_weighted`, a cash-renter-household weighted mean of tract
  `median_contract_rent`

Allocation notes:

- PIT uses the existing 2020 CoC boundary to Census MSA 2023 area allocation:
  `data/curated/xwalks/msa_coc_xwalk__B2020xMcensus_msa_2023xC2023.parquet`.
- ACS uses Census MSA 2023 county membership; tracts are assigned to MSAs by
  their county FIPS because Census MSAs are county-defined.
- Puerto Rico MSAs are excluded before selecting the top 50.
