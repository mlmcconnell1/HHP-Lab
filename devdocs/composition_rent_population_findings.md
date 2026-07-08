# Compositional Effects on Rent Independent of Population

## Renter Household Size (ACS1 B25010)

Generated with:

```bash
uv run python scripts/build_household_size_composition_panel.py
```

This screen uses the pooled top-50 plus rank-51-150 MSA first-difference
design, excluding 2021. The base panel is merged to ACS1 metro-native B25010
with the standard lag rule: ACS1 vintage `E` is used for PIT year `E + 1`.

Generated, ignored artifacts:

- `outputs/composition_rent_population/household_size_composition_levels.parquet`
- `outputs/composition_rent_population/household_size_composition_fd.parquet`
- `outputs/composition_rent_population/household_size_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/household_size_composition_fd_regressions.csv`
- `outputs/composition_rent_population/household_size_composition_summary.json`

Coverage:

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS1 vintages used: 2009-2019 and 2021-2024
- Complete renter-household-size FD rows for the main rent/unsheltered screen: 1,078

Models use clustered standard errors by `msa_id` and year fixed effects.

| Model | Composition term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_average_household_size_total` | 0.0118 | 0.0172 | 0.490 | 1,084 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_average_household_size_owner_occupied` | 0.0132 | 0.0128 | 0.299 | 1,084 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_average_household_size_renter_occupied` | -0.0022 | 0.0068 | 0.748 | 1,084 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_average_household_size_total` | 0.2606 | 0.3273 | 0.426 | 1,078 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_average_household_size_owner_occupied` | 0.3003 | 0.2432 | 0.217 | 1,078 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_average_household_size_renter_occupied` | -0.1371 | 0.1561 | 0.380 | 1,078 |

Interpretation: average renter household-size shifts do not independently
predict rent growth or unsheltered-rate growth in this pooled FD screen. The
point estimate for renter household size is close to zero in the rent model
and negative but imprecise in the unsheltered model. This does not support the
household-size channel as an explanation for rent increases when population
growth is flat or falling.
