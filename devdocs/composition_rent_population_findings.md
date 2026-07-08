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

## Recent-Mover Income (ACS1 B07011)

Generated with:

```bash
uv run python scripts/build_recent_mover_income_composition_panel.py
```

B07011 reports median income by mobility origin, but not a single all-movers
median. This screen therefore keeps origin-specific ratios explicit. The main
gentrification-pressure proxy is:

```text
median_income_moved_diff_state / median_income_mobility_total
```

Generated, ignored artifacts:

- `outputs/composition_rent_population/recent_mover_income_composition_levels.parquet`
- `outputs/composition_rent_population/recent_mover_income_composition_fd.parquet`
- `outputs/composition_rent_population/recent_mover_income_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/recent_mover_income_composition_fd_regressions.csv`
- `outputs/composition_rent_population/recent_mover_income_composition_summary.json`

Coverage:

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Complete different-state mover-income-ratio FD rows: 1,078

Median level ratios to total ACS1 mobility-universe median income:

| Mobility origin | Median ratio |
| --- | ---: |
| Moved within county | 0.859 |
| Moved from different county, same state | 0.817 |
| Moved from different state | 0.880 |
| Moved from abroad | 0.616 |

Key FD models:

| Model | Composition term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_moved_diff_state_income_ratio_total` | 0.0001 | 0.0026 | 0.984 | 1,084 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_moved_diff_state_income_ratio_total` | -0.0207 | 0.0561 | 0.712 | 1,078 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_moved_diff_county_same_state_income_ratio_total` | 0.0045 | 0.0032 | 0.153 | 1,084 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_moved_diff_county_same_state_income_ratio_total` | -0.0688 | 0.0646 | 0.286 | 1,078 |

Interpretation: B07011 does not support the "incoming residents richer than
standing population" channel in this pooled FD design. Different-state movers
have a median income ratio below 1 in levels, and changes in that ratio do not
independently predict rent growth or unsheltered-rate growth. This is broadly
consistent with the earlier IRS SOI screen's low mover-income signal
(approximately 0.73x), while using a different income concept: Census
individual median income by mobility origin rather than IRS AGI per return.
