# Compositional Effects on Rent Independent of Population

## Overall Result

The three household-composition screens do not support a positive
composition-driven explanation for rent growth independent of population
growth. Renter tenure share is negatively associated with rent growth in the
pooled FD screen and null for unsheltered-rate growth; renter household size
is null; and recent-mover income ratios are null. The compositional channels
tested here therefore do not explain the earlier finding that population
growth and rent growth are nearly orthogonal across the pooled top-150 MSA
design.

## Renter Household Share (ACS5 B25003)

Generated with:

```bash
uv run python scripts/build_renter_household_share_composition_panel.py
```

This screen uses the pooled top-50 plus rank-51-150 MSA first-difference
design, excluding 2021. The base panel is merged to existing ACS5-to-MSA
curated measures from `data/curated/measures/measures__msa__A*.parquet` with
the standard lag rule: ACS5 vintage end year `E` is used for PIT year `E + 1`.

Generated, ignored artifacts:

- `outputs/composition_rent_population/renter_household_share_composition_levels.parquet`
- `outputs/composition_rent_population/renter_household_share_composition_fd.parquet`
- `outputs/composition_rent_population/renter_household_share_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/renter_household_share_composition_fd_regressions.csv`
- `outputs/composition_rent_population/renter_household_share_composition_summary.json`

Coverage:

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS5 vintages used: 2009-2019 and 2021-2024
- Complete renter-household-share FD rows for the main unsheltered screen: 1,090

Median level composition:

| Measure | Median |
| --- | ---: |
| Renter households per ACS person | 0.131 |
| Renter households per panel person | 0.128 |
| Renter households / total households | 0.346 |
| Owner households / total households | 0.654 |
| Renter households / owner households | 0.528 |

Key FD models:

| Model | Composition term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_household_share` | -0.4011 | 0.1800 | 0.026 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_household_share` | -0.4427 | 2.4536 | 0.857 | 1,090 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_households_per_acs_person` | -0.8476 | 0.4515 | 0.060 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_households_per_acs_person` | 0.1050 | 5.8306 | 0.986 | 1,090 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_households_per_panel_person` | -1.5965 | 0.5714 | 0.005 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_households_per_panel_person` | -3.2731 | 6.4402 | 0.611 | 1,090 |

Interpretation: rising renter tenure share does not explain rent increases
when population is flat or falling. In this pooled FD screen the rent
coefficients are negative, not positive, and the unsheltered-rate coefficients
are imprecise and null. This rejects the simple tenure-mix channel: MSAs do
not appear to experience higher rent growth because a larger share of their
households are renters, after controlling for population growth and year
effects.

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
