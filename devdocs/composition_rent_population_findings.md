# Compositional Effects on Rent Independent of Population

## Overall Result

The five composition and direct-demand screens do not support a positive,
state-year-robust explanation for rent growth independent of population
growth. Renter tenure share is negatively associated with rent growth under
plain year FE and under pooled MSA+year levels FE, but the first-difference
rent result does not survive a state x year FE check (see addenda below) and
the unsheltered-rate result is null either way; the direct household-formation
proxy (`d_log_total_households - d_log_pop`, implemented as
`d_log_total_households_per_panel_person`) is also negative under plain year FE
and goes null under region/state x year FE; renter household size is null;
recent-mover income ratios are null; and the new direct local-income screen
finds positive ACS1 median-income growth coefficients under plain year FE and,
for renter income, region x year FE, but both growth coefficients collapse
under state x year FE. The new ACS5 income-inequality (`gini_index`) screen
leans negative rather than positive in first differences and also goes null
once region/state x year shocks are absorbed, while inequality levels are null
throughout. Income levels themselves do line up strongly with rent levels even
after MSA and state-year fixed effects, especially renter median income, so
richer metros are more expensive. What is *not* supported is the claim that
within-state annual local income growth or rising local inequality
independently explains year-to-year rent growth.

## Renter Household Share (ACS5 B25003)

Generated with:

```bash
uv run hhplab build result renter-household-share-composition --json
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

| Model | Screen term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_household_share` | -0.4011 | 0.1800 | 0.026 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_household_share` | -0.4427 | 2.4536 | 0.857 | 1,090 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_households_per_acs_person` | -0.8476 | 0.4515 | 0.060 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_households_per_acs_person` | 0.1050 | 5.8306 | 0.986 | 1,090 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_renter_households_per_panel_person` | -1.5965 | 0.5714 | 0.005 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_renter_households_per_panel_person` | -3.2731 | 6.4402 | 0.611 | 1,090 |
| `d_log_zori ~ d_log_pop + composition + year FE` | `d_log_total_households_per_panel_person` | -0.3577 | 0.1619 | 0.027 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + composition + year FE` | `d_log_total_households_per_panel_person` | -1.1013 | 1.5358 | 0.473 | 1,090 |

Interpretation: rising renter tenure share does not explain rent increases
when population is flat or falling. In this pooled FD screen the rent
coefficients are negative, not positive, and the unsheltered-rate coefficients
are imprecise and null. This rejects the simple tenure-mix channel: MSAs do
not appear to experience higher rent growth because a larger share of their
households are renters, after controlling for population growth and year
effects.

**2026-07-08 addendum: the one significant result here (`d_renter_household_share`
on rent, p=0.026) weakens under region x year FE and does not survive a
state x year FE check.** Following the same state-level-confound audit
already applied to several other findings this project has made (see
`devdocs/state_level_robustness_rechecks.md` and the Vera jail work), re-ran
this spec with both `region x year` and `primary_state x year` fixed effects
instead of plain year FE, on the identical n=1096 sample:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_renter_household_share` (year FE) | -0.401 | 0.180 | 0.026 | 1,096 |
| `d_renter_household_share` (region x year FE) | -0.290 | 0.165 | 0.079 | 1,096 |
| `d_renter_household_share` (state x year FE) | -0.177 | 0.218 | 0.415 | 1,096 |

The region x year tier absorbs shocks common to an entire multi-state Census
region in a given year; the state x year tier absorbs state-specific shocks
and policy confounds, while identifying only from within-state, cross-MSA
variation. These are complementary diagnostics, not one loose and one strict
version of the same test. Read the ladder as follows: surviving all three is
the strongest pattern; surviving region x year but dying under state x year
points toward a state-specific confound; dying even under region x year
points toward a broader multi-state regional confound. Here the coefficient
attenuates under region x year and evaporates under state x year, so renter
tenure share does not survive as a first-difference rent-growth predictor
once regional and state-year confounding are checked, although the later
tracked levels-FE check below finds a strong negative pooled levels
association.

**2026-07-09 addendum: the direct household-formation proxy also fails the
same regional/state-year robustness ladder.** This proxy is
`d_log_total_households - d_log_pop`, implemented in the tracked workflow as
`d_log_total_households_per_panel_person`, and directly tests whether more
households are forming per person even when headcount growth is flat. On the
same pooled first-difference sample (n=1096), the year-FE coefficient is
negative and nominally significant, but it attenuates and goes null once the
same confound audit is applied:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_log_total_households_per_panel_person` (year FE) | -0.358 | 0.162 | 0.027 | 1,096 |
| `d_log_total_households_per_panel_person` (region x year FE) | -0.135 | 0.170 | 0.427 | 1,096 |
| `d_log_total_households_per_panel_person` (state x year FE) | -0.094 | 0.183 | 0.609 | 1,096 |

So the data do not support the story that faster household formation per
person is a positive independent driver of MSA rent growth in this design.
If anything, the raw pooled year-FE association points in the wrong direction,
and even that disappears once regional and state-year confounding are
absorbed.

## Renter Household Size (ACS1 B25010)

Generated with:

```bash
uv run hhplab build result household-size-composition --json
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
uv run hhplab build result recent-mover-income-composition --json
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

## Local Income (ACS1 B25119)

Generated with:

```bash
uv run hhplab build result local-income-composition --json
uv run hhplab build result composition-rent-population-robustness --json
```

Rather than falling back to ACS5 `median_household_income` or
`per_capita_income`, this screen uses the metro-native annual ACS1 B25119
medians already present in the curated ACS1 artifacts:

- `median_household_income_by_tenure_total`
- `median_household_income_renter_occupied`

These are converted to logs and tested as direct local-income growth screens
against `d_log_zori`, with the usual pooled top-150 MSA design, clustered
standard errors by `msa_id`, and the standard ACS1 lag rule (`E -> E + 1`).

Generated, ignored artifacts:

- `outputs/composition_rent_population/local_income_composition_levels.parquet`
- `outputs/composition_rent_population/local_income_composition_fd.parquet`
- `outputs/composition_rent_population/local_income_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/local_income_composition_fd_regressions.csv`
- `outputs/composition_rent_population/local_income_composition_summary.json`

Coverage:

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS1 vintages used: 2009-2019 and 2021-2024
- Complete FD rows for total median-income growth: 1,078
- Median ACS1 level income: total household income = 61,876; renter household income = 38,720

Key year-FE first-difference models:

| Model | Income term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + income_growth + year FE` | `d_log_median_household_income_by_tenure_total` | 0.0319 | 0.0180 | 0.076 | 1,084 |
| `d_log_zori ~ d_log_pop + income_growth + year FE` | `d_log_median_household_income_renter_occupied` | 0.0233 | 0.0081 | 0.0039 | 1,084 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + income_growth + year FE` | `d_log_median_household_income_by_tenure_total` | 0.2650 | 0.3516 | 0.451 | 1,078 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + income_growth + year FE` | `d_log_median_household_income_renter_occupied` | -0.0247 | 0.1763 | 0.889 | 1,078 |

The raw year-FE read is suggestive: metros with faster renter-income growth
also show faster rent growth, with a smaller and only marginal total-income
growth coefficient.

But the robustness ladder says that signal is not stable to tighter spatial
confound absorption:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_log_median_household_income_by_tenure_total` (year FE) | 0.0319 | 0.0180 | 0.076 | 1,084 |
| `d_log_median_household_income_by_tenure_total` (region x year FE) | 0.0290 | 0.0182 | 0.110 | 1,084 |
| `d_log_median_household_income_by_tenure_total` (state x year FE) | 0.0056 | 0.0179 | 0.753 | 1,084 |
| `d_log_median_household_income_renter_occupied` (year FE) | 0.0233 | 0.0081 | 0.0039 | 1,084 |
| `d_log_median_household_income_renter_occupied` (region x year FE) | 0.0166 | 0.0075 | 0.0266 | 1,084 |
| `d_log_median_household_income_renter_occupied` (state x year FE) | 0.0080 | 0.0083 | 0.337 | 1,084 |

So the direct local-income-growth story does **not** survive the same
state-year robustness check used elsewhere in this project. Renter income
growth survives the intermediate region-year tier, but once identification is
restricted to within-state, cross-MSA variation in the same year, the effect
shrinks by roughly two-thirds and becomes null.

Income *levels* are a different story. On the balanced ZORI-covered levels
sample (`n=1357`, `137` MSAs), both annual ACS1 income measures are strongly
positive predictors of rent levels even after MSA fixed effects:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `log_median_household_income_by_tenure_total` (msa + year FE) | 0.533 | 0.083 | 1.49e-10 | 1,357 |
| `log_median_household_income_by_tenure_total` (msa + region x year FE) | 0.532 | 0.087 | 8.72e-10 | 1,357 |
| `log_median_household_income_by_tenure_total` (msa + state x year FE) | 0.209 | 0.066 | 0.0015 | 1,357 |
| `log_median_household_income_renter_occupied` (msa + year FE) | 0.237 | 0.048 | 7.52e-07 | 1,357 |
| `log_median_household_income_renter_occupied` (msa + region x year FE) | 0.200 | 0.049 | 4.70e-05 | 1,357 |
| `log_median_household_income_renter_occupied` (msa + state x year FE) | 0.0927 | 0.0409 | 0.0234 | 1,357 |

Interpretation: richer metros, and metros whose renters are richer, do have
higher rent levels in a robust within-MSA levels design. What this screen does
*not* show is that year-to-year local income growth is a robust independent
driver of year-to-year rent growth once state-specific shocks are absorbed.

## Income Inequality (ACS5 B19083)

Generated with:

```bash
uv run hhplab build result income-inequality-composition --json
uv run hhplab build result composition-rent-population-robustness --json
```

This screen uses the tract-derived ACS5 `gini_index` already present in the
curated MSA measures. As documented in
`devdocs/acs5_expanded_covariates.md`, this is a population-weighted average
of tract Gini estimates, so it should be read as an inequality proxy rather
than a true pooled-geography Gini. The workflow uses the standard ACS5 lag
rule (`E -> E + 1`) and the same pooled top-150 MSA design as the other
composition channels.

Generated, ignored artifacts:

- `outputs/composition_rent_population/income_inequality_composition_levels.parquet`
- `outputs/composition_rent_population/income_inequality_composition_fd.parquet`
- `outputs/composition_rent_population/income_inequality_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/income_inequality_composition_fd_regressions.csv`
- `outputs/composition_rent_population/income_inequality_composition_summary.json`

Coverage:

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS5 vintages used: 2009-2019 and 2021-2024
- Complete FD rows for `gini_index`: 1,090
- Median level `gini_index`: 0.4086

Key year-FE first-difference models:

| Model | Inequality term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + d_gini_index + year FE` | `d_gini_index` | -0.5406 | 0.2503 | 0.0308 | 1,096 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + d_gini_index + year FE` | `d_gini_index` | -7.8369 | 3.9482 | 0.0472 | 1,090 |

The raw year-FE read is already opposite the motivating "barbell" story:
metros with faster increases in local inequality show *slower* rent growth on
average, not faster. And that negative sign does not survive tighter spatial
confound absorption:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_gini_index` (year FE) | -0.5406 | 0.2503 | 0.0308 | 1,096 |
| `d_gini_index` (region x year FE) | -0.3840 | 0.2184 | 0.0787 | 1,096 |
| `d_gini_index` (state x year FE) | -0.2901 | 0.2151 | 0.1775 | 1,096 |

Inequality *levels* also fail to line up with rent levels in the tracked
within-MSA checks:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `gini_index` (msa + year FE) | -0.434 | 0.598 | 0.468 | 1,370 |
| `gini_index` (msa + region x year FE) | -0.457 | 0.604 | 0.449 | 1,370 |
| `gini_index` (msa + state x year FE) | -0.192 | 0.666 | 0.773 | 1,370 |

Interpretation: this ACS5 inequality proxy does **not** support the claim that
rising local inequality is a positive, independent rent-growth channel in the
pooled top-150 MSA design. If anything, the raw association points in the
wrong direction, and even that disappears once the same regional/state-year
confound ladder used elsewhere in the project is applied.

## 2026-07-08 Code Review Addendum

Requested review of correctness, completeness, corner cases, and test
coverage for all four closed beads under this epic. Reread all three
scripts and independently reran each one end to end, reproducing every
reported coefficient in this document exactly (to the reported decimal
places). Also checked: `_safe_ratio`/`_safe_log` correctly guard
divide-by-zero and log-of-nonpositive by producing NaN rather than
inf/warnings; the ACS5/ACS1 lag alignment (vintage end year `E` -> PIT year
`E + 1`) is applied consistently and matches the project convention used
elsewhere; the glob patterns correctly scope to a single MSA definition
version (`Dcensusmsa2023` / `Mcensusmsa2023`) with no duplicate-vintage
fan-out into the merge; B25003's `total_households` equals
`owner_households + renter_households` exactly in every row (zero mismatch
across 1,750 levels rows); B07011's cell order was re-verified byte-for-byte
against the live Census API (`api.census.gov/data/2023/acs/acs1/groups/B07011.json`)
and confirmed non-null (98-99%) across the full 2009-2024 ingestion, not
just a handful of spot-checked years. No correctness bugs found.

Two completeness gaps found, neither of which changes any conclusion:

1. **The levels-FE (entity+year) robustness spec needed to be tracked, and
   the original prose-only check was wrong for renter tenure share.** The
   committed robustness script now runs the paired levels-FE convention used
   elsewhere in this project:

   ```bash
   uv run hhplab build result composition-rent-population-robustness --json
   ```

   The tracked pooled-MSA levels-FE check on the current
   `renter_household_share_composition_levels.parquet` complete-case sample
   is `log_zori ~ log_pop + renter_household_share + msa FE + year FE`,
   clustered by `msa_id`, with n=1370 (137 MSAs x 10 ZORI-covered years).
   It estimates `renter_household_share` b=-2.018, SE=0.446, p=5.97e-06.
   This is the opposite sign and significance from the earlier unsaved
   addendum number (`b=+3.97, p=0.231, n=1364`), which could not be
   reproduced from the committed panel artifacts. A from-scratch LSDV
   replication matches the tracked script, and plausible alternative specs
   do not recover the old number. The closest diagnostic clue is that using
   the complementary `owner_household_share` reverses the sign, and a
   top-50-only owner-share analogue is near the old magnitude, but the old
   ad hoc check was not committed and cannot be audited beyond that.

   The six-row n difference also appears to be a symptom of the old ad hoc
   calculation rather than a current sample-construction rule: the current
   renter-share levels sample is exactly balanced over 137 MSAs and 10
   years. The ACS1 levels-FE samples are n=1357, not the old n=1351, because
   the same 137 ZORI-covered MSAs are narrowed only by ACS1 measure coverage
   (`average_household_size_renter_occupied` b=+0.058, p=0.091;
   `moved_diff_state_income_ratio_total` b=+0.016, p=0.183). Those two
   ACS1 levels checks remain conventionally null, but renter tenure share
   does not: it is a strong negative levels association with rent after
   MSA and year fixed effects.
2. **No test coverage for any of the three scripts**, despite this
   project's established precedent for testing exactly this class of
   tracked analysis script end to end with synthetic fixtures
   (`tests/test_build_supply_iv_panel.py`). Filed as
   `coclab-composition-panel-test-coverage` (P3) rather than fixed here --
   real engineering work, not a quick addendum. The safe_ratio/safe_log
   guards and the ACS-lag merge are the two places a silent bug would most
   plausibly hide and should be the first things covered.

## 2026-07-08 Addendum: renter tenure share's levels result survives region and state x year FE

The corrected `renter_household_share` levels-FE result above (b=-2.018,
p=5.97e-06, msa+year FE) had not yet been run through the full three-tier
fixed-effect ladder described above. Added
`rent_levels_renter_household_share_msa_region_year_fe` and
`rent_levels_renter_household_share_msa_state_year_fe` to
`uv run python -m hhplab.results.workflows.analyze_composition_rent_population_robustness`
and re-ran:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `renter_household_share` (msa + year FE) | -2.018 | 0.446 | 5.97e-06 | 1,370 |
| `renter_household_share` (msa + region x year FE) | -2.053 | 0.469 | 1.21e-05 | 1,370 |
| `renter_household_share` (msa + state x year FE) | -1.756 | 0.581 | 0.0025 | 1,370 |

**This survives.** The coefficient attenuates by about 13% but stays
significant at p=0.0025. The design matrix is rank-deficient (568 columns,
rank 526) because 14 of 43 states have only one MSA -- for those, the MSA
dummy and the state-year dummies are collinear and the pseudo-inverse
solution assigns them zero marginal contribution. Verified this isn't a
numerical artifact: restricting to only the 29 states with 2+ MSAs (123
MSAs, n=1,230) gives the identical coefficient to 10 decimal places
(-1.7563115377808844 vs -1.7563115377813843), just a smaller-cluster p-value
(0.0013). So the identifying variation is entirely the well-identified
multi-MSA-state subsample, and the result is not an artifact of the
singleton-state collinearity.

**This makes renter household share (in levels, not first-differenced) the
second result in this whole two-epic investigation to survive a state x
year FE check**, after housing supply constraint. Net reading: MSAs with a
structurally higher renter share have persistently lower rent levels after
controlling for population, MSA fixed effects, and state-specific year
shocks -- a real, robust cross-sectional/levels association, even though the
corresponding first-differenced (growth-on-growth) version of the same
variable does not survive the same check (see the 2026-07-08 addendum
above). This does not change the overall "no compositional channel explains
rent *growth* independent of population growth" conclusion, since the FD
spec is the one that speaks to growth; it does mean renter tenure share is
not simply a null variable overall -- it has a real, level-effect
relationship with rent that a future analysis of *why* rents differ across
metros (as opposed to why they're *rising*) should not ignore.

## 2026-07-09 Addendum: renter tenure share's homelessness link is purely cross-sectional

The exploratory pass behind `coclab-1m4ev` found a strong positive raw
correlation between `renter_household_share` and unsheltered homelessness on
the same committed levels panel now used by the tracked robustness workflow:
pooled MSA-year rows give `r=0.442` (`n=1716`), and collapsing to one row per
MSA by averaging over time still gives `r=0.488` (`n=150`). That makes the
question worth keeping, but the tracked within-MSA checks are completely null.

Tracked with:

```bash
uv run python -m hhplab.results.workflows.analyze_composition_rent_population_robustness
```

Levels, clustered by `msa_id`:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `renter_household_share` (msa + year FE) | -1.020 | 2.909 | 0.726 | 1,716 |
| `renter_household_share` (msa + region x year FE) | +0.179 | 2.822 | 0.949 | 1,716 |
| `renter_household_share` (msa + state x year FE) | -0.176 | 4.242 | 0.967 | 1,716 |

First differences, clustered by `msa_id`:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_renter_household_share` (year FE) | -3.298 | 2.334 | 0.158 | 1,420 |
| `d_renter_household_share` (region x year FE) | -3.669 | 2.463 | 0.136 | 1,420 |
| `d_renter_household_share` (state x year FE) | -1.427 | 3.333 | 0.668 | 1,420 |

This is a clean between-versus-within split. Metros that are structurally more
renter-heavy also tend to have structurally higher unsheltered rates, but the
relationship vanishes once each metro is compared to itself over time. The
same variable that has a robust levels association with rent does **not** have
any detectable within-MSA relationship with unsheltered homelessness, either
in levels-with-MSA-FE or in first differences.

## 2026-07-09 Addendum: rent levels do not robustly bridge renter share to homelessness

Because renter share's real signal sits in rent *levels* while the project's
headline rent result sits in homelessness *growth*, the missing bridge is a
levels-to-levels rent check: does `log_zori` predict `log_unshelt_rate` on the
same levels panel?

Without `log_pop`:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `log_zori` (msa + year FE) | +0.835 | 0.467 | 0.074 | 1,364 |
| `log_zori` (msa + region x year FE) | +0.552 | 0.471 | 0.242 | 1,364 |
| `log_zori` (msa + state x year FE) | +1.030 | 0.633 | 0.104 | 1,364 |

With `log_pop` added:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `log_zori` (msa + year FE) | +0.936 | 0.441 | 0.034 | 1,364 |
| `log_zori` (msa + region x year FE) | +0.450 | 0.457 | 0.326 | 1,364 |
| `log_zori` (msa + state x year FE) | +0.976 | 0.629 | 0.121 | 1,364 |

This stays positive-signed throughout and is the same rough order of magnitude
as the tracked growth elasticity, but it only clears conventional significance
under plain `msa + year` FE with `log_pop` added. Once region x year or state
x year shocks are absorbed, the levels-based rent-to-homelessness link becomes
too noisy to count as a robust bridge. So there is still no well-supported
empirical chain connecting renter share to homelessness through rent in this
dataset: renter share predicts rent levels, rent growth predicts homelessness
growth, but the direct levels-to-levels bridge is weak and non-robust.

## 2026-07-09 Addendum: renter tenure share does not moderate the rent-growth elasticity

`coclab-u25pu` asked whether higher-renter-share metros are "more fertile
ground" for rent shocks to pass through into unsheltered homelessness growth.
The tracked workflow now fits the interaction on the same complete-case sample
as the core elasticity spec (`n=1090`, `137` MSAs), centering
`renter_household_share` at the sample mean `0.355` before interacting it with
`d_log_zori`.

| FE tier | `d_log_zori` | SE | p-value | `d_log_zori x renter_share_c` | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| year FE | 1.935 | 0.435 | 8.5e-06 | -2.556 | 6.086 | 0.674 | 1,090 |
| region x year FE | 1.137 | 0.495 | 0.022 | -5.738 | 6.266 | 0.360 | 1,090 |
| state x year FE | 1.789 | 0.716 | 0.012 | -2.186 | 8.987 | 0.808 | 1,090 |

The interaction is null in every tier, and the sign leans opposite the
hypothesis. Under the state x year FE coefficients, the implied rent-growth
elasticity is `1.870` at the 25th percentile of renter share (`0.318`),
`1.800` at the median (`0.350`), and `1.715` at the 75th percentile
(`0.389`) -- a small, non-significant decline rather than amplification.

Combined with the direct null above, this closes off renter share as a live
mechanism in the rent-growth-to-homelessness story in this dataset. It does
not independently predict within-MSA homelessness changes, and it does not
change how strongly rent growth predicts those changes either.
