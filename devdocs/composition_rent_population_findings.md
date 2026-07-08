# Compositional Effects on Rent Independent of Population

## Overall Result

The three household-composition screens do not support a positive
composition-driven explanation for rent growth independent of population
growth. Renter tenure share is negatively associated with rent growth under
plain year FE and under pooled MSA+year levels FE, but the first-difference
rent result does not survive a state x year FE check (see addenda below) and
the unsheltered-rate result is null either way; renter household size is null;
and recent-mover income ratios are null. None of the three compositional
channels tested here support a positive composition-driven explanation for
the earlier finding that population growth and rent growth are nearly
orthogonal across the pooled top-150 MSA design -- whatever is driving rent up
independent of headcount, it is not simply "more renters,"
"smaller/larger renter households," or "richer people moving in."

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

**2026-07-08 addendum: the one significant result here (`d_renter_household_share`
on rent, p=0.026) does not survive a state x year FE check.** Following the
same state-level-confound audit already applied to several other findings
this project has made (see `devdocs/state_level_robustness_rechecks.md` and
the Vera jail work), re-ran this spec with `primary_state x year` fixed
effects instead of plain year FE, on the identical n=1096 sample (137 MSAs,
29 of 43 states with 2+ MSAs -- well-identified). The coefficient shrinks by
more than half (-0.401 -> -0.177) and loses significance entirely
(p=0.026 -> 0.415). This is now the **third** time in this project's record
that a plain-entity+year-FE result evaporates under state x year FE (after
the jail-vs-unsheltered flip and the migration-churn non-replication) --
strong enough repetition to treat state x year FE as a standard, not
optional, check for this class of MSA panel, not a check that only
sometimes matters. Net effect: renter tenure share does not survive as a
first-difference rent-growth predictor once state x year confounding is
absorbed, although the later tracked levels-FE check below finds a strong
negative pooled levels association.

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
   uv run python scripts/analyze_composition_rent_population_robustness.py
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

## 2026-07-08 Addendum: renter tenure share's levels result survives state x year FE

The corrected `renter_household_share` levels-FE result above (b=-2.018,
p=5.97e-06, msa+year FE) had not yet been run through this project's
strictest check: replacing plain year FE with `primary_state x year` FE,
the test that has caught every other significant MSA-panel result in this
project's record except housing supply constraint (see
`devdocs/noncompositional_rent_population_findings.md`). Added
`rent_levels_renter_household_share_msa_state_year_fe` to
`scripts/analyze_composition_rent_population_robustness.py`
(`fixed_effects=("msa_id", "primary_state_year")`) and re-ran:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `renter_household_share` (msa + year FE) | -2.018 | 0.446 | 5.97e-06 | 1,370 |
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
