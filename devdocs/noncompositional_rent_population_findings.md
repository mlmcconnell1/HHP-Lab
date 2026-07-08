# Non-Compositional Rent/Population Mechanism Screens

Generated with:

```bash
uv run python scripts/build_noncompositional_rent_population_panel.py
uv run python scripts/analyze_noncompositional_rent_population_robustness.py
```

This screen follows the pooled top-50 plus rank-51-150 MSA first-difference
design used in `devdocs/composition_rent_population_findings.md`, excluding
2021. It tests non-compositional mechanisms raised after the household
composition screens failed to explain the population/rent orthogonality.

Generated, ignored artifacts:

- `outputs/noncompositional_rent_population/noncompositional_rent_population_levels.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd_regressions.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_fd_regressions.csv`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_summary.json`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_robustness_regressions.parquet`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_robustness_regressions.csv`
- `outputs/noncompositional_rent_population/noncompositional_rent_population_robustness_summary.json`

## Measure Discovery

- Housing supply constraints are supported by already curated
  `census_bps` and Saiz covariates. The direct screen below uses
  pre-sample permit scarcity from the Census Building Permits Survey.
- A true short-term-rental source is not registered. The plausible free ACS
  fallback, B25004 vacancy status with the seasonal/recreational/occasional
  category, is now included in the refreshed ACS1 metro artifacts and is
  tested below as a vacation-home/STR-adjacent proxy.
- ACS work-from-home commute table B08301 now has registry support and is
  available in the refreshed ACS1 metro artifacts. It is tested below as the
  direct remote-work demand proxy. ACS1 B25068 bedroom mix among gross-rent
  units remains as a weaker fallback proxy for rental-space demand.

## Coverage

- Levels rows: 1,750
- First-difference rows with 1-year gaps: 1,450
- MSAs: 150
- Analysis years: 2010-2020 and 2022-2025
- ACS1 vintages used for bedroom-mix proxy: 2009-2019 and 2021-2024
- Complete supply-constraint FD rows: 1,096
- Complete short-term-rental proxy FD rows: 1,070
- Complete remote-work proxy FD rows: 1,046
- Complete space-demand-proxy FD rows: 1,074

Median level bedroom mix among ACS1 gross-rent units:

| Measure | Median |
| --- | ---: |
| 2+ bedroom share | 0.717 |
| 3+ bedroom share | 0.325 |

Median level ACS1 B25004 seasonal/recreational/occasional vacancy share:

| Measure | Median |
| --- | ---: |
| Seasonal/recreational vacancy share | 0.151 |

Median level ACS1 B08301 work-from-home share:

| Measure | Median |
| --- | ---: |
| Work-from-home share | 0.057 |

## Housing Supply Constraints

The supply screen asks whether constrained metros have higher rent growth net
of population growth, and whether population changes translate differently
into rent changes under tighter supply. Models use MSA-clustered standard
errors and year fixed effects:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `supply_constraint_bps` | 0.0055 | 0.0009 | <0.001 | 1,096 |
| `d_log_pop x supply_constraint_bps` | 0.1145 | 0.0873 | 0.189 | 1,096 |
| `supply_constraint_bps_long` | 0.0034 | 0.0012 | 0.003 | 1,096 |
| `d_log_pop x supply_constraint_bps_long` | 0.1027 | 0.0832 | 0.217 | 1,096 |

Interpretation: constrained metros do show higher rent growth net of
population growth and common year shocks. The interaction term is positive
but imprecise, so this run does not show that population growth itself has a
statistically different rent slope in constrained metros. The evidence is
therefore more consistent with a supply-constraint level channel -- persistent
rent pressure in constrained places independent of headcount changes -- than
with a sharply different population/rent elasticity by constraint level.

## Short-Term/Vacation-Rental Proxy

No commercial STR-platform source is registered in the project. The free proxy
tested here is the ACS1 B25004 share of vacant housing units marked for
seasonal, recreational, or occasional use. This is not a direct Airbnb/VRBO
measure, but it is the closest currently supported public proxy for housing
stock being held outside regular long-term occupancy.

The pooled top-150 first-difference model asks whether growth in that share
predicts rent growth net of population growth and common year shocks, with
MSA-clustered standard errors:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_seasonal_recreational_vacancy_share` | 0.0305 | 0.0140 | 0.029 | 1,070 |

Interpretation: metros with rising seasonal/recreational/occasional vacancy
share have higher ZORI rent growth net of population growth in this screen.
The magnitude is moderate because the proxy is a share change, but the sign
matches the proposed mechanism: more stock held for seasonal or occasional use
is associated with rent growth not explained by registered population growth.
This should be treated as proxy evidence, not as a direct STR-platform result.

## Space-Demand Proxy

The direct remote-work proxy is ACS1 B08301 work-from-home share. The
secondary fallback proxy remains ACS1 B25068 bedroom mix among gross-rent
units, which can detect shifts toward larger rental units but is not a direct
remote-work measure. Models use MSA-clustered standard errors and year fixed
effects:

| Model | Proxy term | Estimate | SE | p-value | N |
| --- | --- | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + proxy + year FE` | `d_work_from_home_share` | 0.0506 | 0.0648 | 0.435 | 1,046 |

Interpretation: the direct B08301 work-from-home proxy does not explain rent
growth net of population growth in this pooled FD screen. The coefficient is
positive, but it is imprecise and statistically null.

The bedroom-mix fallback results remain:

| Model | Proxy term | Estimate | SE | p-value | N |
| --- | --- | ---: | ---: | ---: | ---: |
| `d_log_zori ~ d_log_pop + proxy + year FE` | `d_gross_rent_2plus_bedroom_share` | -0.0320 | 0.0231 | 0.165 | 1,080 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + proxy + year FE` | `d_gross_rent_2plus_bedroom_share` | 1.1950 | 0.5471 | 0.029 | 1,074 |
| `d_log_zori ~ d_log_pop + proxy + year FE` | `d_gross_rent_3plus_bedroom_share` | -0.0639 | 0.0199 | 0.001 | 1,080 |
| `d_log_unshelt_rate ~ d_log_zori + d_log_pop + proxy + year FE` | `d_gross_rent_3plus_bedroom_share` | -0.0726 | 0.5730 | 0.899 | 1,074 |

Interpretation: the fallback also does not support a positive
rental-space-demand explanation for rent rising independent of population.
The rent coefficients are negative, and the 3+ bedroom share coefficient is
statistically significant in the opposite direction. The positive 2+
bedroom-share coefficient in the unsheltered-rate model is exploratory and
does not pair with higher rent growth, so it is not evidence for the proposed
rent channel.

## Interpretation

Among the non-compositional candidates tested here, supply constraints remain
the strongest mechanism: constrained MSAs have higher rent growth even after
controlling for population growth. The refreshed ACS1 B25004
seasonal/recreational vacancy proxy now also supports the vacation-home/STR
channel directionally and statistically in the pooled FD design. Together
these results directly weaken the idea that population changes alone should
explain rent changes.

The remote-work/space-demand channel is not supported in either its direct
B08301 work-from-home form or the weaker B25068 bedroom-mix fallback.

## 2026-07-08 Code Review Addendum

Requested review of correctness, completeness, corner cases, and test
coverage. Independently reran the supply-constraint and space-demand
regressions and reproduced every reported coefficient exactly. All new
registry cell orders (B08301, B25004, and the pre-existing B25068 used for
the bedroom-mix fallback) were re-verified byte-for-byte against the live
Census API and are correct; the bedroom-mix "2+/3+ share" denominator
(`gross_rent_bedrooms_total`) correctly includes the no-bedroom/studio
category even though that category isn't separately loaded, so the shares
themselves are computed correctly. No correctness bugs found in the tested
code paths.

**One result is genuinely new and worth highlighting: `supply_constraint_bps`
is the first result in this whole rent/population investigation (both this
epic and the sibling compositional one) to survive a state x year FE
check**, the robustness test that has caught three other significant
MSA-panel results in this project's record. Re-ran with `primary_state x
year` fixed effects instead of plain year FE on the identical n=1096 sample:
the coefficient attenuates by about half (+0.0055 -> +0.0029) but stays
significant (p<0.0001 -> p=0.012). Housing supply constraint is the one
candidate mechanism, across both epics, that holds up under the strictest
check available.

**One corner case worth flagging, not currently causing wrong numbers but a
latent risk**: `supply_constraint_bps`/`supply_constraint_bps_long` are
static, pre-sample exposures built from *permits in 2010-2014* (and
2000-2014 for the long version) -- by design, meant to strictly precede the
analysis window they instrument for. But `build_levels_panel()` merges this
onto `load_pooled_base_panel()`'s output, which (for the top-50 cohort)
spans years back to **2010**, i.e. the exposure-construction window itself.
This script contains no comment, assertion, or year filter documenting or
enforcing that the exposure must precede the analysis sample -- unlike the
original supply-IV work (`scripts/build_supply_iv_panel.py`), which was
deliberately scoped to a 2015-2025 analysis window specifically to avoid
this overlap. In practice this does not corrupt the current result: `d_log_zori`
(ZORI) itself has no data before 2015, so the complete-case regression
sample already starts at 2016 regardless (verified: identical n and
coefficients whether or not years before 2016 are explicitly excluded).
**This is correct by data-availability coincidence, not by design** -- if
this script is ever pointed at a rent measure with 2010-2014 coverage (e.g.
ACS1 stock rent, which does have it), the pre-sample-exposure contamination
would silently reappear. Filed as
`coclab-noncompositional-rent-population-epic-t6lo8.4` (P3).

**Test coverage gap**: unlike the sibling composition-panel scripts (which
got full `build_levels_panel()` integration tests with monkeypatched globs),
this script's tests (`tests/test_build_noncompositional_rent_population_panel.py`)
only cover the pure-function helpers (`add_space_demand_columns`,
`add_first_differences`, `_model_specs`) -- the merge orchestration itself
(`build_levels_panel`, `load_acs1_space_demand_panel`, and the `msa_id`-only
broadcast merge of the static supply exposure) is untested. Filed as
`coclab-noncompositional-rent-population-epic-t6lo8.5` (P3).

**STR (bead .2) was subsequently completed after reingesting the ACS1 metro
artifacts with B25004**: the seasonal/recreational/occasional vacancy-share
proxy is now loaded into the non-compositional panel and tested directly
against rent growth net of population growth. **Remote-work (bead .3) was
subsequently completed after the same ACS1 refresh with B08301**: the
work-from-home share proxy is now loaded into the non-compositional panel and
tested directly against rent growth net of population growth, with a null
result.

## 2026-07-08 Addendum: STR proxy does not survive state x year FE

The STR (seasonal/recreational vacancy share) FD result above (b=+0.0305,
p=0.029, plain year FE) had never been re-run with `primary_state x year`
FE, the check that has already caught renter household share (FD) and every
other significant plain-FE MSA-panel result in this project except supply
constraint and, as of the addendum above, renter household share in levels.
Added `scripts/analyze_noncompositional_rent_population_robustness.py`
(mirrors `analyze_composition_rent_population_robustness.py`'s pattern:
`fixed_effects=("primary_state_year",)`) and re-ran on the identical n=1,070
sample:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `d_seasonal_recreational_vacancy_share` (year FE) | 0.0305 | 0.0140 | 0.029 | 1,070 |
| `d_seasonal_recreational_vacancy_share` (state x year FE) | 0.0157 | 0.0116 | 0.176 | 1,070 |

**Does not survive.** The coefficient roughly halves and loses significance
entirely. Independently re-verified with a from-scratch replication
(separate design-matrix construction, same input parquet) -- matches
exactly. Net effect: the STR/vacation-rental proxy joins renter household
size, recent-mover income, and remote-work as a non-surviving or null
candidate. **Housing supply constraint remains the only FD (growth-on-growth)
result, and renter household share in levels the only cross-sectional
result, to survive a state x year FE check across both epics.**

## 2026-07-08 Addendum: supply constraint robustness is now tracked

Closed the reproducibility gap noted above by adding the headline supply
constraint state x year FE check to
`scripts/analyze_noncompositional_rent_population_robustness.py`. The tracked
spec matches the primary year-FE supply model: `d_log_zori ~ d_log_pop +
supply_constraint_bps + d_log_pop_x_supply_constraint_bps`, replacing year FE
with `primary_state x year` FE and preserving MSA-clustered standard errors.
The script now also enforces that supply-constraint complete-case samples do
not overlap the 2010-2014 BPS exposure window.

Tracked output:

| Model term | Estimate | SE | p-value | N |
| --- | ---: | ---: | ---: | ---: |
| `supply_constraint_bps` (year FE) | 0.0055 | 0.0009 | <0.001 | 1,096 |
| `supply_constraint_bps` (state x year FE) | 0.0029 | 0.0012 | 0.012 | 1,096 |
| `d_log_pop x supply_constraint_bps` (state x year FE) | 0.1308 | 0.0712 | 0.066 | 1,096 |

Independently re-verified from scratch with a separate statsmodels formula
construction on the same input parquet:
`supply_constraint_bps` b=0.002912331262, SE=0.001161385601,
p=0.012154211949, n=1,096, 137 MSA clusters, analysis years 2016-2025.
This confirms the earlier prose claim while making the check re-runnable.
