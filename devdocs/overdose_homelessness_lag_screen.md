# Homelessness -> Overdose Deaths: Lag Screen (pooled top-150)

Date: 2026-07-07 (extended to total/sheltered margins same day)

## Question

Does an increase in homelessness predict an increase in drug overdose deaths
with a time lag, using the pooled top-50 + rank-51-150 MSA cohort (150 MSAs)
to get more power than the top-50 cohort alone offers? Run across all three
PIT margins (unsheltered, total, sheltered) for completeness, prompted by an
anecdotal observation (Boulder County, CO autopsy-report review, 2023: ~50%
of overdose decedents were homeless or formerly homeless in permanent
supportive housing).

**Measurement mismatch with the anecdote, stated up front**: HUD's PIT
taxonomy does not count Permanent Supportive Housing (PSH) residents as
homeless once housed -- PSH is tracked as permanent housing inventory (HIC),
not a PIT sheltered/unsheltered category. Neither margin in this panel can
see the "formerly homeless, now in PSH" population the anecdote describes.
`sheltered` here means emergency shelter + transitional housing + Safe Haven
on a single January night, not PSH tenure. This screen can only test whether
*current* PIT homelessness levels predict *subsequent* overdose deaths in the
same metro -- a different, weaker test than the anecdote's within-population
claim, and one that would silently understate any real relationship running
through the PSH-exit pathway.

## Data and Construction

- Base panels: `outputs/top50_msa_longitudinal_2010_2025.parquet` (50 MSAs)
  + `outputs/msa_rank51_150_replication/panel__msa_rank51_150__Y2015-2025@Mcensusmsa2023.parquet`
  (100 MSAs), pooled on the shared core columns. No `msa_id` overlap between
  cohorts (150 distinct MSAs total).
- Overdose: `data/curated/cdc/cdc_overdose__msa__Y2020-2025@Mcensusmsa2023xC2023.parquet`,
  already January-aligned (one 12-month-ending-in-January row per MSA-year).
  Joined on `msa_id`/`year`.
- Window: 2020, 2022, 2023, 2024, 2025 (2021 excluded on both sides -- PIT
  2021 is COVID-disrupted, and 2021 was also a national fentanyl-driven
  overdose outlier year unrelated to local homelessness dynamics).
- `overdose_per_1000 = overdose_deaths_12mo / population * 1000` using each
  panel's own PEP population, for consistency with `unshelt_per_1000` etc.
  elsewhere in this project.
- Quality filter: `overdose_coverage_ratio >= 0.8` (same threshold convention
  as ZORI). `aggregate_county_overdose_to_msa` now computes this ratio as the
  share of MSA county population with non-suppressed CDC data; the older raw
  county-count fraction is retained separately as `county_coverage_ratio`.
- Build script: `scripts/build_overdose_lag_panel.py` (tracked, reproducible).
  Panel/regression outputs under `outputs/overdose_lag/` (gitignored).

Levels panel: 750 MSA-years (150 x 5). Annual first-difference panel
(year_gap==1 only): 450 rows, but only 3 valid annual transitions exist in
this window (2022-23, 2023-24, 2024-25) since 2020-2022 is a 2-year gap.
Requiring a *lagged* first-difference term (needed for a strict FD lag design)
drops to 2 usable transitions (2023-24, 2024-25), because the 2022-23
transition would need an unavailable 2021-22 lag.

## Specs and Results

All models: entity+year FE for levels, year FE for first differences;
clustered SEs by `msa_id`; `overdose_coverage_ratio >= 0.8` applied. Run
identically across all three PIT margins (`unshelt`, `total`, `shelt`).

| Spec | Margin | Term | Estimate | SE | p | n |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| A: levels, contemporaneous | unsheltered | `log_unshelt_rate` | -0.003 | 0.046 | 0.953 | 455 |
| A: levels, contemporaneous | total | `log_total_rate` | +0.128 | 0.071 | 0.076 | 455 |
| A: levels, contemporaneous | sheltered | `log_shelt_rate` | +0.081 | 0.065 | 0.210 | 455 |
| B: levels, lag1 (mixed 1/2-yr gap) | unsheltered | `log_unshelt_rate_lag1` | -0.064 | 0.037 | 0.082 | 371 |
| B: levels, lag1 (mixed 1/2-yr gap) | total | `log_total_rate_lag1` | +0.041 | 0.069 | 0.559 | 372 |
| B: levels, lag1 (mixed 1/2-yr gap) | sheltered | `log_shelt_rate_lag1` | +0.126 | 0.064 | 0.053 | 372 |
| B1: levels, lag1 (strict 1-yr gap) | unsheltered | `log_unshelt_rate_lag1` | -0.122 | 0.056 | 0.033 | 175 |
| B1: levels, lag1 (strict 1-yr gap) | total | `log_total_rate_lag1` | -0.148 | 0.100 | 0.141 | 175 |
| B1: levels, lag1 (strict 1-yr gap) | sheltered | `log_shelt_rate_lag1` | +0.165 | 0.112 | 0.145 | 175 |
| C: FD, contemporaneous | unsheltered | `d_log_unshelt_rate` | +0.038 | 0.032 | 0.240 | 274 |
| C: FD, contemporaneous | total | `d_log_total_rate` | +0.070 | 0.061 | 0.251 | 274 |
| C: FD, contemporaneous | sheltered | `d_log_shelt_rate` | -0.046 | 0.055 | 0.402 | 274 |
| D: FD, with lag1 (lag term) | unsheltered | `d_log_unshelt_rate_lag1` | -0.071 | 0.038 | 0.068 | 175 |
| D: FD, with lag1 (lag term) | total | `d_log_total_rate_lag1` | -0.099 | 0.068 | 0.153 | 175 |
| D: FD, with lag1 (lag term) | sheltered | `d_log_shelt_rate_lag1` | -0.017 | 0.078 | 0.827 | 175 |

`log_zori` and FD-contemp coefficients for each spec/margin are in the full
result files; omitted here for brevity since the rent coefficient's pattern
(negative, sometimes marginal, flips sign in B1) is materially the same
across all three margins and discussed once below.

## Interpretation

**No margin, at any lag structure, supports "more homelessness -> more
overdose deaths" at conventional significance and with stability.** The
closest thing to a positive, hypothesis-consistent signal is **sheltered**
homelessness in the levels-lag1 spec (B: +0.126, p=0.053) -- directionally
positive and just short of significance, unlike unsheltered's negative
-0.064 (p=0.082) in the identical spec. But it doesn't hold up as evidence:
it weakens (not flips) to +0.165, p=0.145 in the strict 1-year-gap
subsample (B1) -- same direction, lost significance, small n=175. Total
tracks between the two (weak positive in levels-contemp, p=0.076; flips
negative under the strict-lag filter, p=0.141) -- the least stable of the
three, as expected for a sum of two series pointing different directions.
Unsheltered's B1 result (-0.122, p=0.033) is the only nominally-significant
coefficient across all 15 term/margin combinations, and it's negative --
opposite the hypothesis -- and itself unstable (see prior write-up: moves
from p=0.082 to p=0.033 just by dropping 99 rows with a 2-year instead of
1-year lag gap).

Reading all three margins together: there is a consistent *directional*
split -- sheltered leans positive across every levels spec (+0.081, +0.126,
+0.165), unsheltered leans negative across every levels-lag spec (-0.064,
-0.122) -- but none of it clears the bar of significance *and* stability
simultaneously. Given the measurement mismatch noted above (PSH residents
invisible to both margins), if the sheltered-margin lean is a faint echo of
the anecdote's mechanism (shelter/transitional-housing stays as a marker of
the same population later at PSH-overdose risk), this design is not built to
confirm or reject that -- it would need PSH stock (HIC has PSH bed counts)
or ideally person-level linkage, neither of which this panel has.

The unexpected negative, sometimes-significant `log_zori` coefficient in the
levels specs (rent *down* predicting overdose *up*, controlling for entity+year
FE) recurs identically across all three margins and is also not something to
over-read: with only 3-5 years of within-MSA variation and FE absorbing most
cross-sectional signal, this is likely soaking up residual national-timing
effects (e.g., 2020's COVID rent dip coinciding with elevated overdose deaths
nationally) rather than a real local economic relationship -- it flips sign
and loses significance in every strict-lag (B1) subsample.

**Biggest threat not addressed by this design**: national fentanyl/synthetic-
opioid supply shocks are a dominant, time-varying driver of overdose deaths
plausibly unrelated to local homelessness dynamics. Year FE absorb the
*average* national shock across all 3-5 years in the sample, but not
differential regional exposure to it (e.g., early vs. late fentanyl market
penetration by region) -- with this few years, that confound cannot be
separated from any true local relationship.

## Caveats

- CDC coverage_ratio is county-count-weighted, not population-weighted (see
  Data and Construction above) -- a real methodology gap worth fixing before
  leaning further on this filter.
- Pooling changes the cohort composition per spec (455/371/274/175 rows,
  97-113 of 150 MSAs) as different completeness requirements bind; results
  are not directly comparable across specs on identical samples.
- 2020 is a mixed COVID year for overdose deaths nationally even though PIT
  2020 predates the main COVID PIT disruption; not separately flagged here
  beyond the year FE.
- This is a screening pass, not a publishable causal estimate. Given the
  instability already found at n=175-274, extending further (top-150+
  cohort, more years as CDC's window ages) would be needed before drawing
  any real conclusion, and even then the fentanyl-shock confound would need
  an explicit control (e.g., a national or regional synthetic-opioid
  exposure proxy), not just year FE.

## Caveats (continued)

- PSH measurement gap (see Question section): this panel cannot see
  formerly-homeless PSH residents, the population the motivating anecdote is
  about. HIC PSH bed counts (already used elsewhere in this project, see
  the HIC bed-count confirmation in the main findings record) are the
  closest available stock proxy if this is revisited.

## HIC Bed-Category Contemporaneous Correlations (2026-07-07)

Follow-up: instead of the entity+year-FE lag specs above, compute simple
same-year Pearson correlations between each HIC bed *category* per capita and
overdose deaths per capita, across MSAs. This is a materially weaker design
than the specs above -- **no fixed effects, no lag, no rent control** -- so a
positive correlation here is consistent with (not evidence for) any causal
story, including the trivial one that bigger/denser metros have more of
everything. Included a population-partialled version as a partial check.

HIC beds by project type rolled up to MSA via `hhplab aggregate coc-measure`
per year, era-matched CoC boundaries (B2020 for 2020, B2024 for 2022-2025)
and county vintage 2023 (for CT planning-region coverage, matching the fix in
bead `coclab-2a508`):

```bash
for year in 2020 2022 2023 2024 2025; do
  BV=2020; [ "$year" != 2020 ] && BV=2024
  hhplab aggregate coc-measure \
    --source data/curated/hic/hic__H${year}.parquet \
    --columns hic_es_year_round_beds,hic_th_year_round_beds,hic_sh_year_round_beds,hic_rrh_year_round_beds,hic_psh_year_round_beds,hic_oph_year_round_beds,hic_total_beds \
    --geo-type msa --boundary-vintage $BV --counties 2023 \
    --definition-version census_msa_2023 \
    --output-dir outputs/overdose_lag/hic_by_category --json
done
```

Concatenated and merged into the levels panel by `build_overdose_lag_panel.py`
(`merge_hic_categories`); `coc_population_coverage_ratio` was >=0.86 for
every MSA-year (mean 0.9997), so no additional coverage filter was needed on
the HIC side. Correlations computed by
`scripts/overdose_hic_category_correlations.py`.

| Category | Pooled log-log r | p | Partial r (\|log_pop) | p | n |
| --- | ---: | ---: | ---: | ---: | ---: |
| Permanent Supportive Housing | **+0.386** | <0.0001 | **+0.434** | <0.0001 | 483 |
| All HIC beds | +0.364 | <0.0001 | +0.397 | <0.0001 | 483 |
| Emergency Shelter | +0.284 | <0.0001 | +0.310 | <0.0001 | 483 |
| Rapid Re-Housing | +0.278 | <0.0001 | +0.303 | <0.0001 | 483 |
| Transitional Housing | +0.231 | <0.0001 | +0.237 | <0.0001 | 479 |
| Other Permanent Housing | +0.134 | 0.0054 | +0.152 | 0.0017 | 428 |
| Safe Haven | +0.045 | 0.495 | +0.056 | 0.396 | 233 |

By year (log-log r; `*` = p<0.05):

| Category | 2020 | 2022 | 2023 | 2024 | 2025 |
| --- | ---: | ---: | ---: | ---: | ---: |
| Permanent Supportive Housing | +0.41* | +0.37* | +0.42* | +0.40* | +0.49* |
| All HIC beds | +0.38* | +0.35* | +0.38* | +0.40* | +0.45* |
| Rapid Re-Housing | +0.30* | +0.28* | +0.33* | +0.32* | +0.31* |
| Transitional Housing | +0.24* | +0.20* | +0.24* | +0.30* | +0.36* |
| Emergency Shelter | +0.23* | +0.25* | +0.31* | +0.33* | +0.39* |
| Other Permanent Housing | -0.01 | +0.06 | +0.11 | +0.20 | +0.18 |
| Safe Haven | +0.06 | -0.04 | -0.08 | -0.12 | -0.06 |

**PSH is the single strongest bed-category correlate of overdose deaths --
stronger than total beds, stronger than emergency shelter -- in every year,
and the correlation strengthens over time (+0.41 in 2020 to +0.49 in 2025).**
It survives partialling out log population (+0.386 -> +0.434, i.e. it is not
purely a "bigger cities have more of everything" artifact, at least not one
captured by population size alone). Safe Haven and Other Permanent Housing
are the only categories without a robust positive correlation.

This is the most direct piece of evidence in this whole screen consistent
with the motivating Boulder County anecdote (formerly-homeless PSH residents
as a large share of overdose decedents): metros with more PSH capacity per
capita have systematically higher overdose death rates, and PSH beats every
other bed category including plain shelter capacity at explaining that
variation. It is still only a same-year, no-FE, no-lag cross-sectional
correlation -- it cannot separate "PSH capacity causes/reflects overdose
risk" from "the same underlying local drug-market or poverty-concentration
conditions drive both PSH need and overdose deaths," and metros build PSH in
direct response to their homelessness population, so reverse causation
(more homelessness/addiction -> more PSH built -> both correlate with
overdose) is at least as plausible as any protective- or risk-conferring
story. The natural next step, not done here, would be running PSH beds
specifically through the same entity+year-FE lag design used for
unsheltered/total/sheltered above -- that would test whether *within-MSA
growth* in PSH capacity predicts *subsequent* overdose growth, a much
stronger test than this cross-sectional correlation.

## PSH Entity+Year-FE Lag Design (2026-07-07)

Ran PSH beds/capita through the identical 5-spec design used for
unsheltered/total/sheltered above (`log_psh_rate`, `d_log_psh_rate`, and
their lag-1 counterparts, added to `build_overdose_lag_panel.py`). This
removes between-MSA level differences (city size, baseline drug-market
conditions, homelessness-system scale) that the raw correlation above cannot
-- it isolates whether a metro's *own* PSH capacity moving up or down
predicts its *own* subsequent overdose deaths moving up or down.

| Spec | Term | Estimate | SE | p | n |
| --- | --- | ---: | ---: | ---: | ---: |
| A: levels, contemporaneous | `log_psh_rate` | +0.065 | 0.058 | 0.260 | 455 |
| B: levels, lag1 (mixed 1/2-yr gap) | `log_psh_rate_lag1` | +0.094 | 0.053 | 0.081 | 372 |
| B1: levels, lag1 (strict 1-yr gap) | `log_psh_rate_lag1` | +0.053 | 0.044 | 0.227 | 175 |
| C: FD, contemporaneous | `d_log_psh_rate` | -0.031 | 0.051 | 0.538 | 274 |
| D: FD, with lag1 (lag term) | `d_log_psh_rate_lag1` | +0.102 | 0.054 | 0.066 | 175 |
| D: FD, with lag1 (contemp term) | `d_log_psh_rate` | +0.024 | 0.051 | 0.641 | 175 |

**The signal shrinks a lot once fixed effects strip out between-MSA
differences, but doesn't fully disappear.** PSH's pooled cross-sectional
correlation was the strongest of any category (r=+0.386-0.434). Here, the
two lag-oriented specs (B, D) are directionally consistent and positive
(as hypothesized: more PSH now -> more overdose next year) at marginal
significance (p=0.066-0.081), while the two contemporaneous specs (A, C)
are null. That's a materially weaker result than the raw correlation
suggested -- most of what made PSH stand out cross-sectionally is between-MSA
variation (bigger PSH systems sit in bigger/denser metros with more overdose
deaths for other reasons), not a clean within-MSA lag relationship.

The lag signal is also **not robust to the same strict-gap check applied
throughout this doc**: restricting to observations with an exact 1-year lag
(B1) weakens it from p=0.081 to p=0.227 (b: +0.094 -> +0.053) -- the
opposite pattern from sheltered's B/B1 pair earlier in this doc, where the
strict-gap restriction *strengthened* the signal. That inconsistency (some
margins get stronger, PSH gets weaker, under the identical sample-restriction
choice) is itself evidence this whole family of estimates is dominated by
sampling noise at n=175-372, not a stable underlying relationship in any
direction.

**Bottom line on the anecdote**: the entity+year-FE test -- the right test
for "does more PSH capacity precede more overdose deaths in the same
metro" -- gives a directionally consistent but only marginally significant
and non-robust positive result. It neither confirms nor cleanly refutes the
mechanism your Boulder review suggests; it says the cross-sectional PSH-
overdose association is mostly a between-metro pattern, with at most a weak,
fragile within-metro echo of it. This is not a place to stop if the question
matters to you -- the measurement gap noted throughout this doc (PIT/HIC data
cannot see individual formerly-homeless decedents, only aggregate
metro-year capacity and death counts) means no aggregate panel design can
really adjudicate a claim that's fundamentally about individual histories.
Person-level or cohort linkage (e.g., matching PSH tenancy records to
coroner/ME data, which is what the Boulder autopsy review effectively did at
n=1 county) would be the only design that directly tests it.

## Artifacts

`outputs/overdose_lag/` (gitignored): `overdose_lag_levels.parquet`,
`overdose_lag_fd.parquet`, `spec_{a,b,b1,c,d}_{unshelt,total,shelt}*.parquet`
+ `{A,B,B1,C,D}_{unshelt,total,shelt}_*_result.{parquet,json}`,
`key_coefficients_by_margin.csv`, `hic_by_category/panel__msa-rollup-hic__*.parquet`
(per-year `aggregate coc-measure` outputs), `hic_by_category_pooled.parquet`,
`overdose_hic_by_category.parquet`, `hic_category_correlations_pooled.csv`,
`hic_category_correlations_by_year.csv`, `spec_{a,b,b1,c,d}_psh_*.parquet` +
`{A,B,B1,C,D}_psh_*_result.{parquet,json}`. Build scripts:
`scripts/build_overdose_lag_panel.py`,
`scripts/overdose_hic_category_correlations.py`.
