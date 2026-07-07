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
  as ZORI). **Caveat: this ratio is the fraction of an MSA's *counties* with
  non-suppressed CDC data, not population-weighted** (see
  `aggregate_county_overdose_to_msa` in `hhplab/cdc/overdose.py`) -- unlike
  the population-weighted coverage ratios used elsewhere (ZORI, IRS SOI). A
  large multi-county metro with one small suppressed suburb and a two-county
  metro with one suppressed half its population both move the ratio by a
  similar amount. Treat the filter as a coarse quality screen, not a precise
  population-coverage guarantee.
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

## Artifacts

`outputs/overdose_lag/` (gitignored): `overdose_lag_levels.parquet`,
`overdose_lag_fd.parquet`, `spec_{a,b,b1,c,d}_{unshelt,total,shelt}*.parquet`
+ `{A,B,B1,C,D}_{unshelt,total,shelt}_*_result.{parquet,json}`,
`key_coefficients_by_margin.csv`. Build script:
`scripts/build_overdose_lag_panel.py`.
