# Unsheltered Homelessness -> Overdose Deaths: Lag Screen (pooled top-150)

Date: 2026-07-07

## Question

Does an increase in unsheltered homelessness predict an increase in drug
overdose deaths with a time lag, using the pooled top-50 + rank-51-150 MSA
cohort (150 MSAs) to get more power than the top-50 cohort alone offers?

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
clustered SEs by `msa_id`; `overdose_coverage_ratio >= 0.8` applied.

| Spec | Term | Estimate | SE | p | n | MSAs |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| A: levels, contemporaneous | `log_unshelt_rate` | -0.003 | 0.046 | 0.953 | 455 | 113 |
| A: levels, contemporaneous | `log_zori` | -0.758 | 0.275 | 0.007 | 455 | 113 |
| B: levels, unsheltered lag1 (mixed 1/2-yr gap) | `log_unshelt_rate_lag1` | -0.064 | 0.037 | 0.082 | 371 | 113 |
| B: levels, unsheltered lag1 (mixed 1/2-yr gap) | `log_zori` | -0.975 | 0.499 | 0.053 | 371 | 113 |
| B1: levels, unsheltered lag1 (strict 1-yr gap only) | `log_unshelt_rate_lag1` | -0.122 | 0.056 | 0.033 | 175 | 97 |
| B1: levels, unsheltered lag1 (strict 1-yr gap only) | `log_zori` | +0.445 | 0.944 | 0.638 | 175 | 97 |
| C: FD, contemporaneous | `d_log_unshelt_rate` | +0.038 | 0.032 | 0.240 | 274 | 104 |
| C: FD, contemporaneous | `d_log_zori` | -0.673 | 0.435 | 0.125 | 274 | 104 |
| D: FD, with lag1 | `d_log_unshelt_rate_lag1` | -0.071 | 0.038 | 0.068 | 175 | 97 |
| D: FD, with lag1 | `d_log_unshelt_rate` (contemp) | +0.041 | 0.040 | 0.306 | 175 | 97 |
| D: FD, with lag1 | `d_log_zori` | -0.747 | 0.667 | 0.265 | 175 | 97 |

## Interpretation

**No evidence supports the hypothesized direction (more unsheltered
homelessness -> more overdose deaths with a lag).** Every spec's lagged or
contemporaneous unsheltered term is either indistinguishable from zero
(spec A, C) or negative (spec B, B1, D) -- the opposite sign from the
hypothesis. None of this should be read as "homelessness reduces overdose
deaths" either: the one place the negative coefficient reaches conventional
significance (B1, p=0.033) is **not stable** -- it moves from -0.064
(p=0.082) to -0.122 (p=0.033) simply by dropping the 99 rows that used a
2-year instead of 1-year lag gap, a sample-definition choice that shouldn't
matter for a real effect. That instability, combined with n=175-274 and only
2-3 usable annual transitions total, reads as underpowered/noisy rather than
a genuine relationship in either direction.

The unexpected negative, sometimes-significant `log_zori` coefficient in the
levels specs (rent *down* predicting overdose *up*, controlling for entity+year
FE) is also not something to over-read: with only 3-5 years of within-MSA
variation and FE absorbing most cross-sectional signal, this is likely
soaking up residual national-timing effects (e.g., 2020's COVID rent dip
coinciding with elevated overdose deaths nationally) rather than a real local
economic relationship -- it flips sign and loses significance in the
strict-lag subsample (B1).

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

## Artifacts

`outputs/overdose_lag/` (gitignored): `overdose_lag_levels.parquet`,
`overdose_lag_fd.parquet`, `spec_{a,b,b1,c,d}_*.parquet` +
`spec_{a,b,c,d}_*_result.{parquet,json}`. Build script:
`scripts/build_overdose_lag_panel.py`.
