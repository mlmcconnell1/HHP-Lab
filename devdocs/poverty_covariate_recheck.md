# Poverty Rate vs. the Core Longitudinal Rent-Shock Finding

Date: 2026-07-08

## Question

Poverty share has never been tested as a covariate against the core
rent-shock -> unsheltered-growth elasticity (~1.6-1.9). Two things worth
knowing: does poverty change predict unsheltered growth on its own, and
does controlling for it change the rent coefficient (i.e., is the rent
effect actually a poverty proxy)?

## Data and Construction

- Poverty was never aggregated to MSA before this check. Registered in
  `ACS5_COVARIATE_REGISTRY` (table C17002: `poverty_universe`,
  `below_50pct_poverty`, `50_to_99pct_poverty` -> `population_below_poverty`,
  `poverty_rate`) but no existing panel carried it. Built via the registered
  path, `hhplab aggregate acs --target-geo msa --years 2009-2024 --weighting
  population`, which materialized 16 vintages
  (`data/curated/measures/measures__msa__A{2009..2024}@Mcensusmsa2023xT{2000,2010,2020}.parquet`,
  393 MSAs each, zero missing `msa_poverty_rate`). No new registry work
  needed -- this was sitting in the covariate registry, unlike the CDC/Vera
  gaps found earlier.
- ACS lag rule applied (standard project convention): an ACS vintage ending
  year E is the covariate for PIT year E+1.
- Merged onto the pooled top-50 + rank-51-150 cohort (150 MSAs, 2010-2025
  excl. 2021 for top-50's longer window, 2015-2025 excl. 2021 for
  rank-51-150). Build script: `scripts/build_poverty_longitudinal_panel.py`.

## Results

**FD (primary spec), n=1090, 137 MSAs, cluster by msa_id:**

| Spec | `d_log_zori` (rent) | `d_poverty_rate` |
| --- | --- | --- |
| Base (no poverty) | b=+1.921, p<0.0001 | -- |
| + poverty, plain year FE | b=+1.940, p<0.0001 | b=+0.615, p=0.819 |
| + poverty, state x year FE | b=+1.809, p=0.0115 | b=+0.500, p=0.887 |

**Levels FE (secondary spec, already known to be coverage-sensitive per
`devdocs/msa_rank51_150_replication.md`), n=1364:**

| Spec | `log_zori` (rent) | `poverty_rate` |
| --- | --- | --- |
| Base (no poverty) | b=+0.936, p=0.034 | -- |
| + poverty | b=+0.874, p=0.068 | b=-1.022, p=0.734 |

## Interpretation

**Poverty rate change is not an independent predictor of unsheltered growth
in this design, and it does not explain away the rent elasticity.** In the
FD spec -- the primary, robust design this project leans on -- the rent
coefficient is essentially untouched by adding poverty (+1.921 -> +1.940,
a rounding-level difference) and stays significant even under state x year
FE. Poverty's own coefficient is nowhere near significant in any spec
(p=0.73-0.89), with a large, imprecise SE relative to its point estimate.

The levels-FE version shows a small wobble (rent p=0.034 -> 0.068, right at
the edge) when poverty is added, but this is consistent with levels-FE's
already-documented fragility to sample/covariate composition (the
rank-51-150 replication doc found this same spec type flips between
significant and null depending on the ZORI coverage filter alone, with no
poverty involved) -- not evidence of a poverty confound, especially since
poverty's own coefficient here is wildly imprecise (b=-1.02, p=0.73) rather
than a clean, competing effect.

**Verdict: put to bed.** Poverty doesn't do anything here -- not as a
competitor to rent, not as an omitted variable inflating the rent
coefficient. The FD elasticity, which is where this project's causal
argument actually rests (per the timing-asymmetry logic in the main
record), is unaffected.

## Artifacts

`outputs/poverty_longitudinal/` (gitignored):
`poverty_longitudinal_{levels,fd}.parquet`. `data/curated/measures/` now
also has 16 MSA-level ACS measure vintages (2009-2024, tracked curated data,
reusable for any other ACS5-derived covariate work, not just poverty). Build
script: `scripts/build_poverty_longitudinal_panel.py`.
