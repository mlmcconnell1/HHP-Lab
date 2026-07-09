# QCEW Labor-Market Findings

Updated: 2026-07-09

Workflow:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab build result qcew-labor-market --json
```

Tracked output files:

- `outputs/qcew_labor_market/qcew_labor_market_levels.parquet`
- `outputs/qcew_labor_market/qcew_labor_market_fd.parquet`
- `outputs/qcew_labor_market/qcew_labor_market_regressions.parquet`
- `outputs/qcew_labor_market/qcew_labor_market_summary.json`

Resolves `coclab-mzpm7.15`, the screening follow-up to `coclab-mzpm7.9`'s
QCEW channel assessment (`devdocs/qcew_wage_employment_channel_assessment.md`).

## Data and coverage

BLS's per-area annual "Total, all industries" file
(`https://data.bls.gov/cew/data/api/<year>/a/industry/10.csv`) only serves
2014 onward for this ingest path (2010-2013 return 404 from BLS). The pooled
top-150 base panel (top50 2010-2025, rank51-150 2015-2025, both excluding
2021) is therefore truncated to 2014-2025 for this workflow -- an
availability ceiling, not a build failure. County rows are filtered to
Total Covered ownership (`own_code=0`) and total all-industries
(`industry_code=10`, `agglvl_code=70`), matching the assessment doc's
recommended raw contract.

MSA rollup used the shared `aggregate_county_covariate_to_msa` path with the
existing `coverage_ratio` guard (min 1.0, warn-not-drop by default): 11 of
1550 MSA-year rows had incomplete member-county coverage and were excluded
from the regression sample rather than treated as fully covered. This is the
same guard verified in `coclab-7121v`.

CPI-U (`data/curated/cpi/cpi_u__Aall.parquet`) was extended back to include
2014 (previously 2015-2025) to support real-wage deflation across the full
QCEW window.

## An identity worth flagging: nominal and real wage growth are indistinguishable here

CPI-U is a single national annual scalar with no cross-MSA variation. Every
spec in this project's standard robustness ladder (year FE, state x year FE,
region x year FE) already includes year as part of the fixed effects, which
absorbs any variable that is constant across MSAs within a year -- including
the deflator. So `d_log_wage_real = d_log_wage_nominal - d_log_cpi_u` produces
an **identical** wage-growth coefficient to the nominal version under any of
these specs (verified numerically: estimates and standard errors matched to
6 decimal places). The nominal/real comparison the assessment doc proposed as
a follow-up is therefore not identifiable in this design and was dropped from
the tracked regression set rather than shipped as a redundant, misleading
pair of rows. Real wage growth is kept as the headline spec since it is the
economically meaningful quantity; nominal growth is only useful as a
cross-check in a design *without* any year-level fixed effect, which this
project does not otherwise run.

## Results (150 MSAs pooled, 2014-2025, complete-coverage sample, n=1096)

### Rent growth (`d_log_zori`)

| term | year FE | state x year FE | region x year FE |
|---|---:|---:|---:|
| `d_log_qcew_annual_avg_emplvl` | `+0.330` (`p=5.7e-09`) | `+0.213` (`p=3.8e-04`) | `+0.335` (`p=2.2e-06`) |
| `d_log_qcew_total_annual_wages_real` | `+0.200` (`p=9.9e-06`) | `+0.046` (`p=0.424`) | `+0.170` (`p=0.0028`) |
| `d_log_qcew_annual_avg_weekly_wage_real` | `+0.041` (`p=0.476`) | `-0.082` (`p=0.275`) | `+0.005` (`p=0.940`) |

**QCEW employment growth survives the full 3-tier ladder.** This is a
different pattern from most channels tested in this investigation (STR proxy,
renter-share FD, subsidized-housing stock, eviction rate all decayed to null
under state x year FE): the coefficient stays positive and significant at
every fixed-effect tier, including state x year FE, which is this project's
strictest check for state-level policy confounds. A 1% increase in local
covered employment is associated with roughly 0.21-0.34 percentage points of
additional same-year rent growth net of population growth and state-year
shocks.

Total real wage-bill growth shows the now-familiar decay: significant under
year FE and region x year FE, but not state x year FE (p=0.42) -- consistent
with a state-level confound rather than a genuine local wage-bidding channel
once state policy/economic shocks are absorbed.

Average weekly (per-worker) real wage growth is null throughout. Employment
growth, not wage-per-worker growth, is carrying whatever signal is here --
consistent with a headcount/labor-demand story (more jobs pulling in more
renters) rather than a wage-bargaining-power story.

### Unsheltered rate growth (`d_log_unshelt_rate`, controlling for `d_log_zori` and `d_log_pop`)

All nine QCEW-family unsheltered models are null (p=0.09-0.94; smallest p is
`d_log_qcew_annual_avg_weekly_wage_real` at year FE, p=0.092, which does not
hold up at either state x year or region x year FE). No evidence that QCEW
labor-market growth carries information about unsheltered-rate changes beyond
what rent and population growth already capture.

## Bottom line

Local employment growth is a real, state-year-robust rent-growth channel in
this data -- one of the few channels in this investigation to survive the
full fixed-effect ladder. Wage growth (real total wage bill, real per-worker
weekly wage) does not add a state-year-robust channel beyond employment and
population growth. The nominal-vs-real wage comparison proposed in the
assessment doc turned out to be mechanically non-identifiable given this
project's fixed-effect conventions, which is itself a useful methodological
note for any future covariate that, like CPI-U, varies only by year and not
by MSA.
