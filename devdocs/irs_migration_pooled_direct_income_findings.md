# IRS Migration Pooled Direct-Income Findings

Updated: 2026-07-09

Workflow:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab build result irs-migration-pooled --json
```

Tracked output files:

- `outputs/irs_migration_pooled/irs_migration_pooled_levels.parquet`
- `outputs/irs_migration_pooled/irs_migration_pooled_fd.parquet`
- `outputs/irs_migration_pooled/irs_migration_pooled_regressions.parquet`
- `outputs/irs_migration_pooled/irs_migration_pooled_summary.json`

## Question

This workflow revisits the IRS SOI migration screen on the pooled top-150 MSA
sample, but asks a different question from the older churn-interaction check:

Do direct migrant-income measures such as inflow AGI per return or outflow AGI
per return predict later rent growth?

IRS flow year `Y` is aligned to PIT/ZORI year `Y + 1`, so the direct income
predictors in year `t` are already prior-year migration exposures relative to
the rent-growth outcome `d_log_zori_t`.

## Measures

All AGI-per-return measures are in thousands of dollars per return:

- `inflow_agi_per_return_k = inflow_agi_thousands / inflow_returns`
- `outflow_agi_per_return_k = outflow_agi_thousands / outflow_returns`
- `churn_agi_per_return_k = (inflow_agi_thousands + outflow_agi_thousands) / (inflow_returns + outflow_returns)`
- `inflow_outflow_agi_gap_k = inflow_agi_per_return_k - outflow_agi_per_return_k`

Primary regression family:

`d_log_zori_t ~ d_log_pop_t + migrant_income_measure_t + FE`

with clustered standard errors at `msa_id` and three FE variants:

- year FE
- region x year FE
- state x year FE

The workflow also includes a joint specification:

`d_log_zori_t ~ d_log_pop_t + inflow_agi_per_return_k_t + outflow_agi_per_return_k_t + FE`

## Sample

- 150 MSAs
- 1,350 aligned level rows
- 1,050 first-difference rows with `year_gap == 1`
- 959 complete annual-gap rows for the direct-income rent screens

## Single-predictor results

The inflow-only screen is null. The strongest single-predictor result is
outflow AGI per return: higher-income out-migration is associated with slower
subsequent rent growth, and that relationship strengthens under the tighter FE
specifications.

| term | year FE | region x year FE | state x year FE |
|---|---:|---:|---:|
| `inflow_agi_per_return_k` | `-0.000004` (`p=0.888`) | `-0.000002` (`p=0.943`) | `-0.000038` (`p=0.264`) |
| `outflow_agi_per_return_k` | `-0.000069` (`p=0.068`) | `-0.000123` (`p=0.00056`) | `-0.000143` (`p=0.000073`) |
| `churn_agi_per_return_k` | `-0.000030` (`p=0.396`) | `-0.000048` (`p=0.249`) | `-0.000083` (`p=0.0658`) |
| `inflow_outflow_agi_gap_k` | `+0.000059` (`p=0.146`) | `+0.000110` (`p=0.0194`) | `+0.000065` (`p=0.204`) |

Scale: in the state-year FE screen, a `$10k` increase in outflow AGI per return
corresponds to about `0.00143` lower log rent growth, or roughly `0.14`
percentage points less rent growth. This is statistically clear but
economically modest.

## Joint inflow/outflow specification

When inflow and outflow AGI per return enter together, the signs separate in the
expected direction: richer inflows are associated with faster later rent growth,
and richer outflows with slower later rent growth.

| term | year FE | region x year FE | state x year FE |
|---|---:|---:|---:|
| `inflow_agi_per_return_k` | `+0.000053` (`p=0.128`) | `+0.000101` (`p=0.00330`) | `+0.000063` (`p=0.0405`) |
| `outflow_agi_per_return_k` | `-0.000117` (`p=0.0142`) | `-0.000216` (`p=0.0000020`) | `-0.000205` (`p=0.00000052`) |

This joint model is more informative than the one-variable income-gap screen:
the gap measure is directionally positive but not state-year robust on its own,
while the separate inflow/outflow coefficients are.

## Robustness note

The state-year outflow result is not a single-row artifact:

- Dropping the lone negative `outflow_agi_per_return_k` row leaves the estimate
  essentially unchanged (`-0.000144`, `p=0.000080`, `n=958`).
- Trimming the 1st and 99th percentiles strengthens rather than weakens it
  (`-0.000196`, `p=0.000112`, `n=939`).

These were quick follow-up checks run against the built FD panel, not separate
tracked workflow artifacts.

## Bottom line

The pooled top-150 IRS migration screen does not support a broad "migrant income
in general explains rent growth" story. Inflow AGI per return alone is null,
and the average churn-income measure is weak. But the outflow side is not dead:
higher-income out-migration is consistently associated with slower later rent
growth, and in joint models the pattern looks like richer inflows plus poorer
outflows predicting stronger subsequent rent growth.

That is more signal than the earlier churn-interaction bead found, but it is
still a modest descriptive channel, not a dominant explanation for the larger
unexplained rent-growth variance.
