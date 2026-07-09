# Housing Cost Burden Composition Findings

Updated: 2026-07-09

Workflow:

```bash
HHPLAB_NON_INTERACTIVE=1 uv run hhplab build result housing-cost-burden-composition --json
```

Tracked output files:

- `outputs/composition_rent_population/housing_cost_burden_composition_levels.parquet`
- `outputs/composition_rent_population/housing_cost_burden_composition_fd.parquet`
- `outputs/composition_rent_population/housing_cost_burden_composition_fd_regressions.parquet`
- `outputs/composition_rent_population/housing_cost_burden_composition_summary.json`

## What the workflow now tests

The primary modeled question is timing-aware:

`d_log_zori_t ~ d_log_pop_t + burden_{t-1}`

for ACS5 and ACS1 affordability measures aligned with the project's standard ACS
lag rule. The workflow also keeps same-year first-difference screens as
secondary checks:

`d_log_zori_t ~ d_log_pop_t + d_burden_t`

and the companion unsheltered screen:

`d_log_unshelt_rate_t ~ d_log_zori_t + d_log_pop_t + d_burden_t`

## Primary lagged-level results

ACS5 measures remain weak-to-moderate under year and region-year fixed effects,
but the headline "pre-existing affordability stress predicts later rent growth"
story does not survive the stricter state-year comparison. The signs are also
positive, not the negative same-year pattern seen in the contemporaneous screen.

| term | year FE | region x year FE | state x year FE |
|---|---:|---:|---:|
| `lag(acs5_rent_burden_30_plus)` | `+0.038` (`p=0.052`) | `+0.047` (`p=0.020`) | `+0.018` (`p=0.573`) |
| `lag(acs5_owner_cost_burden_30_plus)` | `+0.052` (`p=0.0005`) | `+0.042` (`p=0.0145`) | `-0.008` (`p=0.747`) |
| `lag(acs5_rent_to_income)` | `+0.017` (`p=0.536`) | `+0.063` (`p=0.031`) | `+0.004` (`p=0.926`) |

Read: there is no robust state-year evidence that higher prior affordability
burden is a leading indicator of subsequent rent growth.

ACS1 lagged measures are weaker still:

- `lag(acs1_rent_burden_40_plus)`: `b=-0.001`, `p=0.961` with year FE
- `lag(acs1_rent_burden_50_plus)`: `b=+0.008`, `p=0.724` with year FE
- `lag(acs1_rent_to_income)`: `b=-0.036`, `p=0.225` with year FE

## Secondary same-year results

The same-year ACS5 first-difference screen remains strong and negative across
all three fixed-effect variants for the headline measures.

| term | year FE | region x year FE | state x year FE |
|---|---:|---:|---:|
| `d_acs5_rent_burden_30_plus` | `-0.511` (`p=1.7e-06`) | `-0.529` (`p=3.9e-09`) | `-0.332` (`p=0.0006`) |
| `d_acs5_owner_cost_burden_30_plus` | `-1.448` (`p=1.1e-26`) | `-1.210` (`p=3.6e-16`) | `-0.463` (`p=0.0055`) |
| `d_acs5_rent_to_income` | `-0.944` (`p=0.0011`) | `-1.156` (`p=2.0e-06`) | `-0.559` (`p=0.053`) |

These are best read as same-year covariation, not evidence for a predictive
"later price dynamics" channel.

The year-FE unsheltered companion models remain null for the same ACS5 measures:

- `d_acs5_rent_burden_30_plus`: `p=0.147`
- `d_acs5_owner_cost_burden_30_plus`: `p=0.967`
- `d_acs5_rent_to_income`: `p=0.647`

## Bottom line

Housing cost burden is worth keeping as a descriptive same-year screen, but it
does not provide robust evidence for the motivating leading-indicator story once
the workflow is written to test prior burden against later rent growth and the
state-year fixed-effect comparison is surfaced directly in the tracked output.
