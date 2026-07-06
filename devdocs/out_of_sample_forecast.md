# Out-of-sample rent forecast test

Bead `coclab-jgul6` requested a pre-2020 fit and 2022-2025 holdout score for
the top-50 MSA rent-elasticity model.

## Inputs

- FD panel: `outputs/top50_msa_fd_2015_2025.parquet`
- Levels panel: `outputs/top50_msa_longitudinal_2015_2025.parquet`
- Generated ignored artifacts:
  - `outputs/out_of_sample_forecast/fd_predictions.csv`
  - `outputs/out_of_sample_forecast/levels_predictions.csv`
  - `outputs/out_of_sample_forecast/forecast_scores.csv`
  - `outputs/out_of_sample_forecast/summary.json`

The FD panel has valid complete-case holdout rows for 2023-2025 only
(`n=150`), because the 2022 differenced row is absent after the 2021 PIT
disruption/gap handling. The levels panel scores 2022-2025 (`n=200`).

## Models

FD change model, fit on 2016-2019:

```text
d_log_unshelt_rate ~ d_log_zori + d_log_pop
```

Estimated coefficients:

| term | estimate |
|---|---:|
| Intercept | 0.0245 |
| `d_log_zori` | -0.1293 |
| `d_log_pop` | 2.0149 |

Levels model, fit on 2015-2019 with MSA fixed effects:

```text
log_unshelt_rate ~ log_zori + log_pop + MSA FE
```

Core estimated coefficients:

| term | estimate |
|---|---:|
| Intercept | -35.2464 |
| `log_zori` | 0.6450 |
| `log_pop` | 1.9127 |

## Holdout scores

Lower RMSE/MAE is better. The holdout national-trend benchmark uses the actual
holdout-year cross-MSA mean change, so it is an upper-bound descriptive
benchmark rather than a deployable forecast.

| spec | model | n | RMSE | MAE | bias |
|---|---|---:|---:|---:|---:|
| FD change | holdout national trend | 150 | 0.2430 | 0.1656 | 0.0000 |
| FD change | pre-2020 mean trend | 150 | 0.2566 | 0.1814 | -0.0357 |
| FD change | rent model | 150 | 0.2587 | 0.1816 | -0.0349 |
| FD change | persistence | 150 | 0.2642 | 0.1865 | -0.0725 |
| Levels FE | holdout national trend | 200 | 0.4586 | 0.3478 | 0.0000 |
| Levels FE | pre-2020 mean trend | 200 | 0.4763 | 0.3473 | -0.1138 |
| Levels FE | rent FE model | 200 | 0.4927 | 0.3791 | -0.0099 |
| Levels FE | persistence | 200 | 0.5448 | 0.3984 | -0.2793 |

## Interpretation

The rent-only descriptive elasticity does not materially improve out-of-sample
forecast accuracy across the COVID break. In first differences, the rent model
barely beats persistence but trails both national-trend benchmarks. In levels,
the MSA fixed-effect rent model beats persistence but trails the simple
pre-2020 mean-trend baseline.

This supports treating the rent coefficients as descriptive associations rather
than a standalone forecasting model for 2022-2025 metro-level unsheltered
homelessness.
