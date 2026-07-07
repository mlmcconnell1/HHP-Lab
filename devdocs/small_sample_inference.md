# Small-Sample Inference Rerun

Date: 2026-07-06

## Implementation

`hhplab analyze regress` now supports opt-in small-sample inference:

```bash
--inference wild-cluster
--inference permutation
--inference-reps <n>
--inference-seed <seed>
--inference-terms <term[,term...]>
```

When `--inference` is used, the requested term(s) use the small-sample
inference p-value in `p_value`; the conventional model p-value is retained as
`asymptotic_p_value`. Terms not listed in `--inference-terms` keep their
conventional p-values.

Wild-cluster inference uses Rademacher cluster weights and bootstrap-t p-values.
Permutation inference shuffles the requested cross-sectional term(s) and refits
the same design; it is restricted to models without fixed effects.

Permutation inference in this implementation is a raw-column permutation test:
the requested term is shuffled while the rest of the design is held fixed. Treat
those p-values as design-valid only when the permuted predictor is independent of
the other regressors, such as a single-predictor cross-section or a literally
randomized treatment. In multi-predictor models with correlated covariates, this
test can be anti-conservative. Prefer `--inference-terms` to name one focal
randomized term, and use wild-cluster or conventional sensitivity checks for
observational multi-covariate specifications.

For 2SLS models, `--inference wild-cluster` refits the full IV design in each
bootstrap draw and can be used with `--endogenous` and `--instruments`.
Permutation inference remains OLS-only.

Anderson-Rubin confidence sets for one-endogenous-variable IV designs are
available as a separate grid-inversion helper:

```bash
hhplab analyze iv-ar \
  --panel <panel.parquet> \
  --outcome <outcome> \
  --predictors <endogenous,controls> \
  --endogenous <endogenous> \
  --instruments <excluded_instrument[,instrument...]> \
  --grid-min -5 --grid-max 5 --grid-step 0.25 \
  --json
```

The JSON payload reports the 2SLS point estimate, per-grid AR p-values, and
the accepted confidence-set intervals at `--alpha` (default 0.05).

## Headline Reruns

All reruns used `--inference-reps 999 --inference-seed 20260706`.

| Spec | Term | Estimate | SE | Asymptotic p | Small-sample p | n |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| Top-50 FD ZORI elasticity | `d_log_zori` | 1.580 | 0.545 | 0.0039 | 0.003 | 400 |
| Top-50 lag/lead | `d_log_zori` | 2.279 | 0.759 | 0.0029 | 0.005 | 300 |
| Top-50 lag/lead | `d_log_zori_lag1` | 0.476 | 0.341 | 0.1630 | 0.185 | 300 |
| Top-50 lag/lead | `d_log_zori_lead1` | -0.223 | 0.469 | 0.6340 | 0.621 | 300 |
| Top-50 sanctuary interaction | `d_log_zori` | 1.289 | 0.673 | 0.0561 | 0.066 | 400 |
| Top-50 sanctuary interaction | `d_log_zori_x_sanctuary` | 0.776 | 1.025 | 0.4494 | 0.483 | 400 |
| IRS outflow churn interaction | `d_log_zori` | 3.661 | 1.700 | 0.0321 | 0.040 | 350 |
| IRS outflow churn interaction | `d_log_zori_x_irs_outflow_returns_per_1000` | -0.203 | 0.120 | 0.0915 | 0.106 | 350 |
| IRS low-AGI outflow interaction | `d_log_zori` | 1.349 | 0.733 | 0.0667 | 0.082 | 350 |
| IRS low-AGI outflow interaction | `d_log_zori_x_irs_low_agi_outflow_signature` | -0.021 | 0.018 | 0.2405 | 0.264 | 350 |
| Cross-sectional sanctuary level | `sanctuary` | 0.638 | 0.265 | 0.0205 | 0.004 | 50 |

## Interpretation

The top-50 first-difference rent-shock result survives wild-cluster inference
with 50 MSA clusters. In the lag/lead specification, the contemporaneous ZORI
shock also survives; lag and lead terms do not. The sanctuary interaction does
not survive small-sample inference. The corrected IRS outflow churn interaction
also does not survive: its p-value moves from `0.0915` to `0.106`. The
low-AGI outflow interaction remains non-significant. The cross-sectional
sanctuary level effect remains strong under permutation inference.

Generated ignored artifacts:

| Artifact | Purpose |
| --- | --- |
| `outputs/small_sample_inference/fd_zori_wild_cluster.parquet` | FD ZORI elasticity |
| `outputs/small_sample_inference/fd_zori_laglead_wild_cluster.parquet` | lag/lead spec |
| `outputs/small_sample_inference/fd_sanctuary_interaction_wild_cluster.parquet` | interaction spec |
| `outputs/small_sample_inference/irs_outflow_interaction_wild_cluster.parquet` | corrected IRS outflow interaction |
| `outputs/small_sample_inference/irs_lowagi_interaction_wild_cluster.parquet` | corrected IRS low-AGI interaction |
| `outputs/small_sample_inference/sanctuary_level_permutation.parquet` | sanctuary permutation test |
