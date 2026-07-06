# IRS SOI Migration Screening Findings

Date: 2026-07-06

This screening joins IRS SOI MSA migration flows to the top-50 MSA longitudinal
PIT/ZORI panel to test whether gross migration churn or income-selective
out-migration helps explain rent growth and unsheltered PIT changes.

## Inputs

- Base panel: `outputs/top50_msa_longitudinal_2010_2025.parquet`
- IRS MSA covariates: `data/curated/covariates/covariate_panel__irs_soi_migration__Y2012-2023.parquet`
- PIT-aligned IRS exposure file: `outputs/top50_msa_irs_soi_migration_pit_aligned.parquet`
- Enriched analysis panel: `outputs/top50_msa_longitudinal_irs_soi_pit_aligned_enriched.parquet`

IRS rows use the later filing year. For PIT-aligned screening, IRS flow year
`Y` was shifted to PIT year `Y + 1`, so 2021-to-2022 migration is used as a
candidate predictor for January 2023 PIT outcomes.

## Derived Measures

- `irs_gross_external_returns_per_1000`: inflow returns plus outflow returns,
  scaled by population.
- `irs_gross_total_churn_returns_per_1000`: external gross returns plus
  intra-MSA returns, scaled by population.
- `irs_net_returns_per_1000`: inflow returns minus outflow returns, scaled by
  population.
- `irs_low_agi_outflow_signature`: negative of outflow AGI-per-return minus
  inflow AGI-per-return, in thousands of dollars. Positive values indicate
  outflow returns have lower AGI than inflow returns.
- Interaction screens:
  `d_log_zori_x_irs_outflow_returns_per_1000` and
  `d_log_zori_x_irs_low_agi_outflow_signature`.

## Artifacts

- Correlations:
  `outputs/top50_msa_irs_soi_screen__analysis_correlate.parquet`
- Outflow interaction regression:
  `outputs/top50_msa_irs_soi_screen__analysis_regress_outflow_interaction.parquet`
- Low-AGI interaction regression:
  `outputs/top50_msa_irs_soi_screen__analysis_regress_lowagi_interaction.parquet`
- Corrected annual-gap panel:
  `outputs/irs_screen_fixed.parquet`
- Corrected outflow interaction regression:
  `outputs/irs_fix_reg_outflow.parquet`
- Corrected low-AGI interaction regression:
  `outputs/irs_fix_reg_lowagi.parquet`
- Each corrected regression parquet has a matching `.manifest.json` sidecar.

The original interaction regression artifacts are retained for auditability but
are misspecified for the exit-margin question: they use entity fixed effects
without year fixed effects and include gap-spanning 2020-to-2022 differences.
Use the corrected artifacts above for inference.

## Findings

In the original 400-row IRS-aligned sample, rent growth was only weakly related
to gross IRS migration churn. `d_log_zori` had correlations of `0.068` with
gross external returns per 1,000 residents and `0.048` with total churn returns
per 1,000. It was more related to net IRS returns per 1,000 (`0.141`), and the
panel's PEP population growth measure had a modest rent-growth correlation
(`0.168`).

Those descriptive rent-growth associations weaken after filtering to the
350-row annual-gap panel used by the corrected regressions. In that sample,
`d_log_zori` correlates `-0.005` with gross external returns per 1,000,
`-0.059` with total churn returns per 1,000, `0.117` with net IRS returns per
1,000, and `0.035` with the low-AGI outflow signature. Residualizing by year
does not restore a gross-churn signal (`-0.022` for gross external returns and
`-0.095` for total churn). The original income-selectivity correlation
(`0.216`) should therefore be treated as sample/specification-sensitive
descriptive evidence, not a stable headline result.

IRS net migration behaves as expected relative to population growth.
In the original screen, `d_log_pop` correlated `0.642` with IRS net returns per
1,000, compared with `0.233` for gross external returns and `0.269` for total
churn. In the corrected annual-gap panel, the same correlations are `0.554`,
`0.256`, and `0.289`; after residualizing by year, the net-returns correlation
is `0.573`. This validates that the IRS signal is aligned with broad
population movement even though it is tax-filer based.

Simple unsheltered-rate correlations do not show evidence that IRS
out-migration or income-selective out-migration substitutes for, or amplifies,
the rent-shock relationship in this top-50 panel. In the corrected annual-gap
sample, correlations between `d_log_unshelt_rate` and IRS migration measures
are all close to zero, including gross external returns (`-0.052`), total churn
(`-0.027`), net returns (`-0.020`), outflow returns (`-0.048`), and low-AGI
outflow signature (`-0.017`).

Corrected fixed-effect regressions use annual-gap rows only (`n=350`), include
MSA and year fixed effects, use MSA-clustered standard errors, and leave
predictors unstandardized so the rent-slope interaction is interpretable:

| Model | Term | Estimate | Std. error | p-value |
|-------|------|----------|------------|---------|
| Outflow interaction | `d_log_zori` | `3.661` | `1.700` | `0.031` |
| Outflow interaction | `irs_outflow_returns_per_1000` | `0.011` | `0.008` | `0.200` |
| Outflow interaction | `d_log_zori_x_irs_outflow_returns_per_1000` | `-0.203` | `0.120` | `0.090` |
| Low-AGI interaction | `d_log_zori` | `1.349` | `0.733` | `0.066` |
| Low-AGI interaction | `irs_low_agi_outflow_signature` | `0.002` | `0.002` | `0.298` |
| Low-AGI interaction | `d_log_zori_x_irs_low_agi_outflow_signature` | `-0.021` | `0.018` | `0.240` |

## Interpretation

For this top-50 MSA/year screening panel, IRS migration adds useful descriptive
information about population movement, but it does not provide strong evidence
that gross churn explains rent growth where net population does not. The
descriptive income-selectivity signal is weaker once the annual-gap/year-FE
analysis sample is enforced.

The exit-versus-unsheltered-margin hypothesis is suggestively supported in the
corrected outflow interaction screen. The rent-growth coefficient is positive
at zero outflow, while the rent-growth by outflow interaction is negative
(`-0.203`, `p=0.090`): MSA-years with higher out-migration have a damped
unsheltered response to rent shocks. This remains exploratory and should not be
read as a final structural finding. The low-AGI outflow interaction is properly
null in the corrected specification.

## Caveats

This is an exploratory top-50 MSA screen with 350 corrected annual-gap
MSA-year observations. IRS SOI excludes non-filers and therefore undercounts
the lowest-income households most relevant to homelessness risk. IRS suppresses
small county-pair flows into other-flow buckets; every MSA row in this run had
partial coverage below 1.0. PIT counts are noisy annual point-in-time measures.
The regressions are descriptive fixed-effect screens, not causal estimates.
