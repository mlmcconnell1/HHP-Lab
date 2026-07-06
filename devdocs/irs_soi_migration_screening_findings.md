# IRS SOI Migration Screening Findings

Date: 2026-07-06

This screening joins IRS SOI MSA migration flows to the top-50 MSA longitudinal
PIT/ZORI panel to test whether gross migration churn or income-selective
out-migration helps explain rent growth and unsheltered PIT changes.

## Inputs

- Base panel: `outputs/top50_msa_longitudinal_2010_2025.parquet`
- IRS MSA covariates: `data/curated/covariates/covariate_panel__irs_soi_migration__Y2011-ongoing.parquet`
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
- Each analysis parquet has a matching `.manifest.json` sidecar.

## Findings

Rent growth is only weakly related to gross IRS migration churn in this
screening sample. `d_log_zori` has correlations of `0.068` with gross external
returns per 1,000 residents and `0.048` with total churn returns per 1,000.
It is more related to net IRS returns per 1,000 (`0.141`), and the panel's PEP
population growth measure has a modest rent-growth correlation (`0.168`).

The strongest rent-growth migration signal is income selectivity, not gross
churn. `d_log_zori` correlates `0.216` with
`irs_low_agi_outflow_signature`, meaning higher rent-growth MSA-years tend to
have lower-AGI outflows relative to inflows. This is consistent with a
displacement-selection pattern, but it is descriptive and should not be read as
causal evidence.

IRS net migration behaves as expected relative to population growth.
`d_log_pop` correlates `0.642` with IRS net returns per 1,000, compared with
`0.233` for gross external returns and `0.269` for total churn. This validates
that the IRS signal is aligned with broad population movement even though it is
tax-filer based.

The unsheltered PIT screen does not show evidence that IRS out-migration or
income-selective out-migration substitutes for, or amplifies, the rent-shock
relationship in this top-50 panel. Correlations between `d_log_unshelt_rate`
and IRS migration measures are all close to zero, including gross external
returns (`-0.011`), total churn (`-0.005`), net returns (`0.013`), outflow
returns (`-0.015`), and low-AGI outflow signature (`-0.006`).

Fixed-effect regressions with standardized predictors and MSA-clustered
standard errors also do not identify a meaningful interaction:

| Model | Term | Estimate | Std. error | p-value |
|-------|------|----------|------------|---------|
| Outflow interaction | `d_log_zori` | `-0.005` | `0.065` | `0.938` |
| Outflow interaction | `irs_outflow_returns_per_1000` | `-0.001` | `0.042` | `0.984` |
| Outflow interaction | `d_log_zori_x_irs_outflow_returns_per_1000` | `0.046` | `0.068` | `0.499` |
| Low-AGI interaction | `d_log_zori` | `0.043` | `0.029` | `0.137` |
| Low-AGI interaction | `irs_low_agi_outflow_signature` | `0.004` | `0.025` | `0.876` |
| Low-AGI interaction | `d_log_zori_x_irs_low_agi_outflow_signature` | `-0.010` | `0.012` | `0.395` |

## Interpretation

For this top-50 MSA/year screening panel, IRS migration adds useful descriptive
information about population movement and income selectivity, but it does not
provide strong evidence that gross churn explains rent growth where net
population does not. The notable result is the positive rent-growth association
with lower-AGI outflow selectivity.

The exit-versus-unsheltered-margin hypothesis is not supported by this initial
screen: the rent-shock by out-migration interactions are small and statistically
weak. Follow-up work should treat this as a null screening result, not a final
structural finding.

## Caveats

This is an exploratory top-50 MSA screen with roughly 400 MSA-year IRS-aligned
observations. IRS SOI excludes non-filers and therefore undercounts the
lowest-income households most relevant to homelessness risk. IRS suppresses
small county-pair flows into other-flow buckets; every MSA row in this run had
partial coverage below 1.0. PIT counts are noisy annual point-in-time measures.
The regressions are descriptive fixed-effect screens, not causal estimates.
