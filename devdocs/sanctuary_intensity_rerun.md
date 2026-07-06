# Sanctuary Intensity Rerun

Date: 2026-07-06

## Measure

The sanctuary MSA panel now includes `doj_sanctuary_population_share`, a
continuous exposure measure equal to the share of reference-year MSA county
population in counties covered by DOJ-listed sanctuary states, counties, or
cities. The reference population year is 2020, using
`data/curated/pep/pep_county__v2020.parquet`.

Generated artifact:

```bash
uv run hhplab generate sanctuary-msa-panel --force --json
```

Full MSA panel summary:

| Metric | Value |
| --- | ---: |
| Rows | 393 |
| Binary matched MSAs | 104 |
| Mean population-weighted intensity | 0.221383 |

## Rerun Panels

The top-50 analysis panels were copied to ignored analysis outputs with
`doj_sanctuary_population_share` merged by `msa_id`:

| Panel | Rows | Mean intensity |
| --- | ---: | ---: |
| `outputs/sanctuary_intensity/top50_msa_untangle_A2024_sanctuary_intensity.parquet` | 50 | 0.329236 |
| `outputs/sanctuary_intensity/tot_longdiff_sanctuary_intensity.parquet` | 50 | 0.329236 |
| `outputs/sanctuary_intensity/top50_msa_beds_longdiff_sanctuary_intensity.parquet` | 50 | 0.329236 |

## Regression Results

The continuous intensity measure replaced the prior binary `sanctuary`
indicator. Standard errors are the project CLI's default OLS standard errors,
matching the existing `hhplab analyze regress` output format.

| Spec | Outcome | Estimate | Std. error | p-value | R-squared |
| --- | --- | ---: | ---: | ---: | ---: |
| 2024 level | `pit_unshelt_per_1000` | 1.118277 | 0.257918 | 0.000092 | 0.701817 |
| 2015-2025 growth | `d_log_unshelt_rate_15_25` | 0.031331 | 0.215729 | 0.885135 | 0.000439 |
| 2015-2025 growth | `d_log_shelt_rate_15_25` | 0.635963 | 0.096612 | 0.000000032 | 0.474440 |
| 2015-2025 growth | `d_log_beds_15_25` | 0.697734 | 0.078888 | 0.000000000012 | 0.619733 |

## Commands

```bash
uv run hhplab analyze regress \
  --panel outputs/sanctuary_intensity/top50_msa_untangle_A2024_sanctuary_intensity.parquet \
  --outcome pit_unshelt_per_1000 \
  --predictors z_non_native_share,z_msa_contract_rent_p25,z_msa_vacancy_rate,z_msa_income,z_log_pop,z_jan_tmin_c,doj_sanctuary_population_share,z_log_dr_ratio \
  --entity-column msa_id \
  --no-entity-fe \
  --no-year-fe \
  --cluster-by "" \
  --output outputs/sanctuary_intensity/cross_section_unsheltered_intensity.parquet \
  --json

uv run hhplab analyze regress \
  --panel outputs/sanctuary_intensity/tot_longdiff_sanctuary_intensity.parquet \
  --outcome d_log_unshelt_rate_15_25 \
  --predictors doj_sanctuary_population_share \
  --entity-column msa_id \
  --no-entity-fe \
  --no-year-fe \
  --cluster-by "" \
  --output outputs/sanctuary_intensity/growth_unsheltered_intensity.parquet \
  --json

uv run hhplab analyze regress \
  --panel outputs/sanctuary_intensity/tot_longdiff_sanctuary_intensity.parquet \
  --outcome d_log_shelt_rate_15_25 \
  --predictors doj_sanctuary_population_share \
  --entity-column msa_id \
  --no-entity-fe \
  --no-year-fe \
  --cluster-by "" \
  --output outputs/sanctuary_intensity/growth_sheltered_intensity.parquet \
  --json

uv run hhplab analyze regress \
  --panel outputs/sanctuary_intensity/top50_msa_beds_longdiff_sanctuary_intensity.parquet \
  --outcome d_log_beds_15_25 \
  --predictors doj_sanctuary_population_share \
  --entity-column msa_id \
  --no-entity-fe \
  --no-year-fe \
  --cluster-by "" \
  --output outputs/sanctuary_intensity/growth_beds_intensity.parquet \
  --json
```
