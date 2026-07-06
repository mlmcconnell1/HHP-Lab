# MSA Rank 51-150 Replication

This documents the out-of-sample replication for bead `coclab-cp5ad`.

## Scope

- Source recipe: `recipes/msa-rank51-150-longitudinal-2015-2025.yaml`
- Source panel: `/home/mlm/Source/HHP-Data/msa_rank51_150_longitudinal_2015_2025_source_top150/panel__msa__Y2015-2025@Mcensusmsa2023.parquet`
- Local analysis artifacts: `outputs/msa_rank51_150_replication/`
- Years: 2015-2025, excluding 2021.
- Cohort rule: build the supported top-150 MSA longitudinal panel, rank by 2020 PEP population, then retain ranks 51-150.

## Inclusion

The replication panel contains 100 MSAs and 1,000 metro-year rows. Adjacent-year first-difference models use 800 rows before complete-case filtering.

ZORI coverage is thinner than in the top-50 sample:

- 87 MSAs have non-null ZORI in all 10 included years.
- 56 MSAs have `zori_coverage_ratio >= 0.8` in all 10 included years.
- Row-level `zori_coverage_ratio >= 0.8` sensitivity panels contain 580 level rows and 464 adjacent-year first-difference rows.

Primary models use complete cases with non-null ZORI. Sensitivity models apply the row-level `zori_coverage_ratio >= 0.8` filter.

## Key Results

| Model | Term | Estimate | SE | p | n |
| --- | --- | ---: | ---: | ---: | ---: |
| FD unsheltered, all non-null ZORI | `d_log_zori` | 1.956 | 0.540 | 0.0003 | 690 |
| FD total, all non-null ZORI | `d_log_zori` | 1.125 | 0.365 | 0.0020 | 691 |
| Levels FE unsheltered, all non-null ZORI | `log_zori` | 0.655 | 0.537 | 0.2227 | 864 |
| FD unsheltered, ZORI coverage >= 0.8 rows | `d_log_zori` | 2.381 | 0.646 | 0.0002 | 459 |
| FD total, ZORI coverage >= 0.8 rows | `d_log_zori` | 1.172 | 0.242 | 0.000001 | 459 |
| Levels FE unsheltered, ZORI coverage >= 0.8 rows | `log_zori` | 1.508 | 0.625 | 0.0158 | 575 |
| Long difference unsheltered 2015-2025 | `d_log_zori_15_25` | -0.144 | 0.769 | 0.8518 | 86 |
| Long difference sanctuary -> unsheltered | `sanctuary` | 0.036 | 0.180 | 0.8395 | 95 |
| Long difference sanctuary -> sheltered | `sanctuary` | 0.423 | 0.080 | 0.0000001 | 95 |

## Interpretation

The mid-size MSA replication supports the annual first-difference rent result: ZORI rent growth predicts unsheltered growth with an elasticity near 2.0 in the all-ZORI complete-case sample, and near 2.4 when restricted to rows with at least 0.8 ZORI coverage.

The levels FE result is sensitive to the coverage rule. It is weak with all non-null ZORI rows, but positive and significant under the 0.8 coverage filter. The 2015-2025 long-difference rent result does not replicate in this rank window.

The sanctuary decomposition does replicate directionally: sanctuary status is unrelated to unsheltered growth, while sheltered growth is substantially higher in sanctuary metros.

Lag/lead models are underpowered after requiring adjacent previous and next rent changes. Same-year rent remains positive, but lag and lead terms are imprecise in both all-ZORI and coverage-filtered samples.
