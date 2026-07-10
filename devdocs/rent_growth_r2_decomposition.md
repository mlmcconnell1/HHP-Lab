# Rent-growth R-squared decomposition

## Question

How much annual MSA rent-growth variation remains unexplained after adding the
completed `coclab-mzpm7` covariate roadmap to the original population,
composition, supply, short-term-rental, and remote-work channels?

Run the tracked analysis with:

```bash
uv run hhplab build result rent-growth-r2-decomposition --json
```

## Design

The outcome is `d_log_zori`. Every model includes year fixed effects and is
estimated by OLS on one strict complete-case intersection. The roadmap is
represented by one previously screened focal measure per channel, except for
subsidized housing and the joint IRS inflow/outflow model, where both terms are
part of the tracked finding. The final model contains 20 predictors.

The common sample is 281 MSA-years from 106 MSAs in 2016-2018. Eviction Lab's
2018 coverage ceiling restricts the years, and BPS valuation availability is
the final material source of row attrition. This is much narrower than the
1,029-row sample in `coclab-0to4c`, so the old and new R-squared levels are not
directly comparable across samples.

## Results

| Model | Predictors | R-squared | Adjusted R-squared | Delta vs year FE |
| --- | ---: | ---: | ---: | ---: |
| Year FE only | 0 | 0.067 | 0.060 | 0.000 |
| + population growth | 1 | 0.242 | 0.233 | 0.174 |
| + original covariates | 8 | 0.492 | 0.473 | 0.425 |
| + roadmap Tier 1 | 15 | 0.526 | 0.496 | 0.459 |
| Everything tested | 20 | 0.589 | 0.554 | 0.522 |

On this common sample, adding the completed roadmap raises unadjusted
R-squared from 0.492 for the original predictor set to 0.589, a 9.7 percentage
point gain. The later roadmap block containing eviction growth, joint IRS
migrant income, QCEW employment growth, and BPS valuation growth contributes
6.3 points of that gain. About 41.1% remains unexplained by the full model;
using adjusted R-squared, about 44.6% remains unexplained.

The updated answer is therefore still "substantial unexplained variation," but
not the old claim that roughly three quarters remains unexplained. That older
claim described a different, much broader sample. The tracked coverage
artifact makes this sample dependency explicit and should be consulted before
using either number as a general population estimate.

## Artifacts

- `outputs/rent_growth_r2_decomposition/rent_growth_r2_decomposition_models.parquet`
- `outputs/rent_growth_r2_decomposition/rent_growth_r2_decomposition_coverage.parquet`
- `outputs/rent_growth_r2_decomposition/rent_growth_r2_decomposition_summary.json`

Both parquet artifacts include HHP-Lab provenance metadata.
