# ACS5 Expanded Covariates

This note documents the recipe-selectable ACS5 covariates that can be ingested
at tract geography and aggregated to CoC, metro, MSA, or other target
geographies. These columns are intentionally not added wholesale to the default
canonical ACS panel measure set. Default panel conformance remains driven by
`ACS_MEASURE_COLUMNS`; recipes and panel requests can opt into expanded columns
explicitly.

The code-owned source of truth is:

- `hhplab.acs.variables.ACS5_COVARIATE_REGISTRY`
- `hhplab.acs.variables.ACS5_EXPANDED_COVARIATE_COLUMNS`
- `hhplab.recipe.recipe_schema.ACS5_RECIPE_SELECTABLE_MEASURES`

## Supported Tables

| ACS5 table | Output family | Rollup method | Denominator or weight | Notes |
| --- | --- | --- | --- | --- |
| `B01003` | `total_population`, `moe_total_population` | Area-weighted count; root-sum-squared MOE | `area_share` | Count MOE propagation is implemented for total population. |
| `B01001` | `adult_population` | Area-weighted count | `area_share` | Derived from age-by-sex bins before rollup. |
| `B19013` | `median_household_income` | Weighted mean of tract medians | `total_population` fallback path | Approximation, not a pooled household median. |
| `B19301` | `per_capita_income` | Population-weighted mean | `total_population` | Scalar tract estimate averaged with population weights. |
| `B19083` | `gini_index` | Population-weighted mean | `total_population` | Approximate weighted average of tract Gini estimates; not a true pooled geography-level Gini. |
| `B25064` | `median_gross_rent` | Denominator-weighted mean of tract medians | `renter_households`, falling back to `total_population` | Approximation, not a pooled renter-household median. |
| `B25077` | `median_owner_occupied_home_value` | Denominator-weighted mean of tract medians | `owner_households`, falling back to `total_population` | Approximation, not a pooled owner-housing-unit median. |
| `B25002` | `vacancy_rate` and occupancy counts | Ratio of area-weighted sums | `total_housing_units` | Derived after count rollup. |
| `B25003` | tenure counts | Area-weighted counts | `area_share` | Provides owner/renter household denominators. |
| `C17002` | `poverty_rate`, poverty counts | Ratio of area-weighted sums | `poverty_universe` | `population_below_poverty` is derived from under-50% and 50-99% bins. |
| `B23025` | `unemployment_rate`, labor-force counts | Ratio of area-weighted sums | `civilian_labor_force` | Derived after count rollup. |
| `B19001` | household income distribution bins | Area-weighted counts | `area_share` | Supports recipe-selected distribution summaries and grouped-income comparisons. |
| `B25075` | owner-occupied value distribution bins | Area-weighted counts | `area_share` | Top bins `B25075_026E` and `B25075_027E` are unavailable before ACS5 vintage 2015. |
| `B25063` | gross rent distribution bins | Area-weighted counts | `area_share` | Supports recipe-selected rent distribution summaries. |
| `B25070` | `rent_burden_30_plus`, rent burden bins | Ratio of area-weighted sums | `gross_rent_pct_income_total` | Excludes households where rent burden is not computed. |
| `B25091` | owner cost burden bins | Area-weighted counts | `area_share` | Recipe-selectable support columns. |
| `B25118` | tenure-by-household-income bins | Area-weighted counts | `area_share` | Supports owner/renter income distribution analysis. |
| `B15003` | educational attainment 25+ bins | Area-weighted counts | `area_share` | Unavailable before ACS5 vintage 2012. |

## Gini Approximation

`B19083_001E` publishes a tract-level Gini index. HHP-Lab aggregates
`gini_index` as a population-weighted average of tract Gini estimates. This is
an explicit approximation. It is useful as a tract-weighted inequality
covariate, but it is not mathematically equivalent to recomputing a Gini index
from the pooled income distribution of the target geography.

If stronger methodology is needed later, compare the tract-weighted `B19083`
estimate against a grouped-income approximation derived from `B19001`
household income bins. That comparison should be treated as a separate
validation task because grouped-bin Gini estimates require interpolation and
top-code assumptions.

## Recipe Selection

Recipes can request expanded columns by naming them in a resample step, for
example:

```yaml
resample:
  dataset: acs5_tracts
  to_geometry:
    type: coc
    vintage: 2025
    source: hud_exchange
  method: aggregate
  via: tract_to_coc
  measures:
    per_capita_income:
      aggregation: weighted_mean
    gini_index:
      aggregation: weighted_mean
    owner_occupied_value_total:
      aggregation: sum
```

For source-owned ACS aggregation helpers, count and distribution bins use
area-weighted sums, rate outputs are recomputed from weighted numerators and
denominators, and scalar estimates use the denominator choices declared in the
registry. Generic recipe resampling still follows the aggregation methods
declared in the recipe step; recipe authors should use `sum` for count/bin
columns and `weighted_mean` for scalar estimates unless a source-owned
aggregation command is being used.
