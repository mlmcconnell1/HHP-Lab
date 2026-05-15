# Methodology: ACS Aggregation

This section documents how ACS demographic measures are aggregated from census tracts to analysis geographies (CoC or metro).

## Target Geographies

The aggregation engine is geography-neutral via the `geo_id_col` parameter in `hhplab.measures.measures_acs.aggregate_to_geo()`:

- **CoC**: tracts are assigned to CoCs via area-weighted spatial crosswalk (`xwalk__B{boundary}xT{tract}.parquet`)
- **Metro**: tracts are assigned to metros via county membership (tract's county FIPS → metro county membership table)

The same weighting and coverage logic applies to both targets.

## Aggregation Algorithm

HHP-Lab uses **weighted tract-level aggregation** to produce geography-level estimates. The algorithm differs by measure type:

### Count Variables (population, poverty counts)

```
CoC_estimate = Σ(tract_value × weight)
```

Where `weight` is either:
- `area_share`: fraction of tract area falling within the CoC
- `pop_share`: population-proportional weight (`tract_pop × area_share / total`)

### Median Variables (income, rent)

```
CoC_estimate = Σ(tract_median × pop_weight) / Σ(pop_weight)
```

These are **population-weighted averages** of tract medians—NOT true medians computed from underlying household distributions.

## Why This Approach Is Acceptable

| Justification | Explanation |
|---------------|-------------|
| **Standard practice** | Aligns with HUD's own CoC-level reporting and academic research (e.g., Byrne et al., 2012). The Census Bureau does not publish CoC-level tabulations. |
| **ACS design constraints** | PUMS microdata uses PUMAs (~100k people) that don't nest within CoC boundaries, making true microdata pooling infeasible for most CoCs. |
| **Large-aggregate convergence** | CoCs typically span dozens to hundreds of tracts. At this scale, weighted aggregation converges toward true values (Central Limit Theorem). |
| **Explicit diagnostics** | The `coverage_ratio` field quantifies crosswalk completeness, enabling identification of problematic estimates. |

## Known Limitations vs True Pooled Microdata

### 1. Median Estimates Are Approximate

Averaging tract medians ≠ true population median. Example:

| Tract | Median Income | Population |
|-------|---------------|------------|
| A     | $100,000      | 5,000      |
| B     | $30,000       | 5,000      |

**Weighted average**: $65,000 — but true CoC median depends on the actual income distributions, not just tract medians.

### 2. MOE Propagation Not Implemented

ACS estimates include margins of error (MOE). Proper aggregated MOEs require variance formulas accounting for covariance. **CoC estimates should be treated as point estimates only.**

### 3. Ecological Inference Risk

Tract-level rates (e.g., poverty rate) may not reflect within-CoC variation. Using aggregated rates for individual-level inference is subject to **ecological fallacy**.

### 4. Boundary Mismatch Artifacts

When CoC boundaries cut through tracts, area weighting assumes uniform population distribution—false for mixed urban/rural tracts. Population weighting mitigates but doesn't eliminate this.

### 5. Temporal Mismatch

ACS 5-year estimates pool data across 5 years (e.g., A2022 covers 2018-2022). CoC boundaries may change during that period. This module assumes boundaries are static.

This creates an unavoidable **vintage gap**: the latest ACS vintage typically lags 1-2 years behind current boundaries. For example, B2025 boundaries can only be paired with A2023 or earlier. See [[08-Temporal-Terminology|Temporal Terminology]] for detailed discussion of vintage alignment.

### 6. Small-CoC Instability

CoCs with few tracts or low populations have estimates more sensitive to individual tract values and crosswalk precision.

### 7. Housing-Market Representativeness

Population-weighted tract coverage does not guarantee housing-market representativeness. Tracts with high population density may have systematically different rental markets, vacancy rates, or housing stock than lower-density tracts within the same CoC. Sensitivity analysis should explicitly test weighting choices (`area` vs `population`) and report effect size on downstream outcomes.

## ACS1 Modeled Tract Poverty

Census does not publish ACS1 tract estimates. HHP-Lab's ACS1 tract poverty
artifacts are modeled products that use ACS1 county control totals and ACS5
tract shares:

1. Read ACS1 county numerator/denominator controls for the target vintage.
2. Read ACS5 tract poverty numerator/denominator support for the matching tract
   era.
3. Within each county, compute tract shares from ACS5 support denominators.
4. Allocate ACS1 county totals to tracts with those shares.
5. Recompute rates from allocated numerators and denominators.

The output columns are
`acs1_imputed_population_below_poverty`,
`acs1_imputed_poverty_universe`, and
`acs1_imputed_poverty_rate`; optional household controls use the
`acs1_imputed_*` prefix. Allocation conserves the source county totals when
support is available. Nonzero source counties with all-null support are surfaced
as diagnostics or errors rather than silently fabricated.

These estimates should be described as modeled ACS1-imputed measures, not direct
ACS1 tract measures.

## ACS1/ACS5 Small-Area Estimation

SAE recipes allocate richer ACS1 county components through ACS5 tract
distributions and then roll the allocated components to target geographies such
as CoCs. The current allocation method is `tract_share_within_county`.

SAE v1 is component-based:

- Labor force counts allocate ACS1 county `civilian_labor_force` and
  `unemployed_count`, then derive `sae_unemployment_rate`.
- Rent-burden and owner-cost-burden measures allocate bins and derive rates
  from allocated numerators and denominators.
- Household-income and gross-rent median/quintile outputs are derived from
  allocated distributions, not by averaging median columns.

SAE outputs use the `sae_*` prefix and carry lineage/diagnostic fields for
source vintage, support vintage, tract vintage, source county counts,
zero-denominator conditions, missing support, allocation residuals, and optional
direct-county comparisons.

ACS1 vintage 2020 is unavailable. Preflight reports that as an availability
gap with no direct ingest command; recipes must choose another ACS1 vintage or
declare an explicit fallback policy.

## References

- Byrne, T., et al. (2012). "Predicting Homelessness Using ACS Data."
- HUD Exchange CoC Analysis Tools methodology documentation
- Census Bureau ACS Handbook, Chapter 12: "Working with ACS Data"

---

**Previous:** [[09-Workflows]] | **Next:** [[11-Methodology-ZORI-Aggregation]]
