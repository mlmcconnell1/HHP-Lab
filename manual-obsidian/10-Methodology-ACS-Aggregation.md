# Methodology: ACS Aggregation

This section documents how ACS demographic measures are aggregated from census tracts to analysis geographies (CoC or metro).

## Target Geographies

The aggregation engine is geography-neutral via the `geo_id_col` parameter in `coclab.measures.acs.aggregate_to_geo()`:

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

## References

- Byrne, T., et al. (2012). "Predicting Homelessness Using ACS Data."
- HUD Exchange CoC Analysis Tools methodology documentation
- Census Bureau ACS Handbook, Chapter 12: "Working with ACS Data"

---

**Previous:** [[09-Workflows]] | **Next:** [[11-Methodology-ZORI-Aggregation]]
