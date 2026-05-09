# SAE Measure Contract v1

Date: 2026-05-09
Bead: `coclab-uc9r.1`

This spec defines the first HHP-Lab small-area estimation (SAE) contract for
allocating ACS1 county aggregates to tract components with ACS5 tract
distribution supports, then rolling the allocated components to CoCs or other
target geographies.

SAE v1 is intentionally component-based. It allocates counts, bins, and
distribution components. It does not average county or tract medians.

## Supported Measure Families

| Measure family | ACS1 source tables | ACS5 tract support tables | Denominator / support | Aggregation rule | Output columns |
| --- | --- | --- | --- | --- | --- |
| Household income bins | `B19001` | `B19001` | matching household income bin counts | Allocate each ACS1 county bin to tracts by the tract share of that county bin; sum allocated bins to target geography | `sae_household_income_*`, `sae_household_income_total` |
| Gross rent bins | `B25063` | `B25063` | matching gross-rent bin counts | Allocate each ACS1 county rent bin to tracts by the tract share of that county bin; sum allocated bins | `sae_gross_rent_distribution_*`, `sae_gross_rent_distribution_total` |
| Rent burden bins | `B25070` | `B25070` | matching renter burden bin counts | Allocate burden bins, then derive rates from allocated numerators and denominators | `sae_rent_burden_30_plus`, `sae_rent_burden_50_plus`, `sae_rent_burden_denominator` |
| Owner cost burden bins | `B25091` | `B25091` | matching owner burden bin counts by mortgage status | Allocate with- and without-mortgage burden bins; derive owner burden rates from allocated components | `sae_owner_cost_burden_30_plus`, `sae_owner_cost_burden_50_plus`, `sae_owner_cost_burden_denominator` |
| Tenure by household income | `B25118` | `B25118` | matching owner/renter income-bin counts | Allocate tenure-income bins independently, preserving owner/renter totals | `sae_tenure_income_*` |
| Labor-force counts | `B23025` | ACS5 tract labor-force counts where available | `civilian_labor_force`, `unemployed_count` | Allocate ACS1 county counts by ACS5 tract labor-force shares; derive unemployment rate after rollup | `sae_civilian_labor_force`, `sae_unemployed_count`, `sae_unemployment_rate` |

## Derived Measures

Rates are derived only after allocation and rollup:

- `sae_rent_burden_30_plus = allocated renter households with rent burden >= 30% / allocated computed renter burden denominator`
- `sae_rent_burden_50_plus = allocated renter households with rent burden >= 50% / allocated computed renter burden denominator`
- `sae_owner_cost_burden_30_plus = allocated owner households with owner cost burden >= 30% / allocated computed owner burden denominator`
- `sae_owner_cost_burden_50_plus = allocated owner households with owner cost burden >= 50% / allocated computed owner burden denominator`
- `sae_unemployment_rate = sae_unemployed_count / sae_civilian_labor_force`

Distribution-derived medians and quantiles are allowed only when implemented
from allocated distribution bins. They must expose the source bin family and
interpolation rule in provenance. Median and quintile values from ACS1 or ACS5
tables such as `B19013`, `B25064`, `B19080`, `B19081`, `B19082`, and `B25119`
are context fields only in v1 and must not be averaged.

## Recipe Semantics

SAE recipe syntax should be explicit rather than encoded as generic weighted
mean behavior. A recipe step should declare:

- `source_dataset`: ACS1 county artifact id.
- `support_dataset`: ACS5 tract artifact id and terminal vintage.
- `source_geometry`: `county`.
- `support_geometry`: `tract`.
- `target_geometry`: e.g. `coc`, `metro`, or another supported analysis geography.
- `allocation_method`: `tract_share_within_county`.
- `measures`: named SAE measure families and optional derived outputs.
- `zero_denominator_policy`: `null_rate`, `diagnostic`, or future explicit fallback.
- `diagnostics`: whether to emit conservation, denominator, and direct-county comparison diagnostics.

The planner must reject ambiguous recipes that request SAE from direct median
columns, omit ACS1 or ACS5 vintage metadata, use incompatible tract eras, or ask
for a target crosswalk that is not available.

## Output Schema

SAE component artifacts should include:

- geography columns: `target_geo_type`, `target_geo_id`, and the appropriate
  target identifier such as `coc_id`.
- vintage columns: `acs1_vintage`, `acs5_vintage`, `tract_vintage`.
- source lineage: `source_county_fips`, `allocation_method`, `support_table`,
  `source_table`, `denominator_column`.
- allocated component columns for requested bins/counts.
- derived measure columns prefixed with `sae_`.
- diagnostics columns: `sae_source_county_count`, `sae_missing_support_count`,
  `sae_zero_denominator_count`, `sae_unallocated_source_total`,
  `sae_allocation_residual`, `sae_direct_county_comparable`.

Final panel integration should add only requested derived `sae_*` measures to
analysis panels and keep component-heavy artifacts available separately for QA.

## Provenance

Every SAE parquet artifact must use existing provenance helpers and record:

- ACS1 source artifact path, vintage, tables, variables, and row count.
- ACS5 support artifact path, vintage, tract vintage, tables, variables, and
  row count.
- crosswalk artifact path, target geometry, tract era, and weighting method.
- requested measures and derived output columns.
- allocation method and zero-denominator policy.
- diagnostics summary, including conservation residuals.

## Unsupported In v1

SAE v1 intentionally excludes:

- averaging ACS1 or ACS5 medians.
- averaging ACS income quintile cutoffs, quintile means, or income shares.
- model-based smoothing, shrinkage, or uncertainty intervals.
- unsupported ACS products or non-county ACS1 source geometries for CoC SAE.
- cross-era tract support without the required tract relationship and explicit
  terminal-vintage support artifact.
- fallback from missing ACS1 2020 estimates to ACS5 without an explicit recipe
  policy. ACS1 2020 is unavailable and must be diagnosed as such.

## Implementation Responsibilities

| Area | Responsibility |
| --- | --- |
| `hhplab/acs` | Load and normalize ACS1 county source aggregates, load ACS5 tract support distributions, compute within-county tract shares, allocate source components, and provide reusable SAE helpers. |
| `hhplab/recipe` | Add schema models, planner tasks, preflight validation, missing-artifact remediation, and recipe execution hooks for SAE steps. |
| `hhplab/schema` | Define canonical SAE component, derived-measure, lineage, and diagnostics column constants. |
| `hhplab/panel` | Validate requested `sae_*` measures, merge final SAE outputs into panels, and keep SAE lineage distinct from direct ACS1/ACS5 measures. |
| `tests` | Use declarative fixtures and parametrized truth tables for source normalization, support normalization, allocation conservation, zero denominators, derived rates, recipe validation, provenance, and panel conformance. |

## Dependency Order

1. Normalize ACS1 county source components.
2. Normalize ACS5 tract support distributions.
3. Add recipe schema and planner/preflight validation.
4. Implement county-to-tract allocation.
5. Roll allocated tract components to target geographies.
6. Add derived rates, distribution-derived medians/quantiles where supported,
   diagnostics, panel integration, and examples.
