# Methodology: Panel Assembly

Panel assembly uses `hhplab build recipe` to align heterogeneous inputs to a geography×year frame, then join. The target geography is CoC by default but can be metro in HHP-Lab workflows.

## Assembly Pattern

1. Resolve analysis year universe.
2. Resolve dataset/year paths and effective geometries.
3. Resample to target geometry (`identity`, `aggregate`, `allocate`).
4. Join resampled datasets on common keys (typically `geo_id`, `year`).
5. Persist panel/provenance outputs when requested by runtime mode and target outputs.

## Recipe Panel Characteristics

- Uses explicit YAML declarations for datasets/transforms/pipelines
- Planner resolves dataset-year tasks deterministically
- Executor runs `materialize -> resample -> join`, then persists only when the target includes `panel` output (default)
- Current persisted panel target is the configured `output_root/<recipe-name>/`
  (`outputs/<recipe-name>/` with built-in defaults)
- Recipe-native non-panel outputs such as `map`, `containment`, and
  `msa_coc_coverage` persist their own artifacts under the same recipe output
  namespace when declared in `targets[].outputs`
- Writes `*.manifest.json` sidecar listing consumed assets, with root-aware
  asset references for configurable storage layouts

## Metro Panel Assembly

When the target geography is metro, the recipe system adapts its behavior:

| Step | CoC | Metro |
|------|-----|-------|
| PIT source | CoC-native PIT counts | Aggregated from member CoCs via metro-CoC membership table |
| ACS5 measures | Tract→CoC crosswalk | Tract→county→metro via county membership |
| ACS1 metro measures | Not applicable | Metro-native CBSA identity resampling (optional, `panel_policy.acs1`) |
| ACS1 modeled/SAE measures | Modeled tract resampling or `small_area_estimate` | Not handled by `panel_policy.acs1`; use explicit recipe datasets/steps |
| PEP population | County→CoC crosswalk | County→metro via county membership |
| ZORI rents | County→CoC crosswalk | County→metro via county membership |
| Boundary alignment | `period_faithful` or `retrospective` | `definition_fixed` (metros are version-pinned, not vintaged) |
| `boundary_changed` | Tracks year-over-year CoC boundary shifts | Always `False` (definition is fixed within a version) |
| Schema | `PANEL_COLUMNS` with `coc_id` | `METRO_PANEL_COLUMNS` with `metro_id`, `geo_type`, `geo_id` |

PIT coverage tracking for metro panels reports `coc_count`, `coc_expected`, `coc_coverage_ratio`, and `missing_cocs` per metro-year to surface incomplete aggregation.

### ACS 1-Year Metro Integration

ACS 1-year data provides CBSA-native measures that do not require crosswalk-based aggregation. The current ingest covers B23025 employment/unemployment plus income distribution, housing-cost, utility-cost, tenure, housing-stock, and household-size detailed tables. When `panel_policy.acs1.include` is set on a metro target:

- ACS1 data is resampled via identity (metro-native) and joined into the panel on `geo_id` and `year`
- The merge is optional: if no ACS1 data is available for a given vintage, the panel proceeds with requested ACS1 measure columns set to null
- `acs1_vintage_used` reflects the resolved ACS1 input vintage (derived from the data, not a lag heuristic)
- `acs_products_used` tracks which ACS products contributed (`"acs5"` or `"acs5,acs1"`)

### ACS1-Derived CoC Integration

CoC panels can include ACS1-derived measures in two ways:

- Modeled ACS1 poverty tract datasets are resampled through the normal tract to
  CoC crosswalk. Rates are derived from weighted numerator/denominator counts
  after aggregation.
- SAE recipes run a `small_area_estimate` step that allocates ACS1 county
  components through ACS5 tract support distributions, rolls them to CoC, and
  joins the resulting `sae_*` measures.

These paths are intentionally separate from metro-native ACS1 integration.
`panel_policy.acs1` only controls direct CBSA ACS1 joins for metro targets.
CoC ACS1-derived measures must be declared as recipe datasets or SAE outputs so
their modeled lineage remains visible.

## Quality Signals

Across both paths, users should monitor:

- `coverage_ratio` and related diagnostics fields
- boundary change indicators in panel outputs
- missingness after joins

## MSA-CoC Overlap Coverage Methodology

MSA-CoC overlap coverage is a first-class recipe output for measuring how
population-ranked MSAs intersect CoC boundaries. The output is separate from
panel assembly because it has a one-year coverage grain, not a
geography-by-year measure panel grain.

The recipe executor builds coverage as follows:

1. Load CoC boundary polygons for `coc_boundary_vintage`.
2. Load TIGER county polygons for `county_vintage`.
3. Load MSA county membership for `msa_definition_version` and dissolve counties
   to selected MSA geometries.
4. Rank MSAs using the recipe-resampled MSA population frame identified by
   `ranking_population_source` and `ranking_reference_year`.
5. Keep the top `top_n` MSAs.
6. Intersect selected MSA geometries with CoC geometries.
7. Emit one row per MSA/CoC/basis pair for each requested `overlap_basis`.

For `area` rows, denominators are projected geometry areas in the analysis CRS.
For `population` rows, the required denominator source is ACS5 tract
`total_population`; tract population is allocated to MSA/CoC intersections by
tract area share. Other denominator families such as households or renter
households are not part of the current contract.

Two reciprocal percentages are emitted for every row:

- `msa_covered_by_coc_percent = intersection_value / msa_denominator`
- `coc_contained_in_msa_percent = intersection_value / coc_denominator`

These fields intentionally have different denominators. Use the first when
asking whether a CoC covers a meaningful share of a selected MSA. Use the
second when asking whether a CoC is mostly contained in a selected MSA.

Thresholds are basis-aware:

- `min_msa_area_coverage_share` applies only to `overlap_basis: area` rows.
- `min_msa_population_coverage_share` applies only to `overlap_basis:
  population` rows.
- Both thresholds filter on `msa_covered_by_coc_percent`.

Because area and population density are uneven, area coverage and population
coverage can rank the same MSA/CoC pair differently. Treat this as a diagnostic
signal rather than a contradiction.

Population-basis coverage requires these curated inputs:

```text
data/curated/acs/acs5_tracts__A<acs5_population_vintage>xT<tract_vintage>.parquet
data/curated/tiger/tracts__T<tract_vintage>.parquet
```

Run preflight before execution to surface missing denominator inputs:

```bash
hhplab build recipe-preflight --recipe recipes/examples/msa-coc-coverage.yaml --json
hhplab build recipe-plan --recipe recipes/examples/msa-coc-coverage.yaml --json
```

`recipe-plan --json` exposes the canonical `msa_coc_coverage_path`,
`msa_coc_coverage_manifest_path`, optional CSV path, included bases, denominator
vintages, thresholds, and resolved defaults so agents do not need to infer
artifact names from logs.

---

**Previous:** [[11-Methodology-ZORI-Aggregation]] | **Next:** [[13-Bundle-Layout]]
