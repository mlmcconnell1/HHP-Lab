# Recipe Format

Recipes are declarative YAML files that describe a complete build: which datasets to pull, what geometry to target, how to resample and join data, and what outputs to produce. The recipe system separates **structural** validation (schema shape, types, referential integrity) from **semantic** validation (whether adapters exist for referenced geometries and datasets).

Unless stated otherwise, path examples in this chapter assume the built-in
storage-root defaults:

- `asset_store_root = <project_root>/data`
- `output_root = <project_root>/outputs`

## Minimal Example

```yaml
version: 1
name: demo
universe:
  range: "2018-2024"

targets:
  - id: coc_panel
    geometry: { type: coc, vintage: 2025, source: hud_exchange }
    outputs: [panel]

datasets:
  pit:
    provider: hud
    product: pit
    version: 1
    native_geometry: { type: coc }
    params: { vintage: 2024, align: point_in_time_jan }
    path: data/curated/pit/pit_vintage__P2024.parquet

transforms: []

pipelines:
  - id: build_coc_panel
    target: coc_panel
    steps:
      - resample:
          dataset: pit
          to_geometry: { type: coc, vintage: 2025 }
          method: identity
          measures: [pit_total]
      - join:
          datasets: [pit]
          join_on: [geo_id, year]
```

## Running a Recipe

```bash
# No-execute readiness check
hhplab build recipe-preflight --recipe recipes/demo.yaml --json

# Optional: inspect resolved tasks while authoring/debugging
hhplab build recipe-plan --recipe recipes/demo.yaml --json

# Execute
hhplab build recipe --recipe recipes/demo.yaml
```

`hhplab build recipe` is the normal entrypoint. Use `--dry-run` when you want
the same validation/preflight path without execution.

Use `--asset-store-root` and `--output-root` when you need the canonical asset
and output locations to resolve somewhere other than the default repo-local
layout.

## Connecticut County-Equivalent Transition

Connecticut changed Census county-equivalent geography from 8 legacy counties
(`09001`-`09015`) to 9 planning regions (`09110`-`09190`). This matters for
county-native recipe inputs because HHP-Lab can now combine:

- legacy county crosswalks such as `xwalk__B2025xC2020.parquet`
- newer county-native datasets whose Connecticut rows already use planning-region
  FIPS, especially `pep_county__v2024.parquet`
- county-weighted transforms where the `population_source` dataset can switch to
  planning-region FIPS even if the main dataset still uses legacy county FIPS

Near-term HHP-Lab policy:

- Do not silently drop or misweight Connecticut rows.
- If the recipe crosswalk is keyed to legacy counties and the dataset or support
  weights are keyed to Connecticut planning regions, the runtime applies a
  Connecticut special-case normalization and translates planning-region rows
  back to legacy counties before the aggregate join.
- If that normalization cannot be performed, preflight/build fail explicitly
  instead of emitting near-zero population values or null weighted measures.

Current runtime diagnostics:

- `hhplab build recipe-preflight --json` emits `ct_county_alignment` findings
  when this special-case path will be used.
- `hhplab build recipe --json` records per-step `notes` when the alignment was
  actually applied during execution.
- Human-readable `build recipe` output prints the same alignment note inline
  during the affected resample step.

Required bridge artifact:

- The current runtime builds the authoritative Connecticut bridge from
  `asset_store_root/curated/tiger/counties__C2020.parquet` and
  `asset_store_root/curated/tiger/counties__C2023.parquet`
  (`data/curated/...` with built-in defaults).
- If `counties__C2023.parquet` is missing, the remediation command is:

```bash
hhplab ingest tiger --year 2023 --type counties
```

Affected situations:

- CoC or metro recipes that aggregate county-native PEP inputs through a
  legacy-county crosswalk
- county-weighted ZORI recipe steps whose `population_source` is PEP
- any other county-native recipe path that mixes Connecticut planning-region
  dataset rows with a legacy-county crosswalk

This behavior is intentional. It exists to prevent the Connecticut corruption
fixed in beads `coclab-i6od`, `coclab-vnzm`, `coclab-ybc0`, and `coclab-2kag`.

## Top-Level Structure

Every recipe is a YAML mapping with these top-level keys:

| Key | Type | Required | Description |
|-----|------|----------|-------------|
| `version` | `int` | Yes | Schema version. Currently `1`. |
| `name` | `string` | Yes | Human-readable recipe name. |
| `description` | `string` | No | Optional longer description. |
| `universe` | `YearSpec` | Yes | Year domain for the build. |
| `targets` | `list[TargetSpec]` | Yes | Output targets (geometries + output types). |
| `datasets` | `dict[string, DatasetSpec]` | Yes | Named dataset declarations. |
| `filters` | `dict[string, FilterSpec]` | No | Temporal filters keyed by dataset id, applied before resampling. |
| `transforms` | `list[TransformSpec]` | Yes | Spatial transform operators (may be empty). |
| `pipelines` | `list[PipelineSpec]` | Yes | Ordered step sequences that produce targets. |
| `validation` | `ValidationPolicy` | No | Override default validation thresholds. |

## YearSpec

Defines the temporal universe. Exactly one of `range` or `years` must be set.

```yaml
# Range form (inclusive)
universe:
  range: "2018-2024"

# Explicit list form
universe:
  years: [2018, 2020, 2022, 2024]
```

## GeometryRef

References a geometry universe. Used in targets, datasets, and resample steps.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `type` | `string` | Yes | Geometry type identifier (open set: `coc`, `tract`, `county`, `state`, `zcta`, etc.). |
| `vintage` | `int` | No | Vintage year for the geometry. |
| `source` | `string` | No | Provenance hint (e.g., `hud_exchange`, `tiger`, `nhgis`). |

```yaml
geometry: { type: coc, vintage: 2025, source: hud_exchange }
```

Geometry types are an **open set** — the schema accepts any string. Runtime adapter registries validate whether an adapter exists for the referenced type.

## Targets

Each target declares a geometry and requested output types.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `string` | Yes | Unique target identifier. Referenced by pipelines. |
| `geometry` | `GeometryRef` | Yes | Target geometry for the pipeline. |
| `outputs` | `list[string]` | No | Output kinds: `panel`, `diagnostics`, `export`. Default: `[panel]`. |
| `panel_policy` | `PanelPolicy` | No | Declarative panel output and finalization policy (ZORI, ACS1, aliases). |
| `cohort` | `CohortSelector` | No | Optional cohort filter to keep a ranked subset of geographies. |

```yaml
targets:
  - id: coc_panel
    geometry: { type: coc, vintage: 2025, source: hud_exchange }
    outputs: [panel, diagnostics]
    cohort:
      rank_by: total_population
      method: top_n
      n: 50
      reference_year: 2024
```

Target ids must be unique within a recipe.

> **Current runtime note:** `targets[].outputs` now gates persistence behavior.
> Panel persistence occurs only when:
> 1. the pipeline has join tasks, and
> 2. the target outputs include `panel` (default).
>
> Declaring currently unsupported outputs (for example `diagnostics`, `export`)
> is allowed by schema and retained as intent, but runtime emits a warning and
> does not materialize those output types yet.

### Panel Policy

Optional declarative policy controlling panel finalization behavior. This is the recipe-native mechanism for ZORI eligibility, ACS 1-year integration, source labeling, and column renaming — replacing the implicit defaults that were previously baked into `build_panel`.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `source_label` | `string` | No | Override the default source label (e.g. `hhplab_panel`). |
| `zori` | `ZoriPolicy` | No | ZORI eligibility and provenance policy. Null means no ZORI integration. |
| `acs1` | `Acs1Policy` | No | ACS 1-year merge policy (metro targets only). |
| `column_aliases` | `dict[str, str]` | No | Column rename mapping for output (e.g. `{total_population: acs_total_population}`). |

**ZoriPolicy fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `min_coverage` | `float` | `0.90` | Minimum `zori_coverage_ratio` for a geography to be ZORI-eligible. |

When `panel_policy.zori` is present, the executor canonicalizes the aggregated ZORI measure to `zori_coc`, applies eligibility rules, computes `rent_to_income`, and adds provenance columns (`rent_metric`, `rent_alignment`, `zori_min_coverage`).

**Acs1Policy fields:**

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `include` | `bool` | `false` | If true, merge ACS 1-year metro-native measures into the panel. |

When `panel_policy.acs1.include` is true on a metro target, the executor preserves requested ACS1 metro-native measure columns, adds `acs1_vintage_used`, and sets `acs_products_used` to `"acs5,acs1"`. Existing example recipes request `unemployment_rate_acs1`, but the ACS1 ingest artifact also contains additional income, housing-cost, utility-cost, tenure, housing-stock, and household-size measures.

Recipe datasets with `provider: census`, `product: acs1`, and `native_geometry.type: county` may point at county-native ACS1 artifacts such as `data/curated/acs/acs1_county__A2023.parquet`. Use `year_column: acs1_vintage` and `geo_column: county_fips` or `geo_id`; recipes must account for Census ACS1 threshold sparsity because omitted counties are not present in the artifact.

ACS1-derived CoC measures are not enabled through `panel_policy.acs1`, which is
metro-only. For CoC workflows, use either a modeled ACS1 tract dataset
(`product: acs1_poverty`) with normal `resample` steps, or an explicit
`small_area_estimate` step that allocates ACS1 county components through ACS5
tract support distributions.

```yaml
targets:
  - id: metro_panel
    geometry: { type: metro, source: census_msa_2023 }
    outputs: [panel]
    panel_policy:
      acs1:
        include: true
      zori:
        min_coverage: 0.90
      source_label: hhplab_metro_panel
```

### Cohort Selector

Optional declarative filter that ranks geographies by a measure column at a reference year and keeps only the selected subset.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `rank_by` | `string` | Yes | Measure column to rank on (e.g., `total_population`). |
| `method` | `string` | Yes | `top_n`, `bottom_n`, or `percentile`. |
| `n` | `int` | Conditional | Number of geographies to keep. Required for `top_n`/`bottom_n`. |
| `threshold` | `float` | Conditional | Percentile cutoff (0.0–1.0). Required for `percentile`. `0.75` keeps the top 25%. |
| `reference_year` | `int` | Yes | Year whose values are used for ranking. Must be in the universe. |

The cohort filter is applied during output persistence, so all pipeline steps still operate on the full geography set.

## Datasets

Named dataset declarations keyed by a unique string id. The id is used to reference datasets in pipeline steps.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `provider` | `string` | Yes | Dataset provider namespace (e.g., `hud`, `census`, `zillow`). |
| `product` | `string` | Yes | Product name within provider (e.g., `pit`, `acs5`, `pep`). |
| `version` | `int` | Yes | Adapter version (schema evolution control). Must be >= 1. |
| `native_geometry` | `GeometryRef` | Yes | Native spatial granularity of the dataset. |
| `years` | `YearSpec` | No | Optional declared year coverage for static-path datasets. |
| `params` | `dict` | No | Free-form adapter parameters. Default: `{}`. |
| `path` | `string` | No | Optional project-relative path to a pre-materialized dataset file. Must be relative (not absolute). Explicit `path` values are not remapped through storage-root config. |
| `file_set` | `FileSetSpec` | No | Time-banded path/geometry config for datasets whose source files vary by year. |
| `optional` | `bool` | No | If true, missing dataset does not fail the build. Default: `false`. |

```yaml
datasets:
  pit:
    provider: hud
    product: pit
    version: 1
    native_geometry: { type: coc }
    params: { vintage: 2024, align: point_in_time_jan }
    path: data/curated/pit/pit_vintage__P2024.parquet

  acs:
    provider: census
    product: acs5
    version: 1
    native_geometry: { type: tract }
    params: { vintage: "2019-2023", measures: [total_population, median_household_income] }
    path: data/curated/measures/measures__A2023@B2025xT2020.parquet

  zori_county:
    provider: zillow
    product: zori
    version: 1
    native_geometry: { type: county }
    params: { align: point_in_time_jan }
    path: data/curated/zori/zori__county__Z2025.parquet
```

For ZORI ingest outputs, the canonical curated path format is
`data/curated/zori/zori__{geography}__Z{max_year}.parquet`, where `max_year`
is the maximum year present in the ingested series.

Like geometry types, provider/product combinations are an open set validated by runtime dataset adapters.

### FileSetSpec (Compact Vintage Rules)

Use `file_set` when a dataset path or geometry vintage changes by year. This keeps YAML terse while still expressing an explicit vintage set.

```yaml
datasets:
  acs:
    provider: census
    product: acs
    version: 1
    native_geometry: { type: tract }
    file_set:
      path_template: "data/curated/acs/acs5_tracts__A{acs_end}xT{tract}.parquet"
      segments:
        - years: "2015-2019"
          geometry: { type: tract, vintage: 2010, source: nhgis }
          constants: { tract: 2010 }
          year_offsets: { acs_end: -1 }
        - years: "2020-2024"
          geometry: { type: tract, vintage: 2020, source: tiger }
          constants: { tract: 2020 }
          year_offsets: { acs_end: -1 }
```

`file_set.path_template` supports:
- `{year}`: the analysis year
- `constants`: segment-level fixed variables
- `year_offsets`: segment-level derived variables (`value = year + offset`)

Optional `overrides` still let you replace specific years with an explicit path.

Modeled ACS1 poverty recipes use the same `file_set` mechanism because the
tract era changes across analysis years:

```yaml
datasets:
  acs1_poverty_tract:
    provider: census
    product: acs1_poverty
    version: 1
    native_geometry: { type: tract }
    file_set:
      path_template: "data/curated/acs/acs1_poverty_tracts__A{acs1_end}xT{tract}.parquet"
      segments:
        - years: "2010-2019"
          geometry: { type: tract, vintage: 2010 }
          constants: { tract: 2010 }
          year_offsets: { acs1_end: -1 }
        - years: "2020-2024"
          geometry: { type: tract, vintage: 2020 }
          constants: { tract: 2020 }
          year_offsets: { acs1_end: -1 }
```

This artifact is modeled, not a direct Census tract ACS1 product. Census does
not publish ACS1 tract data.

## Transforms

Spatial transform operators that define how data moves between geometries. Each transform has a unique `id` and a `type` that selects the operator.

### Crosswalk Transform

Builds crosswalk shares between two geometries. Direct tract/county overlays
use `scheme: area` or `scheme: population`. County-native source data can also
request `scheme: tract_mediated`, which uses the wide
`xwalk_tract_mediated_county__A{acs}@B{boundary}xC{county}xT{tract}.parquet`
artifact instead of the direct county overlay.

```yaml
transforms:
  - id: tract_to_coc
    type: crosswalk
    from: { type: tract, vintage: 2023 }
    to: { type: coc, vintage: 2025 }
    spec:
      weighting:
        scheme: population
        population_source: acs
        population_field: total_population
```

| `spec.weighting` Field | Type | Required | Description |
|------------------------|------|----------|-------------|
| `scheme` | `area`, `population`, or `tract_mediated` | Yes | Weighting method or family for crosswalk shares. |
| `population_source` | `string` | No | Dataset id for population weights (when `scheme: population`). |
| `population_field` | `string` | No | Field name in the population dataset. |
| `variety` | `area`, `population`, `households`, or `renter_households` | For single `tract_mediated` runs | One tract-mediated county weighting variety. |
| `varieties` | list of weighting varieties | For multi-variety `tract_mediated` runs | Planned as independent resample tasks for sensitivity analysis. |
| `tract_vintage` | integer or string | For `tract_mediated` | Tract vintage used to build the mediated artifact. |
| `acs_vintage` | integer or string | For `tract_mediated` | ACS denominator vintage used by demographic varieties. |

Example tract-mediated sensitivity transform:

```yaml
transforms:
  - id: county_to_coc_tract_mediated
    type: crosswalk
    from: { type: county, vintage: 2020 }
    to: { type: coc, vintage: 2025 }
    spec:
      weighting:
        scheme: tract_mediated
        varieties: [area, population, renter_households]
        tract_vintage: 2020
        acs_vintage: 2023
```

Denominator semantics:

- `area` uses tract intersection area.
- `population` uses `total_population` from ACS tract denominators.
- `households` uses `total_households`.
- `renter_households` uses `renter_households`.

All varieties reuse the same wide tract-mediated crosswalk artifact. Missing
optional household denominators produce nullable household weight columns;
missing required tract crosswalk or ACS denominator inputs are preflight
blockers with generation/ingest command hints.

### Rollup Transform

Deterministic administrative rollup (e.g., tract → county via FIPS prefix).

```yaml
transforms:
  - id: tract_to_county
    type: rollup
    from: { type: tract, vintage: 2023 }
    to: { type: county, vintage: 2023 }
    spec:
      keys:
        from_key: geoid
        to_key: state_fips
      derive: {}
```

Transform ids must be unique within a recipe.

## Pipelines

Each pipeline targets one target and defines an ordered sequence of steps.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | `string` | Yes | Unique pipeline identifier. |
| `target` | `string` | Yes | Target id that this pipeline materializes. |
| `steps` | `list[StepSpec]` | Yes | Ordered steps. |

Pipeline ids must be unique. The `target` must reference a declared target id.

### Step Syntax

Steps can be written in either **wrapper** or **canonical** form:

```yaml
# Wrapper form (preferred for readability)
steps:
  - resample:
      dataset: pit
      method: identity
      # ...

# Canonical form (explicit kind discriminator)
steps:
  - kind: resample
    dataset: pit
    method: identity
    # ...
```

### Materialize Step

Ensures that referenced transforms are computed and cached.

```yaml
- materialize:
    transforms: [tract_to_coc]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `transforms` | `list[string]` | Yes | Transform ids to materialize. Must reference declared transforms. |

### Resample Step

Resamples a dataset to a different geometry.

```yaml
- resample:
    dataset: acs
    to_geometry: { type: coc, vintage: 2025 }
    method: aggregate
    via: tract_to_coc
    measures: [total_population, median_household_income]
    aggregation: weighted_mean
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `dataset` | `string` | Yes | Dataset id to resample. Must reference a declared dataset. |
| `to_geometry` | `GeometryRef` | Yes | Destination geometry. |
| `method` | `string` | Yes | `identity`, `allocate`, or `aggregate`. |
| `via` | `string` | Conditional | Transform id. Required for `allocate` and `aggregate`; forbidden for `identity`. |
| `measures` | `list[string]` | Yes | Field names to carry through. |
| `aggregation` | `string` | No | `sum`, `mean`, or `weighted_mean`. Used with `method: aggregate`. |

**Method semantics:**

| Method | Direction | `via` | Use case |
|--------|-----------|-------|----------|
| `identity` | Same geometry | Forbidden | Dataset already at target geometry (e.g., PIT → CoC). |
| `allocate` | Few → many | Required | Distribute values via crosswalk shares. |
| `aggregate` | Many → few | Required | Aggregate values via crosswalk (e.g., tract ACS → CoC). |

For modeled ACS1 poverty rates, derive rates from allocated components rather
than averaging tract rates directly:

```yaml
aggregation:
  acs1_imputed_poverty_universe: { aggregation: sum }
  acs1_imputed_poverty_rate:
    aggregation: rate_from_weighted_counts
    source_rate_column: acs1_imputed_poverty_rate
    denominator_column: acs1_imputed_poverty_universe
    numerator_output_column: acs1_imputed_population_below_poverty
```

### SmallAreaEstimate Step

`small_area_estimate` allocates ACS1 county aggregate components through ACS5
tract support distributions, then rolls the allocated tract components to the
target geography.

```yaml
- small_area_estimate:
    output_dataset: acs_sae_coc
    source_dataset: acs1_county_sae
    support_dataset: acs5_tract_sae_support
    to_geometry: { type: coc, vintage: 2025 }
    via: tract_to_coc
    terminal_acs5_vintage: 2022
    tract_vintage: 2020
    allocation_method: tract_share_within_county
    measures:
      - family: labor_force
        outputs:
          - sae_civilian_labor_force
          - sae_unemployed_count
          - sae_unemployment_rate
```

| Field | Required | Description |
|-------|----------|-------------|
| `output_dataset` | Yes | Dataset id produced by the step and referenced by later joins. |
| `source_dataset` | Yes | ACS1 county source aggregate dataset. |
| `support_dataset` | Yes | ACS5 tract support-distribution dataset. |
| `to_geometry` | Yes | Final analysis geography. |
| `via` | Yes | Tract-to-target crosswalk transform. |
| `terminal_acs5_vintage` | Yes | ACS5 vintage used for support shares. |
| `tract_vintage` | Yes | Tract era for support and crosswalk compatibility. |
| `allocation_method` | Yes | Currently `tract_share_within_county`. |
| `measures` | Yes | SAE measure families such as `labor_force`, `rent_burden`, `owner_cost_burden`, `household_income_bins`, and `gross_rent_bins`. |

SAE medians and quintiles are derived from allocated bins. Do not request direct
weighted means of ACS1 or ACS5 median columns as SAE outputs.

### Join Step

Joins resampled datasets into a single target panel.

```yaml
- join:
    datasets: [pit, acs]
    join_on: [geo_id, year]
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `datasets` | `list[string]` | Yes | Dataset ids to join. Must reference declared datasets. |
| `join_on` | `list[string]` | No | Join keys. Default: `[geo_id, year]`. |

## Validation Policy

Override default validation thresholds. All fields are optional.

```yaml
validation:
  missing_dataset:
    default: fail       # "fail" or "warn"
    zori: warn          # per-dataset overrides
  crosswalk_coverage:
    warn_below: 0.95
    fail_below: 0.90
```

| Block | Field | Default | Description |
|-------|-------|---------|-------------|
| `missing_dataset` | `default` | `fail` | Action when a required dataset is missing. |
| `missing_dataset` | `<dataset_id>` | — | Per-dataset override. |
| `crosswalk_coverage` | `warn_below` | `0.95` | Coverage ratio warning threshold. |
| `crosswalk_coverage` | `fail_below` | `0.90` | Coverage ratio failure threshold. |

## Temporal Filters

Temporal filters adjust dataset time alignment before geographic resampling. They are declared as a top-level `filters` mapping keyed by dataset id.

```yaml
filters:
  pep:
    method: interpolate_to_month
    month: 1
  zori:
    method: point_in_time
    month: 1
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `method` | `string` | Yes | `point_in_time`, `calendar_mean`, `calendar_median`, or `interpolate_to_month`. |
| `month` | `int` | Conditional | Target month (1–12). Required for `point_in_time` and `interpolate_to_month`. |

**Method semantics:**

| Method | Description |
|--------|-------------|
| `point_in_time` | Select the observation closest to the given month. |
| `calendar_mean` | Mean across all months in each calendar year. |
| `calendar_median` | Median across all months in each calendar year. |
| `interpolate_to_month` | Linearly interpolate between adjacent annual observations to estimate the value at the target month. Useful for aligning PEP July estimates to January for PIT comparisons. |

## Referential Integrity

The schema enforces referential integrity across all ids:

- Target `id` values must be unique.
- Transform `id` values must be unique.
- Pipeline `id` values must be unique.
- Pipeline `target` must reference a declared target.
- Materialize step `transforms` must reference declared transforms.
- Resample step `dataset` must reference a declared dataset.
- Resample step `via` must reference a declared transform.
- Join step `datasets` must reference declared datasets.

Violations produce clear error messages at load time, before any execution begins.

## YAML Pitfalls

**Quoting `on`:** YAML 1.1 (used by PyYAML) treats bare `on`, `yes`, `no`, `true`, and `false` as booleans. The join step uses `join_on` instead of `on` to avoid this issue. If you introduce custom keys, be aware of this behavior — quote keys that collide with YAML reserved words.

**Quoting year ranges:** Always quote year range strings (e.g., `range: "2018-2024"`), otherwise YAML may interpret them as integers or other types.

## Versioning and Extensibility

Recipes declare a `version` key. The loader dispatches to the matching schema class (`RecipeV1`, etc.), so introducing `RecipeV2` does not break existing `version: 1` recipes.

Within a version, the schema is extended by adding new transform types or pipeline step kinds to the discriminated unions. Existing transform types and step kinds are never removed or redefined. See [[04-CLI-Reference]] for the `hhplab build recipe` command.

---

**Previous:** [[04-CLI-Reference]] | **Next:** [[06-Python-API]]
