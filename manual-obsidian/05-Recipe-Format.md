# Recipe Format

Recipes are declarative YAML files that describe a complete build: which datasets to pull, what geometry to target, how to resample and join data, and what outputs to produce. The recipe system separates **structural** validation (schema shape, types, referential integrity) from **semantic** validation (whether adapters exist for referenced geometries and datasets).

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
coclab build recipe-preflight --recipe recipes/demo.yaml --json

# Optional: inspect resolved tasks while authoring/debugging
coclab build recipe-plan --recipe recipes/demo.yaml --json

# Execute
coclab build recipe --recipe recipes/demo.yaml
```

`coclab build recipe` is the normal entrypoint. Use `--dry-run` when you want
the same validation/preflight path without execution.

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

```yaml
targets:
  - id: coc_panel
    geometry: { type: coc, vintage: 2025, source: hud_exchange }
    outputs: [panel, diagnostics]
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
| `path` | `string` | No | Optional project-relative path to a pre-materialized dataset file. Must be relative (not absolute). |
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

## Transforms

Spatial transform operators that define how data moves between geometries. Each transform has a unique `id` and a `type` that selects the operator.

### Crosswalk Transform

Builds area- or population-weighted crosswalk shares between two geometries.

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
| `scheme` | `area` or `population` | Yes | Weighting method for crosswalk shares. |
| `population_source` | `string` | No | Dataset id for population weights (when `scheme: population`). |
| `population_field` | `string` | No | Field name in the population dataset. |

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

Within a version, the schema is extended by adding new transform types or pipeline step kinds to the discriminated unions. Existing transform types and step kinds are never removed or redefined. See [[04-CLI-Reference]] for the `coclab build recipe` command.

---

**Previous:** [[04-CLI-Reference]] | **Next:** [[06-Python-API]]
