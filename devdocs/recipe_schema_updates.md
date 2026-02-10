# CoC-Lab Recipe Schema Updates (Pydantic + Resolution Planner)

**Goal:** Implement incremental improvements to the recipe system:
1. Support **human-readable YAML** “decorator” step syntax (e.g. `- resample: {...}`, `- join: {...}`), normalized into canonical internal structures.
2. Add dataset **`file_set` with `segments`** for time-banded geometry + paths (e.g., ACS tracts 2010 for 2015–2019; tracts 2020 for 2020–2024).
3. Standardize join keys field to **`join_on`** (avoid YAML 1.1 boolean coercion issues like `on:`).
4. Add **`via: auto`** for resample steps to automatically select the appropriate transform per-year (based on dataset effective native geometry for that year).
5. Implement a deterministic **resolution order** / planner expansion that maps `(dataset_id, year)` → `(path, effective_geometry, params)` and chooses transforms.

This document is intended to be handed to a coding agent.

---

## 1) Canonical Concepts

### 1.1 Recipe authoring style (human readable)

Steps are expressed as single-key maps, e.g.:

```yaml
steps:
  - resample:
      dataset: acs
      method: aggregate
      to_geometry: { type: coc, vintage: 2025 }
      via: auto
      measures: [total_population, median_household_income]

  - join:
      datasets: [acs, pit]
      join_on: [geo_id, year]
```

### 1.2 Canonical internal model

Internally, normalize each step to include a discriminator `kind`:

```json
{"kind": "resample", "dataset": "acs", "method":"aggregate", ...}
{"kind": "join", "datasets":["acs","pit"], "join_on":["geo_id","year"]}
```

**Important:** The on-disk YAML need not contain `kind`, but the Pydantic models should be applied to the normalized structure.

---

## 2) Pydantic v2 Model Changes

### 2.1 YearSpec (existing)
Keep existing `YearSpec` concept (exactly one of `range` or `years`) and provide a helper to expand it to a sorted list of ints.

Required helper (in planner/normalizer module, not necessarily in models):
- `expand_year_spec(YearSpec | str | list[int]) -> list[int]`

You may already have something similar; reuse if available.

---

## 2.2 Add `file_set` to DatasetSpec

### YAML example (ACS segmented by tract vintage)

```yaml
datasets:
  acs:
    provider: census
    product: acs
    version: 1
    native_geometry: { type: tract }

    file_set:
      path_template: "coclab/data/curated/acs/acs_{year}.parquet"
      segments:
        - years: "2015-2019"
          geometry: { type: tract, vintage: 2010, source: nhgis }
          overrides:
            2017: "coclab/data/curated/acs/acs_special_2017.parquet"

        - years: "2020-2024"
          geometry: { type: tract, vintage: 2020, source: tiger }
```

### New models

Add to your schema:

```python
class FileSetSegment(BaseModel):
    years: YearSpec
    geometry: GeometryRef
    overrides: dict[int, str] = {}
    # Optional future: params: dict[str, Any] = {}

class FileSetSpec(BaseModel):
    path_template: str
    segments: list[FileSetSegment]
```

Add to `DatasetSpec`:

```python
file_set: FileSetSpec | None = None
```

### Validation rules

Implement **semantic** validation (can be in `RecipeV1.model_validator` or a separate “semantic validate” stage invoked after Pydantic validation):

1. `file_set.path_template` must contain `{year}`.
2. For each segment:
   - every `overrides` key must fall within the segment years.
   - `segment.geometry.type` must equal `dataset.native_geometry.type` (e.g., tract).
3. No overlapping segment years for a dataset:
   - if two segments both include year `y`, error.
4. (Optional) ensure segments cover all `universe.years` where dataset is referenced (warn or fail based on policy).

---

## 2.3 Join step: `join_on`

### Canonical join step fields

Update join step schema so the author-facing YAML uses `join_on`:

```yaml
- join:
    datasets: [pit, acs]
    join_on: [geo_id, year]
```

### Pydantic JoinStep

Canonical internal `JoinStep` must include:

- `datasets: list[str]`
- `join_on: list[str]`

Optional compatibility:
- Accept `on` as an alias (deprecated). This is optional since the user decided to use `join_on`.

---

## 2.4 Resample step: allow `via: auto`

### Author-facing YAML

```yaml
- resample:
    dataset: acs
    method: aggregate
    to_geometry: { type: coc, vintage: 2025 }
    via: auto
    measures: [...]
```

### Pydantic

In the internal ResampleStep, allow:

- `via: str | None` OR `via: str` where `"auto"` is permitted.

Recommended:

```python
via: str | None = None  # may be 'auto'
```

Validation:
- `method in {allocate, aggregate}` requires `via` (string), and `via` may be `"auto"`.
- `method == identity` should not set `via`.

---

## 2.5 Keep transform schema as-is, but ensure matching uses geometry vintages

Crosswalk and rollup transforms remain:

- `TransformSpec` discriminated union by `type` (`crosswalk`, `rollup`)
- Each transform has `from` and `to` GeometryRef and a `spec`.

**Important:** matching logic for `via:auto` must consider:
- geometry `type`
- geometry `vintage` (when present)
- optionally `source` if needed (usually ignore unless ambiguous)

---

## 3) YAML Normalizer (“decorator” to canonical)

### Input
Steps are single-key dicts, e.g.:

```yaml
- resample: {...}
- join: {...}
- materialize: {...}
```

### Output
Convert each step to:

- `{"kind": "<key>", **value}`

Example:

```python
{"resample": {"dataset":"acs", "via":"auto", ...}}
→ {"kind":"resample", "dataset":"acs", "via":"auto", ...}
```

Normalizer rules:

1. Each step must be a dict with exactly one key, and the key must be one of:
   - `materialize`, `resample`, `join`
2. The value must be a dict.
3. Emit a canonical dict with `kind` and merged fields.
4. Apply this normalization before Pydantic validation of `RecipeV1`.

Note: you may already have this implemented—extend it to support `join_on`.

---

## 4) Resolution Planner: Deterministic Expansion Order

### 4.1 Objective

Given:
- recipe universe years `U`
- datasets with optional `file_set` segments
- pipelines describing resample/join operations

Produce a compiled execution plan that resolves, for each dataset and year:
- path to input artifact (if applicable)
- effective native geometry (type+vintage)
- required transforms (and which one, per-year, if `via:auto`)
- intermediate artifact IDs/paths

### 4.2 Resolution Order (per pipeline)

For each `PipelineSpec`:

1. **Expand years:** Determine the pipeline year domain `Y`.
   - Default: `Y = universe.years`
   - (Optional future) allow per-step `years` override.

2. **Materialize transforms:** For each `materialize` step, ensure required crosswalk/rollup artifacts exist or are scheduled to be built.

3. **Resolve datasets per year:** For each `resample` step and each year in `Y`:
   - Resolve dataset-year inputs:
     - If dataset has `file_set`:
       - Find the **single segment** whose years include the year (error if none or multiple).
       - Resolve `path`:
         - `path = overrides.get(year) or path_template.format(year=year)`
       - Resolve effective native geometry:
         - `eff_geom = segment.geometry`
       - Effective params:
         - `eff_params = dataset.params` (+ segment params if you add later)
     - Else:
       - Adapter-specific resolution (e.g., `params.path`, `params.vintage`, registry lookup). Keep deterministic.

4. **Choose transform for resample (`via`):**
   - If `via` is a concrete transform id: use it (validate it exists).
   - If `via == "auto"`:
     - Choose exactly one transform that is compatible for this dataset-year.
     - Recommended compatibility rules:
       - For spatial crosswalk aggregation/allocation between dataset-year geometry and `to_geometry`, pick a transform whose endpoints match the required relationship.
       - At minimum: match transform’s `to` geometry to the dataset effective native geometry (type+vintage), and `from` geometry to `to_geometry` OR vice versa, depending on how your crosswalk files are defined.
     - If 0 matches: error with list of available transforms + dataset-year effective geometry.
     - If >1 matches: error (ambiguous) and list candidate transforms.

5. **Emit resample tasks:** Create a task per (dataset_id, year) producing an intermediate artifact at `to_geometry`.
   - Task metadata should include:
     - dataset_id
     - year
     - input path
     - eff native geometry
     - chosen transform id (if method != identity)
     - method (identity/allocate/aggregate)
     - measures
     - aggregation strategy (if provided; otherwise leave to adapter/measure capability resolver)

6. **Emit join tasks:** After all resample tasks for a year are defined, emit join task(s) that merge resampled dataset-year tables on `join_on`.
   - Inputs must all be at the pipeline target geometry (or a shared geometry).
   - Output is the panel for that target.

---

## 5) Edge Cases & Diagnostics

### 5.1 Segment coverage gaps
If a dataset is referenced in a pipeline for years outside its `file_set.segments`, planner should:
- fail (default), or
- warn and skip those years, depending on dataset `optional` and `validation.missing_dataset` policy.

### 5.2 YAML boolean pitfalls
- Avoid `on:` entirely. Standardize on `join_on`.
- If any legacy `on` support remains, require quoting `"on"` in YAML; but best is to remove it from docs.

### 5.3 Crosswalk versioning
When geometry vintage changes (ACS 2010 vs 2020 tracts), ensure:
- the correct crosswalk exists for each segment (two transforms), or
- `via:auto` can locate them unambiguously.

---

## 6) Required Code Deliverables

1. **Schema updates**
   - Add `FileSetSpec` + `FileSetSegment` models.
   - Add `DatasetSpec.file_set`.
   - Update join schema to use `join_on`.
   - Update resample schema to allow `via: "auto"` and validate it correctly.

2. **Normalizer updates**
   - Ensure wrapper steps normalize to `kind` form.
   - Preserve `join_on` field.

3. **Semantic validation**
   - Segment overlap detection per dataset.
   - Overrides-in-segment checks.
   - Segment geometry type match to `native_geometry.type`.
   - `path_template` must contain `{year}`.

4. **Planner resolution order**
   - Implement dataset-year resolution, segment lookup, and path expansion.
   - Implement transform auto-selection with clear error messages.
   - Emit structured plan objects (even if execution engine is separate).

---

## 7) Suggested Error Messages (make debugging pleasant)

### Segment overlap
> Dataset 'acs' file_set segments overlap on years: [2018, 2019]

### Override outside segment
> Dataset 'acs' segment years 2015-2019 has override for year 2020 (not in segment)

### via:auto no match
> Resample step for dataset 'acs' year 2017 has effective geometry tract@2010 but no compatible transform found for to_geometry coc@2025. Available transforms: [coc_to_tract_2020, coc_to_county_2023]

### via:auto ambiguous
> Resample step for dataset 'acs' year 2021 has multiple compatible transforms: [coc_to_tract_2020, coc_to_tract_2020_alt]. Specify via explicitly.

---

## 8) Acceptance Tests (minimum)

1. Recipe with `join_on` parses and validates.
2. Recipe with ACS segmented `file_set` validates:
   - no overlaps
   - overrides within range
3. Planner resolves:
   - ACS year 2017 → geometry tract@2010, uses 2010 crosswalk under `via:auto`
   - ACS year 2021 → geometry tract@2020, uses 2020 crosswalk under `via:auto`
4. Planner error cases:
   - missing segment year coverage
   - ambiguous `via:auto`

---

## Appendix A: Example Transforms for ACS Segments

```yaml
transforms:
  - id: coc_to_tract_2010
    type: crosswalk
    from: { type: coc, vintage: 2025 }
    to:   { type: tract, vintage: 2010 }
    spec:
      weighting:
        scheme: population
        population_source: acs
        population_field: total_population

  - id: coc_to_tract_2020
    type: crosswalk
    from: { type: coc, vintage: 2025 }
    to:   { type: tract, vintage: 2020 }
    spec:
      weighting:
        scheme: population
        population_source: acs
        population_field: total_population
```

---

## Appendix B: Reminder about existing CLI structure

Your CLI already distinguishes `generate xwalks`, `aggregate *`, and `build panel/export`. This recipe/planner layer should compile to those primitives rather than replacing them. fileciteturn0file0
