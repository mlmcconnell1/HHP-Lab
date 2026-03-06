# Methodology: Panel Assembly

CoC Lab currently supports two panel assembly paths:

- **Imperative path:** `coclab build panel`
- **Recipe path:** `coclab build recipe` (recommended)

Both follow the same conceptual model: align heterogeneous inputs to a CoC-year frame, then join.

## Shared Assembly Pattern

1. Resolve analysis year universe.
2. Resolve dataset/year paths and effective geometries.
3. Resample to target geometry (`identity`, `aggregate`, `allocate`).
4. Join resampled datasets on common keys (typically `geo_id`, `year`).
5. Persist panel/provenance outputs when requested by runtime mode and target outputs.

## Imperative Panel Characteristics

- Inputs: PIT + ACS, optional yearly ZORI integration
- Uses policy helpers for boundary and ACS vintage alignment
- When `--build` is used, now auto-discovers build-scoped PIT and measures outputs produced by aggregate commands
- Writes panel under build-local `data/curated/panel/` when `--build` is used

## Recipe Panel Characteristics

- Uses explicit YAML declarations for datasets/transforms/pipelines
- Planner resolves dataset-year tasks deterministically
- Executor runs `materialize -> resample -> join`, then persists only when the target includes `panel` output (default)
- Current persisted panel target is canonical `data/curated/panel/...`
- Non-panel outputs declared in `targets[].outputs` are currently intent-only and emit runtime warnings
- Writes `*.manifest.json` sidecar listing consumed assets

## Quality Signals

Across both paths, users should monitor:

- `coverage_ratio` and related diagnostics fields
- boundary change indicators in panel outputs
- missingness after joins

---

**Previous:** [[11-Methodology-ZORI-Aggregation]] | **Next:** [[13-Bundle-Layout]]
