# Methodology: Panel Assembly

Panel assembly uses `coclab build recipe` to align heterogeneous inputs to a geography×year frame, then join. The target geography is CoC by default but can be metro.

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
- Current persisted panel target is the configured `output_root/`
  (`data/curated/panel/` with built-in defaults)
- Non-panel outputs declared in `targets[].outputs` are currently intent-only and emit runtime warnings
- Writes `*.manifest.json` sidecar listing consumed assets, with root-aware
  asset references for configurable storage layouts

## Metro Panel Assembly

When the target geography is metro, the recipe system adapts its behavior:

| Step | CoC | Metro |
|------|-----|-------|
| PIT source | CoC-native PIT counts | Aggregated from member CoCs via metro-CoC membership table |
| ACS5 measures | Tract→CoC crosswalk | Tract→county→metro via county membership |
| ACS1 unemployment | Not applicable | Metro-native CBSA identity resampling (optional, `--include-acs1`) |
| PEP population | County→CoC crosswalk | County→metro via county membership |
| ZORI rents | County→CoC crosswalk | County→metro via county membership |
| Boundary alignment | `period_faithful` or `retrospective` | `definition_fixed` (metros are version-pinned, not vintaged) |
| `boundary_changed` | Tracks year-over-year CoC boundary shifts | Always `False` (definition is fixed within a version) |
| Schema | `PANEL_COLUMNS` with `coc_id` | `METRO_PANEL_COLUMNS` with `metro_id`, `geo_type`, `geo_id` |

PIT coverage tracking for metro panels reports `coc_count`, `coc_expected`, `coc_coverage_ratio`, and `missing_cocs` per metro-year to surface incomplete aggregation.

### ACS 1-Year Metro Integration

ACS 1-year data provides CBSA-native unemployment rates that do not require crosswalk-based aggregation. When `include_acs1` is enabled for metro panels:

- ACS1 artifacts are loaded per-vintage and merged into the panel on `metro_id`
- The merge is optional: if no ACS1 artifact exists for a given vintage, the panel proceeds with `unemployment_rate_acs1` set to null
- `acs_products_used` tracks which ACS products contributed (`"acs5"` or `"acs5,acs1"`)

## Quality Signals

Across both paths, users should monitor:

- `coverage_ratio` and related diagnostics fields
- boundary change indicators in panel outputs
- missingness after joins

---

**Previous:** [[11-Methodology-ZORI-Aggregation]] | **Next:** [[13-Bundle-Layout]]
