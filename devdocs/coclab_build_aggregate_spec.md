# CoC Lab Build + Aggregate CLI Spec (v1)

## Status
Accepted for implementation planning.

## 1. Objectives

1. Make build scope explicit and reproducible.
2. Require `--years` on build creation.
3. Separate build creation from dataset aggregation.
4. Make PIT aggregation explicit (never implicit).
5. Pin all build inputs and aggregation runs in a manifest.

## 2. Command Model

## 2.1 Build Creation

Command:

```bash
coclab build create --name <build> --years <year_spec> [--builds-dir <path>]
```

Required:

- `--name`, `-n`: build name.
- `--years`: required year list/range.

Accepted `--years` forms:

- Range: `2018-2024`
- List: `2018,2019,2020`
- Mixed: `2018-2020,2022`

Normalization:

- Parse into integer years.
- Deduplicate.
- Sort ascending.
- Reject empty result.

Hard errors:

- Missing `--years`.
- Invalid year token/range.
- Empty normalized year set.
- Missing required boundary asset for any requested year.

Behavior:

- Create build directory scaffold.
- Populate build-local `base/` with required CoC boundary assets for each requested year.
- Write/update build manifest with pinned assets (including checksums/provenance).

## 2.2 Aggregate Command Family

New command group:

```bash
coclab aggregate <dataset> --build <build> [options]
```

Datasets:

- `acs`
- `zori`
- `pep`
- `pit`

Common contract:

- `--build`, `-b` is required.
- `--years` optional; defaults to build years.
- If `--years` is provided, default rule is subset-of-build-years.

No aggregate work runs during `build create`.

## 3. Temporal Alignment

Alignment is explicit and dataset-specific (not global).

## 3.1 `coclab aggregate pep`

```bash
coclab aggregate pep --build <build> [--years ...] --align <mode>
```

Alignment modes:

- `as_of_july` (default)
- `lagged` (`--lag-months 0..12`)

## 3.2 `coclab aggregate pit`

```bash
coclab aggregate pit --build <build> [--years ...] --align <mode>
```

Alignment modes:

- `point_in_time_jan` (default)
- `to_calendar_year`
- `water_year` (reserved for later if needed)

## 3.3 `coclab aggregate acs`

```bash
coclab aggregate acs --build <build> [--years ...] --align <mode> [--acs-vintage ...]
```

Alignment modes:

- `vintage_end_year` (default)
- `window_center_year`

> `as_reported` is reserved for future implementation.

## 3.4 `coclab aggregate zori`

```bash
coclab aggregate zori --build <build> [--years ...] --align <mode>
```

Alignment modes:

- `monthly_native` (default for monthly outputs)
- `pit_january` (for PIT-aligned annual view)
- `calendar_year_average`

## 4. Build Manifest and Registry

## 4.1 Build-Local Manifest (required)

Path:

- `builds/<build>/manifest.json`

Schema v1 fields:

- `schema_version`
- `build`
  - `name`
  - `created_at`
  - `years` (normalized)
- `base_assets[]`
  - `asset_type` (`coc_boundary` now; extensible to `county_boundary`, `state_boundary`)
  - `year`
  - `source`
  - `relative_path`
  - `sha256`
- `aggregate_runs[]`
  - `run_id`
  - `dataset` (`acs|zori|pep|pit`)
  - `invoked_at`
  - `years_requested`
  - `years_materialized`
  - `alignment` (mode + params)
  - `inputs[]` (path/hash)
  - `outputs[]` (path/hash)
  - `status`
  - `error` (optional)

## 4.2 Optional Global Asset Catalog (recommended)

Possible path:

- `data/registry/base_assets.json`

Purpose:

- Inventory available base assets by year/type.
- `build create` resolves available assets, then pins chosen assets into build manifest.

## 5. Build Filesystem Layout

Canonical layout:

```text
builds/<build>/
  base/
    coc_boundary/
      <year>/...
  data/
    raw/
    curated/
  manifest.json
```

Important:

- Do **not** create `builds/<build>/hub/`.
- `base/` replaces prior hub concept as the pinned build-input root.

## 6. Validation Rules

`build create` must validate:

- required flags
- year parsing/normalization
- boundary availability for all requested years
- successful manifest write

`aggregate <dataset>` must validate:

- build exists
- years valid for build scope (unless a future explicit override flag is introduced)
- dataset-specific alignment mode/options
- required upstream inputs

## 7. Backward Compatibility Plan

1. Legacy `coclab build acs|zori|pep` passthroughs have been removed. Use `coclab aggregate acs|zori|pep` instead.
2. `coclab build panel` remains for panel assembly.
3. PIT behavior is explicit-only: no implicit PIT aggregation.

## 8. Example Workflow

```bash
coclab build create --name b2026q1 --years 2018-2024

coclab aggregate pit  --build b2026q1 --align point_in_time_jan
coclab aggregate pep  --build b2026q1 --align lagged --lag-months 6
coclab aggregate acs  --build b2026q1 --align vintage_end_year --acs-vintage 2019-2023
coclab aggregate zori --build b2026q1 --align pit_january
```

## 9. Open Items (Implementation-Level)

1. Finalize symlink vs copy policy for `base/` assets.
2. Decide whether failed aggregate attempts should be recorded in `aggregate_runs` (recommended: yes, with `status=failed`).
3. Define exact CLI exit codes for parse/validation/runtime failures.
4. Define exact semantics for mixed-year ACS vintage mapping when `--years` spans multiple analysis years.
