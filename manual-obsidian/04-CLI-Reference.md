# CLI Reference

The `coclab` command provides access to all core functionality.

## Commands Overview

```mermaid
flowchart LR
    coclab --> ingest-boundaries
    coclab --> ingest-census
    coclab --> ingest-nhgis
    coclab --> ingest-pit
    coclab --> ingest-pit-vintage
    coclab --> ingest-acs-population
    coclab --> ingest-tract-relationship
    coclab --> list-boundaries
    coclab --> check-boundaries
    coclab --> delete-boundaries
    coclab --> show
    coclab --> build-xwalks
    coclab --> aggregate-measures
    coclab --> build-panel
    coclab --> rollup-acs-population
    coclab --> crosscheck-acs-population
    coclab --> crosscheck-pit-vintages
    coclab --> crosscheck-population
    coclab --> verify-acs-population
    coclab --> diagnostics-panel
    coclab --> diagnostics-xwalk
    coclab --> diagnostics-zori
    coclab --> list-census
    coclab --> list-xwalks
    coclab --> list-measures
    coclab --> show-measures
    coclab --> compare-vintages
    coclab --> ingest-zori
    coclab --> aggregate-zori
    coclab --> source-status
    coclab --> export-bundle
    coclab --> registry-rebuild

    ingest --> |"--source hud_exchange"| HUD_EX[Download annual vintage]
    ingest --> |"--source hud_opendata"| HUD_OD[Fetch current snapshot]
    ingest-census --> TIGER[Download TIGER geometries]
    ingest-nhgis --> NHGIS[Download NHGIS tract shapefiles]
    ingest-pit --> PIT[Download & parse PIT counts]
    ingest-pit-vintage --> PITVINT[Parse all years from vintage]
    ingest-acs-population --> ACSPOP[Fetch tract population]
    ingest-tract-relationship --> TRACTREL[Download 2010↔2020 tract relationship]
    list-boundaries --> LIST[List boundary vintages]
    check-boundaries --> BOUNDCHK[Check boundary registry health]
    delete-boundaries --> BOUNDDEL[Remove boundary registry entry]
    show --> MAP[Render interactive map]
    build-xwalks --> XWALK[Create tract/county crosswalks]
    aggregate-measures --> MEAS[Aggregate demographic measures from ACS]
    build-panel --> PANEL[Assemble CoC × year panels]
    rollup-acs-population --> ROLLUP[Aggregate tract pop to CoC]
    crosscheck-acs-population --> XCHECK[Validate rollup vs measures]
    crosscheck-pit-vintages --> PITXCHECK[Compare PIT across vintages]
    crosscheck-population --> POPCHECK[Validate CoC pop vs national]
    verify-acs-population --> VERIFY[Full pipeline: ingest→rollup→check]
    ingest-zori --> ZORI_ING[Download & normalize ZORI data]
    aggregate-zori --> ZORI_AGG[Aggregate ZORI to CoC level]
    diagnostics-panel --> PDIAG[Panel quality & sensitivity]
    diagnostics-xwalk --> DIAG[Crosswalk quality checks]
    diagnostics-zori --> ZORI_DIAG[ZORI coverage diagnostics]
    list-census --> LCENSUS[List census geometry files]
    list-xwalks --> LXWALK[List crosswalk files]
    list-measures --> LMEAS[List measure files]
    show-measures --> SMEAS[Display CoC measures]
    compare-vintages --> COMP[Diff boundary vintages]
    source-status --> SRCSTAT[Display source registry status]
    export-bundle --> EXPORT[Create analysis-ready bundle]
    registry-rebuild --> REGREBUILD[Rebuild source registry from local files]
```

## `coclab aggregate-measures`

Aggregate CoC-level demographic measures from ACS 5-year estimates. Fetches tract-level data from the Census API and aggregates to CoC level using tract crosswalks.

**Measures produced:**

| Column | ACS Table | Description |
|--------|-----------|-------------|
| `total_population` | B01003 | Total population |
| `adult_population` | B01001 | Population 18+ (derived from age groups) |
| `median_household_income` | B19013 | Median household income ($) |
| `median_gross_rent` | B25064 | Median gross rent ($) |
| `population_below_poverty` | C17002 | Population below 100% poverty level |
| `poverty_universe` | C17002 | Population for whom poverty is determined |
| `coverage_ratio` | — | Fraction of CoC area with valid tract data |

```bash
# Aggregate measures with area weighting
coclab aggregate-measures --boundary 2025 --acs 2019-2023

# Use population weighting instead
coclab aggregate-measures --boundary 2025 --acs 2019-2023 --weighting population
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Latest |
| `--acs`, `-a` | ACS 5-year estimate vintage (e.g., `2019-2023`) | `2018-2022` |
| `--tracts`, `-t` | Tract vintage for crosswalk | Same as ACS end year |
| `--weighting`, `-w` | `area` or `population` | `area` |
| `--xwalk-dir` | Directory containing crosswalk files | `data/curated/xwalks` |
| `--output-dir`, `-o` | Output directory | `data/curated/measures` |

**Output:**
- `measures__A{acs}@B{boundary}.parquet`
- Summary statistics printed to console

## `coclab aggregate-zori`

Aggregate ZORI data from county geography to CoC geography using area-weighted crosswalks and ACS-based demographic weights.

```bash
# Basic aggregation with renter household weighting
coclab aggregate-zori --boundary 2025 --counties 2023 --acs 2019-2023

# With yearly output
coclab aggregate-zori -b 2025 -c 2023 --acs 2019-2023 --to-yearly

# Custom weighting method
coclab aggregate-zori -b 2025 -c 2023 --acs 2019-2023 -w housing_units
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage (e.g., `2025`) | Required |
| `--counties`, `-c` | TIGER county vintage year | Required |
| `--acs` | ACS 5-year vintage for weights (e.g., `2019-2023`) | Required |
| `--geography`, `-g` | Base geography type | `county` |
| `--zori-path` | Explicit path to ZORI parquet file | Auto-detected |
| `--xwalk-path` | Explicit crosswalk path | Inferred |
| `--weighting`, `-w` | Weighting: `renter_households`, `housing_units`, `population`, `equal` | `renter_households` |
| `--output-dir`, `-o` | Output directory | `data/curated/zori` |
| `--to-yearly` | Also emit yearly collapsed file | False |
| `--yearly-method` | `pit_january`, `calendar_mean`, `calendar_median` | `pit_january` |
| `--force`, `-f` | Recompute even if output exists | False |

**Prerequisites:**
```bash
coclab ingest-boundaries --source hud_exchange --vintage 2025
coclab ingest-census --year 2023 --type counties
coclab build-xwalks --boundary 2025 --counties 2023
coclab ingest-zori --geography county
```

**Exit Codes:**
- `0` - Success
- `2` - Missing required inputs / mismatched vintages
- `3` - Failure to compute weights (ACS missing)

**Output:**
- `data/curated/zori/zori__A{acs_end}@B{boundary}xC{counties}__w{weight_abbrev}.parquet`
- Optional yearly: `data/curated/zori/zori_yearly__A{acs_end}@B{boundary}xC{counties}__w{weight_abbrev}__m{method}.parquet`

Note: Filenames use temporal shorthand (end year of ACS vintage) and abbreviated weighting
names; legacy `coc_zori__...` names may still exist in older runs.

## `coclab build-panel`

Build analysis-ready CoC × year panels combining PIT counts with ACS measures. Optionally includes ZORI rent data for affordability analysis.

```bash
# Build panel for date range
coclab build-panel --start 2018 --end 2024

# Specify weighting method
coclab build-panel --start 2018 --end 2024 --weighting population

# Custom output path
coclab build-panel --start 2020 --end 2024 --output custom_panel.parquet

# Include ZORI rent data for rent-to-income affordability
coclab build-panel --start 2018 --end 2024 --include-zori

# Custom ZORI coverage threshold (default 0.90)
coclab build-panel --start 2018 --end 2024 --include-zori --zori-min-coverage 0.80

# Explicit ZORI data path
coclab build-panel --start 2018 --end 2024 --include-zori --zori-yearly-path data/curated/zori/zori_yearly__A2023@B2025xC2023__wrenter__mpit_january.parquet
```

| Option | Description | Default |
|--------|-------------|---------|
| `--start`, `-s` | Start year (inclusive) | Required |
| `--end`, `-e` | End year (inclusive) | Required |
| `--weighting`, `-w` | `area` or `population` | `population` |
| `--output`, `-o` | Output file path | Auto-generated |
| `--include-zori` | Include ZORI rent data and compute `rent_to_income` | `False` |
| `--no-include-zori` | Explicitly disable ZORI integration | - |
| `--zori-yearly-path` | Path to yearly ZORI Parquet file | Auto-discover |
| `--zori-min-coverage` | Minimum coverage ratio for ZORI eligibility | `0.90` |

**ZORI Integration:**

When `--include-zori` is enabled, the panel includes:

| Column | Description |
|--------|-------------|
| `zori_coc` | CoC-level ZORI rent value (yearly) |
| `zori_coverage_ratio` | Fraction of CoC covered by ZORI data |
| `zori_is_eligible` | Boolean: meets coverage threshold |
| `zori_excluded_reason` | Why excluded: `missing`, `zero_coverage`, `low_coverage` |
| `rent_to_income` | `zori_coc / (median_household_income / 12.0)` |
| `rent_metric` | Always `ZORI` (provenance) |
| `rent_alignment` | Temporal alignment method (provenance) |
| `zori_min_coverage` | Coverage threshold used (provenance) |

**Eligibility Rules:**
- CoC-year is eligible if `coverage_ratio >= zori_min_coverage`
- Ineligible rows have `zori_coc = null` and `rent_to_income = null`
- High dominance generates warnings but does NOT exclude
- Zero-coverage CoCs are excluded (never imputed)

### Analytic Universe for Rent-to-Income Measures

Analyses that use the `rent_to_income` variable **must be restricted** to CoC-year observations where `zori_is_eligible == True`.

CoC-years that fail ZORI eligibility criteria (e.g., insufficient coverage of underlying counties) have `rent_to_income` set to null and should not be included in rent-affordability inference. No imputation is performed for ineligible CoCs.

**Output:**
- Panel Parquet file with embedded provenance
- Summary statistics (years, CoC count, coverage)
- ZORI summary when enabled (eligible count, rent_to_income stats)

### Panel naming

By default, panels are written to `data/curated/panel/` using the boundary vintage:

Example:

```
data/curated/panel/panel__Y2018-2024@B2025.parquet
```

The filename does not change when `--include-zori` is enabled; the presence of ZORI
columns in the data indicates rent integration.

## `coclab check-boundaries`

Check the boundary registry for missing files, invalid paths, or temporary-directory entries.

```bash
coclab check-boundaries
```

Returns a health report. Exits with code `1` if problems are detected.

## `coclab delete-boundaries`

Remove a boundary vintage entry from the registry (does not delete the data file).

```bash
coclab delete-boundaries 2024 hud_exchange

# Skip confirmation
coclab delete-boundaries 2024 hud_exchange --yes
```

| Option | Description | Default |
|--------|-------------|---------|
| `vintage` | Boundary vintage year | Required |
| `source` | Source name (`hud_exchange`, `hud_opendata`) | Required |
| `--yes`, `-y` | Skip confirmation | False |

## `coclab build-xwalks`

Build area-weighted crosswalks linking CoC boundaries to census tracts and counties.

```bash
# Build crosswalks for a specific boundary and tract vintage
coclab build-xwalks --boundary 2025 --tracts 2023

# Also build county crosswalk
coclab build-xwalks --boundary 2025 --tracts 2023 --counties 2023
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Latest |
| `--tracts`, `-t` | Census tract vintage year | 2023 |
| `--counties`, `-c` | Census county vintage year | Same as tracts |
| `--output-dir`, `-o` | Output directory | `data/curated/xwalks` |

**Output:**
- `xwalk__B{boundary}xT{tracts}.parquet`
- `xwalk__B{boundary}xC{counties}.parquet`
- Diagnostic summary printed to console

## `coclab compare-vintages`

Compare CoC boundaries between two vintages.

```bash
# Basic comparison
coclab compare-vintages --vintage1 2024 --vintage2 2025

# Show unchanged CoCs too
coclab compare-vintages -v1 2024 -v2 2025 --show-unchanged

# Save diff to CSV
coclab compare-vintages -v1 2024 -v2 2025 -o diff_report.csv
```

| Option | Description | Default |
|--------|-------------|---------|
| `--vintage1`, `-v1` | First (older) vintage | Required |
| `--vintage2`, `-v2` | Second (newer) vintage | Required |
| `--show-unchanged` | Also list unchanged CoCs | False |
| `--output`, `-o` | Save diff to CSV | None |

**Output:**
- Summary counts of added, removed, changed, unchanged CoCs
- Lists of affected CoC IDs by category

## `coclab crosscheck-acs-population`

Cross-check population rollup against existing CoC measures (`total_population` from `coc_measures`).

```bash
# Basic crosscheck
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area

# With custom thresholds
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area \
    --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Required |
| `--acs`, `-a` | ACS 5-year vintage | Required |
| `--tracts`, `-t` | Census tract vintage year | Required |
| `--weighting`, `-w` | `area` or `population_mass` | `area` |
| `--warn-pct` | Warning threshold for percent delta | 0.01 (1%) |
| `--error-pct` | Error threshold for percent delta | 0.05 (5%) |
| `--min-coverage` | Minimum coverage ratio | 0.95 |

**Exit Codes:**
- `0` - No errors (warnings allowed)
- `2` - Errors found (threshold exceeded)

**Output:**
- Console report with top 25 worst deltas
- `data/curated/acs/acs_population_crosscheck__{boundary}__{acs}__{tracts}__{weighting}.parquet`

## `coclab crosscheck-population`

Cross-check population totals from crosswalk aggregation against ACS national totals. Validates that CoC-aggregated population approximately equals the national ACS total, helping identify crosswalk coverage issues, double-counting, or data quality problems.

```bash
# Basic crosscheck (auto-detects latest vintages)
coclab crosscheck-population

# Specify vintages
coclab crosscheck-population --boundary 2025 --acs 2019-2023

# Show state-level breakdown
coclab crosscheck-population --by-state

# Adjust warning threshold
coclab crosscheck-population --warn-threshold 0.10
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Latest |
| `--acs`, `-a` | ACS 5-year vintage | Latest |
| `--tracts`, `-t` | Census tract vintage | ACS end year |
| `--xwalk-dir` | Directory containing crosswalk files | `data/curated/xwalks` |
| `--acs-dir` | Directory containing ACS tract files | `data/curated/acs` |
| `--by-state`, `-s` | Show detailed state-level comparison | False |
| `--warn-threshold`, `-w` | Warning threshold for ratio deviation from 1.0 | 0.05 (5%) |

**Diagnostics Computed:**
1. **National total** - Sum of all tract populations from ACS
2. **Crosswalk statistics** - Tract counts, CoC counts, coverage gaps
3. **CoC-aggregated total** - `sum(tract_pop × area_share)` across all CoCs
4. **Ratio validation** - CoC/National ratio with pass/fail status
5. **Area_share validation** - Detects overlaps (>1.01) and partial coverage (<0.99)
6. **State-level comparison** (with `--by-state`) - Identifies states with coverage issues

**Exit Codes:**
- `0` - Ratio within threshold
- `1` - Ratio exceeds threshold (potential data issue)

**Example Output:**
```
POPULATION CROSSWALK SANITY CHECK
======================================================================
1. NATIONAL TOTAL (sum of all tracts): 335,559,225

2. CROSSWALK STATISTICS:
   Unique tracts in crosswalk:    85,275
   Unique CoCs:                   387

3. COC-AGGREGATED TOTAL: 330,903,210
   Ratio (CoC/National):     0.9861
   Status: OK (within 5% threshold)

4. AREA_SHARE VALIDATION:
   Tracts with sum > 1.01 (potential overlap): 0
   Tracts with sum < 0.99 (partial coverage):  4,696
```

## `coclab crosscheck-pit-vintages`

Compare PIT counts between two vintage releases to detect historical data revisions. This helps identify when HUD has revised historical PIT data between releases (e.g., due to CoC mergers or data corrections).

```bash
# Compare 2023 and 2024 vintages
coclab crosscheck-pit-vintages --vintage1 2023 --vintage2 2024

# Filter to a specific year
coclab crosscheck-pit-vintages -v1 2023 -v2 2024 --year 2020

# Save detailed comparison to CSV
coclab crosscheck-pit-vintages -v1 2023 -v2 2024 -o comparison.csv

# Show unchanged records too
coclab crosscheck-pit-vintages -v1 2023 -v2 2024 --show-unchanged
```

| Option | Description | Default |
|--------|-------------|---------|
| `--vintage1`, `-v1` | First (older) vintage to compare | Required |
| `--vintage2`, `-v2` | Second (newer) vintage to compare | Required |
| `--year`, `-y` | Filter to specific PIT year | All common years |
| `--output`, `-o` | Save detailed comparison to CSV | None |
| `--show-unchanged` | Include unchanged records in output | False |

**Output:**
- **Tab Totals**: Year-by-year comparison of all-CoC totals for total, sheltered, and unsheltered counts
- **Summary**: Counts of added, removed, changed, and unchanged CoC-year records
- **Changed**: CoCs with revised counts (shows delta values)
- **Added**: CoC-years present in v2 but not v1
- **Removed**: CoC-years present in v1 but not v2 (often due to mergers)

**Interpreting Results:**

If tab totals match but individual CoCs differ, the changes are likely due to CoC reorganizations (mergers) rather than data corrections. For example, if MA-519 was merged into MA-505:
- MA-505 will show as "changed" with increased counts
- MA-519 will show as "removed"
- Tab totals will remain identical

## `coclab diagnostics-panel`

Run diagnostics and sensitivity checks on panel files.

```bash
# Run diagnostics on a panel
coclab diagnostics-panel --panel data/curated/panels/coc_panel__2018_2024.parquet

# Export diagnostics to CSV files
coclab diagnostics-panel --panel panel.parquet --output-dir ./diagnostics/ --format csv

# Print text summary only
coclab diagnostics-panel --panel panel.parquet --format text
```

| Option | Description | Default |
|--------|-------------|---------|
| `--panel`, `-p` | Path to panel Parquet file | Required |
| `--output-dir`, `-o` | Directory for CSV exports | None |
| `--format`, `-f` | `text` or `csv` | `text` |

**Diagnostics Included:**
- Coverage ratio distribution over time
- Boundary change flags by CoC/year
- Missingness summaries per column
- Panel structure validation

## `coclab diagnostics-xwalk`

Run crosswalk quality diagnostics.

```bash
# Basic diagnostics
coclab diagnostics-xwalk --crosswalk data/curated/xwalks/xwalk__B2025xT2023.parquet

# Show problem CoCs
coclab diagnostics-xwalk -x crosswalk.parquet --show-problems

# Custom thresholds and CSV export
coclab diagnostics-xwalk -x crosswalk.parquet --coverage-threshold 0.90 -o diagnostics.csv
```

| Option | Description | Default |
|--------|-------------|---------|
| `--crosswalk`, `-x` | Path to crosswalk parquet file | Required |
| `--coverage-threshold` | Coverage threshold for flagging | 0.95 |
| `--max-contribution` | Max tract contribution threshold | 0.8 |
| `--show-problems` | Show problem CoCs | False |
| `--output`, `-o` | Save diagnostics to CSV | None |

## `coclab diagnostics-zori`

Summarize CoC ZORI coverage, missingness, and quality metrics.

```bash
# Run diagnostics on CoC ZORI file
coclab diagnostics-zori --coc-zori data/curated/zori/coc_zori__county__b2025.parquet

# Save diagnostics to file
coclab diagnostics-zori --coc-zori coc_zori.parquet --output diagnostics.csv

# Custom thresholds
coclab diagnostics-zori --coc-zori coc_zori.parquet --coverage-threshold 0.85
```

| Option | Description | Default |
|--------|-------------|---------|
| `--coc-zori` | Path to CoC-level ZORI parquet file | Required |
| `--output`, `-o` | Save diagnostics to CSV or parquet | None |
| `--coverage-threshold` | Threshold for flagging low coverage | 0.90 |
| `--dominance-threshold` | Threshold for flagging high dominance | 0.80 |

**Output:**
- Console summary with coverage statistics
- Per-CoC diagnostic flags (low coverage, high dominance)
- Optional CSV/parquet export

## `coclab export-bundle`

Export an analysis-ready bundle with MANIFEST.json for downstream analysis repositories.

```bash
# Basic export with default options
coclab export-bundle --name my_analysis --panel data/curated/panels/coc_panel.parquet

# Include inputs and use specific vintages
coclab export-bundle --name replication --include panel,manifest,codebook,inputs \
  --boundary-vintage 2025 --years 2011-2024

# Create compressed archive
coclab export-bundle --name archive --compress
```

| Option | Description | Default |
|--------|-------------|---------|
| `--name`, `-n` | Logical bundle name for metadata and documentation | Required |
| `--out-dir`, `-o` | Output directory where export-N folders are created | `exports` |
| `--panel`, `-p` | Explicit panel parquet path (inferred from curated if omitted) | Auto-infer |
| `--include`, `-i` | Components to include (comma-separated) | `panel,manifest,codebook,diagnostics` |
| `--boundary-vintage` | Boundary vintage (e.g., 2025) | None |
| `--tracts-vintage` | Census tracts vintage (e.g., 2023) | None |
| `--counties-vintage` | Counties vintage (e.g., 2023) | None |
| `--acs-vintage` | ACS vintage (e.g., 2019-2023) | None |
| `--years` | Year range (e.g., 2011-2024) | None |
| `--copy-mode` | File copy mode: `copy`, `hardlink`, or `symlink` | `copy` |
| `--compress` | Create .tar.gz archive of the bundle | `False` |
| `--force`, `-f` | Create bundle even if identical manifest exists | `False` |

**Include Components:**

| Component | Description |
|-----------|-------------|
| `panel` | Primary panel parquet file(s) |
| `inputs` | Boundaries, crosswalks, raw curated sources required to regenerate |
| `derived` | Derived intermediate artifacts beyond the panel |
| `diagnostics` | Diagnostic outputs |
| `codebook` | Variable descriptions and schema documentation |
| `manifest` | Always created; this flag controls whether it is also copied to `provenance/` |

**Output:**
- New export folder created: `{out-dir}/export-{n}/`
- Each invocation creates a new numbered folder (export-1, export-2, ...) to preserve prior bundles
- Summary of files exported by role and total size

**Exit Codes:**

| Code | Meaning |
|------|---------|
| `0` | Success |
| `2` | Validation failure (missing panel, incompatible vintages, unreadable files) |
| `3` | Filesystem failure (cannot create export directory, copy failure) |
| `4` | Manifest failure (hashing/metadata extraction failure) |

## `coclab ingest-acs-population`

Ingest tract-level population data from ACS 5-year estimates (Census API table B01003).

```bash
# Ingest tract population for ACS 2019-2023 using 2023 tract geometries
coclab ingest-acs-population --acs 2019-2023 --tracts 2023

# Force re-fetch even if cached file exists
coclab ingest-acs-population --acs 2019-2023 --tracts 2023 --force
```

| Option | Description | Default |
|--------|-------------|---------|
| `--acs`, `-a` | ACS 5-year vintage (e.g., `2019-2023` or `2023`) | Required |
| `--tracts`, `-t` | Census tract vintage year | Required |
| `--force` | Re-fetch even if cached file exists | False |

**Output:**
- `data/curated/acs/tract_population__{acs}__{tracts}.parquet`
- Contains: tract_geoid, acs_vintage, tract_vintage, total_population, moe_total_population, data_source, source_ref, ingested_at

## `coclab ingest-tract-relationship`

Download the Census Bureau tract-to-tract relationship file (2010↔2020). This
file is required to translate ACS data from 2010 tract geography to 2020 tract
geography.

```bash
coclab ingest-tract-relationship

# Force a re-download even if cached
coclab ingest-tract-relationship --force
```

| Option | Description | Default |
|--------|-------------|---------|
| `--force`, `-f` | Re-download even if file exists | False |

## `coclab ingest-boundaries`

Ingest CoC boundary data from HUD sources.

**From HUD Exchange (annual vintages):**
```bash
coclab ingest-boundaries --source hud_exchange --vintage 2025
```

**From HUD Open Data (current snapshot):**
```bash
coclab ingest-boundaries --source hud_opendata --snapshot latest
```

| Option       | Description                                    | Default                     |
| ------------ | ---------------------------------------------- | --------------------------- |
| `--source`   | Data source (`hud_exchange` or `hud_opendata`) | Required                    |
| `--vintage`  | Year for HUD Exchange data                     | Required for `hud_exchange` |
| `--snapshot` | Snapshot tag for Open Data                     | `latest`                    |
| `--force`    | Re-ingest even if vintage already exists       | False                       |

## `coclab ingest-census`

Download TIGER census geometries (tracts and/or counties).

```bash
# Download both tracts and counties for 2023
coclab ingest-census --year 2023

# Download only tracts
coclab ingest-census --year 2023 --type tracts

# Force re-download even if files exist
coclab ingest-census --year 2023 --force
```

| Option | Description | Default |
|--------|-------------|---------|
| `--year`, `-y` | TIGER vintage year | 2023 |
| `--type`, `-t` | `tracts`, `counties`, or `all` | `all` |
| `--force` | Re-download even if file exists | False |

## `coclab ingest-nhgis`

Download census tract shapefiles from NHGIS (National Historical Geographic Information System) via the IPUMS API. This is especially useful for 2010 tracts, which TIGER distributes as 3,000+ county-level files that must be downloaded individually.

**Environment Variable:**

| Variable        | Description                                                                 |
| --------------- | --------------------------------------------------------------------------- |
| `IPUMS_API_KEY` | Required. Your IPUMS API key. Get one at https://account.ipums.org/api_keys |

```bash
# Download 2010 and 2020 tracts
coclab ingest-nhgis --year 2010 --year 2020

# Custom poll interval (check every 5 minutes instead of 2)
coclab ingest-nhgis --year 2010 --poll-interval 5

# Pass API key directly (not recommended for scripts)
coclab ingest-nhgis --year 2020 --api-key your_key_here

# Force re-download even if file exists
coclab ingest-nhgis --year 2010 --force
```

| Option | Description | Default |
|--------|-------------|---------|
| `--year`, `-y` | Census year(s) to download (2010, 2020). Can specify multiple. | Required |
| `--api-key` | IPUMS API key. Prefer using `IPUMS_API_KEY` env var. | From env |
| `--poll-interval` | Minutes between status checks while waiting | 2 |
| `--max-wait` | Maximum minutes to wait for extract completion | 60 |
| `--force` | Re-download even if file already exists | False |

**Supported Years:** 2010, 2020

**Workflow:**
1. Submits an extract request to NHGIS via the IPUMS API
2. Polls the API at the specified interval until the extract is ready
3. Downloads and extracts the shapefile
4. Normalizes to the standard tract schema (matching TIGER output)
5. Saves as GeoParquet and registers in source registry

**Output:**
- `data/curated/census/tracts__T{year}.parquet`
- Same schema as TIGER tracts: `geo_vintage`, `geoid`, `geometry`, `source`, `ingested_at`

**Why NHGIS over TIGER?**

For 2010 tracts, Census TIGER distributes data as county-level files (one per county, ~3,200 files). NHGIS provides pre-assembled national shapefiles, making ingestion much simpler. For 2020+ tracts, TIGER provides state-level files which are more manageable, but NHGIS remains a convenient single-file alternative.

## `coclab ingest-pit`

Download and parse PIT (Point-in-Time) count data from HUD Exchange.

```bash
# Ingest PIT data for a specific year
coclab ingest-pit --year 2024

# Force re-download even if file exists
coclab ingest-pit --year 2024 --force

# Parse only (skip download if file exists)
coclab ingest-pit --year 2024 --parse-only
```

| Option | Description | Default |
|--------|-------------|---------|
| `--year`, `-y` | PIT count year to ingest | Required |
| `--force` | Re-download even if file exists | False |
| `--parse-only` | Skip download, parse existing file | False |

**Workflow:**
1. Downloads PIT Excel file from HUD Exchange
2. Parses to canonical schema (coc_id, pit_total, pit_sheltered, pit_unsheltered)
3. Writes Parquet with embedded provenance
4. Registers in PIT registry
5. Runs QA validation checks

## `coclab ingest-pit-vintage`

Ingest **all years** from a PIT vintage file. Unlike `ingest-pit` which extracts only a single year, this command parses every year tab from the HUD Excel file (e.g., 2007-2024 from the 2024 release).

This is useful for detecting when HUD revises historical PIT data between releases.

```bash
# Ingest all years from the 2024 vintage
coclab ingest-pit-vintage --vintage 2024

# Force re-download
coclab ingest-pit-vintage --vintage 2024 --force

# Parse existing file only
coclab ingest-pit-vintage --vintage 2024 --parse-only
```

| Option | Description | Default |
|--------|-------------|---------|
| `--vintage`, `-v` | Vintage/release year to ingest | Required |
| `--force` | Re-download even if file exists | False |
| `--parse-only` | Skip download, parse existing file | False |

**Output:**
- `data/curated/pit/pit_vintage__{vintage}.parquet` containing all years
- Registered in PIT vintage registry

## `coclab ingest-zori`

Download and normalize ZORI (Zillow Observed Rent Index) data from Zillow Economic Research.

```bash
# Ingest county-level ZORI data
coclab ingest-zori --geography county

# Force re-download even if cached
coclab ingest-zori --geography county --force

# Filter to specific date range
coclab ingest-zori --geography county --start 2020-01-01 --end 2024-12-31
```

| Option | Description | Default |
|--------|-------------|---------|
| `--geography`, `-g` | Geography level: `county` or `zip` | `county` |
| `--url` | Override download URL | Auto-detected |
| `--force`, `-f` | Re-download and reprocess even if cached | False |
| `--output-dir`, `-o` | Output directory for curated parquet | `data/curated/zori` |
| `--raw-dir` | Directory for raw downloads | `data/raw/zori` |
| `--start` | Filter to dates >= start (YYYY-MM-DD) | None |
| `--end` | Filter to dates <= end (YYYY-MM-DD) | None |

**Exit Codes:**
- `0` - Success
- `2` - Validation/parse error
- `3` - Download error

**Output:**
- `data/curated/zori/zori__{geography}.parquet`

## `coclab list-boundaries`

List all available boundary vintages in the registry.

```bash
coclab list-boundaries
```

**Example Output:**
```
Available boundary vintages:

Vintage                        Source                    Features   Ingested At
-------------------------------------------------------------------------------------
2025                           hud_exchange_gis_tools    400        2025-01-15 14:30
HUDOpenData_2025-01-10         hud_opendata_arcgis       402        2025-01-10 09:15
```

## `coclab list-census`

List available TIGER census geometry files (tracts and counties).

```bash
# List all census geometry files
coclab list-census

# List only tract files
coclab list-census --type tracts

# List only county files
coclab list-census --type counties
```

| Option | Description | Default |
|--------|-------------|---------|
| `--type`, `-t` | Filter by type: `tracts` or `counties` | All |
| `--dir`, `-d` | Directory to scan | `data/curated/census` |

**Example Output:**
```
Available census geometry files:

Type         Year             Rows         Size Modified
-----------------------------------------------------------------
counties     2022            3,235     119.7 MB 2025-01-06 07:33
counties     2023            3,235     119.5 MB 2025-01-05 18:10
tracts       2022           85,529     619.9 MB 2025-01-06 07:33
tracts       2023           85,529     620.8 MB 2025-01-05 18:18

Total: 4 census file(s)
```

## `coclab list-measures`

List available CoC measure files.

```bash
coclab list-measures
```

| Option | Description | Default |
|--------|-------------|---------|
| `--dir`, `-d` | Directory to scan | `data/curated/measures` |

## `coclab list-xwalks`

List available crosswalk files.

```bash
# List all crosswalks
coclab list-xwalks

# List only tract crosswalks
coclab list-xwalks --type tract
```

| Option | Description | Default |
|--------|-------------|---------|
| `--type`, `-t` | `tract`, `county`, or `all` | `all` |
| `--dir`, `-d` | Directory to scan | `data/curated/xwalks` |

## `coclab registry-rebuild`

Rebuild the source registry by checking local files for missing entries or hash
mismatches. This is useful if files were moved or updated outside the normal
ingest commands.

```bash
coclab registry-rebuild

# Preview changes without writing
coclab registry-rebuild --dry-run
```

| Option | Description | Default |
|--------|-------------|---------|
| `--dry-run`, `-n` | Preview changes without modifying registry | False |
| `--registry`, `-r` | Path to source registry file | `data/curated/source_registry.parquet` |

## `coclab rollup-acs-population`

Build CoC population rollup by aggregating tract population to CoC using existing crosswalks.

```bash
# Build rollup with area weighting
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area

# Use population_mass weighting
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting population_mass

# Force rebuild even if cached
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area --force
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Required |
| `--acs`, `-a` | ACS 5-year vintage | Required |
| `--tracts`, `-t` | Census tract vintage year | Required |
| `--weighting`, `-w` | `area` or `population_mass` | `area` |
| `--force` | Rebuild even if cached file exists | False |

**Output:**
- `data/curated/acs/coc_population_rollup__{boundary}__{acs}__{tracts}__{weighting}.parquet`
- Contains: coc_id, boundary_vintage, acs_vintage, tract_vintage, weighting_method, coc_population, coverage_ratio, max_tract_contribution, tract_count

## `coclab show`

Render an interactive map for a specific CoC boundary.

```bash
# Show using latest vintage
coclab show --coc CO-500

# Specify a vintage
coclab show --coc CO-500 --vintage 2025

# Custom output path
coclab show --coc NY-600 --output my_map.html
```

| Option | Description | Default |
|--------|-------------|---------|
| `--coc` | CoC identifier (e.g., `CO-500`) | Required |
| `--vintage` | Boundary vintage to use | Latest |
| `--output` | Output HTML file path | Auto-generated |

## `coclab show-measures`

Display computed measures for a specific CoC.

```bash
# Show measures (auto-detect latest files)
coclab show-measures --coc CO-500

# Specify vintages
coclab show-measures --coc CO-500 --boundary 2025 --acs 2022

# Output as JSON
coclab show-measures --coc NY-600 --format json
```

| Option | Description | Default |
|--------|-------------|---------|
| `--coc`, `-c` | CoC identifier | Required |
| `--boundary`, `-b` | Boundary vintage | Auto-detect |
| `--acs`, `-a` | ACS vintage year | Auto-detect |
| `--format`, `-f` | `table`, `json`, or `csv` | `table` |

## `coclab source-status`

Display status of tracked external data sources. The source registry tracks all ingested external data (ZORI, boundaries, census, etc.) with SHA-256 hashes to detect upstream changes.

```bash
# Show full registry summary
coclab source-status

# Check for upstream data changes
coclab source-status --check-changes

# Filter by source type
coclab source-status --type zori
```

| Option | Description | Default |
|--------|-------------|---------|
| `--type`, `-t` | Filter to source type (`zori`, `boundary`, `census_tract`, etc.) | All |
| `--check-changes`, `-c` | Highlight sources with multiple different hashes | `False` |

**Source Types Tracked:**
- `zori` - Zillow ZORI rent data
- `boundary` - HUD CoC boundaries
- `census_tract` - TIGER tract geometries
- `census_county` - TIGER county geometries
- `nhgis_tract` - NHGIS tract geometries
- `acs_tract` - ACS tract-level data
- `acs_county` - ACS county-level data
- `pit` - HUD PIT counts

**Change Detection:**

When `--check-changes` is used, the command identifies sources where the upstream data has changed between ingestions (different SHA-256 hashes). This helps detect silent updates to external data sources.

## `coclab verify-acs-population`

One-shot command that runs: ingest → rollup → crosscheck.

```bash
# Full verification pipeline
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area

# With custom thresholds and force rebuild
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area \
    --force --warn-pct 0.02 --error-pct 0.10 --min-coverage 0.90
```

| Option | Description | Default |
|--------|-------------|---------|
| `--boundary`, `-b` | CoC boundary vintage | Required |
| `--acs`, `-a` | ACS 5-year vintage | Required |
| `--tracts`, `-t` | Census tract vintage year | Required |
| `--weighting`, `-w` | `area` or `population_mass` | `area` |
| `--force` | Force re-ingest and rebuild all artifacts | False |
| `--warn-pct` | Warning threshold for percent delta | 0.01 (1%) |
| `--error-pct` | Error threshold for percent delta | 0.05 (5%) |
| `--min-coverage` | Minimum coverage ratio | 0.95 |

**Exit Codes:**
- `0` - No errors (warnings allowed)
- `2` - Errors found (threshold exceeded)

---

**Previous:** [[03-Architecture]] | **Next:** [[05-Python-API]]
