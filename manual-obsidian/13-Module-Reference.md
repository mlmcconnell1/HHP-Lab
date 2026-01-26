# Module Reference

## cli/main.py

The CLI module uses [Typer](https://typer.tiangolo.com/) for command-line parsing.

**Entry Point:** `coclab`

**Commands:**
- `ingest` - Trigger data ingestion
- `list` - List datasets and artifacts
- `show` - Generate interactive maps

## ingest/hud_exchange_gis.py

Handles ingestion from HUD Exchange GIS Tools.

**Key Functions:**
- `download_hud_exchange_gdb()` - Download and extract source files
- `read_coc_boundaries()` - Parse geodatabase or shapefile
- `map_to_canonical_schema()` - Normalize field names
- `ingest_hud_exchange()` - Complete pipeline

**Field Mapping:**
| Source Fields | Canonical Field |
|---------------|-----------------|
| `COCNUM`, `COC_NUM`, `CocNum` | `coc_id` |
| `COCNAME`, `COC_NAME`, `CocName` | `coc_name` |
| `STUSAB`, `STATE`, `ST` | `state_abbrev` |

## ingest/hud_opendata_arcgis.py

Handles ingestion from HUD Open Data ArcGIS Hub.

**API Endpoint:** Continuum of Care Grantee Areas feature service

**Key Functions:**
- `_fetch_page()` - Fetch paginated data (page size: 1000)
- `_fetch_all_features()` - Handle pagination
- `_features_to_geodataframe()` - Convert GeoJSON to GeoDataFrame
- `ingest_hud_opendata()` - Complete pipeline

## geo/normalize.py

Geometry processing and normalization.

**Functions:**
| Function | Purpose |
|----------|---------|
| `normalize_crs()` | Reproject to EPSG:4326 |
| `fix_geometry()` | Apply `shapely.make_valid()` |
| `ensure_polygon_type()` | Filter to Polygon/MultiPolygon |
| `compute_geom_hash()` | SHA-256 of WKB (6 decimal precision) |
| `normalize_boundaries()` | Full pipeline |

## geo/validate.py

Data quality validation.

**Classes:**
- `ValidationResult` - Container for errors/warnings
- `ValidationIssue` - Individual issue with severity

**Validation Checks:**
- Required columns exist with correct types
- `coc_id` uniqueness within vintage
- Geometry validity (non-empty, valid type)
- Anomaly detection (tiny polygons, invalid coordinates)

## geo/io.py

GeoParquet I/O utilities.

**Functions:**
- `read_geoparquet()` - Load GeoParquet to GeoDataFrame
- `write_geoparquet()` - Save with snappy compression
- `curated_boundary_path()` - Generate canonical file paths
- `registry_path()` - Get registry file location

## registry/registry.py

Vintage tracking and version selection.

**Functions:**
- `register_vintage()` - Idempotent registration with hash checking
- `list_boundaries()` - Get all entries sorted by date
- `latest_vintage()` - Resolve current version by source policy
- `compute_file_hash()` - SHA-256 of file contents

## registry/schema.py

Data structures for registry.

**Classes:**
- `RegistryEntry` - Dataclass with serialization methods

## viz/map_folium.py

Interactive map generation with Folium.

**Features:**
- Auto-centering on CoC centroid
- Blue polygon overlay (30% opacity)
- Interactive tooltip (ID, Name, Vintage, Source)
- Auto-fitted bounds

## census/ingest/tracts.py

TIGER/Line census tract geometry ingestion.

**Functions:**
| Function | Purpose |
|----------|---------|
| `download_tiger_tracts()` | Download and parse national tract shapefiles |
| `ingest_tiger_tracts()` | Full pipeline: download, normalize, save to GeoParquet |

**Output Schema:**
- `GEOID` - 11-digit tract identifier (state + county + tract)
- `NAME` - Tract name
- `ALAND` - Land area in square meters
- `AWATER` - Water area in square meters
- `geometry` - Polygon/MultiPolygon in EPSG:4326

## census/ingest/counties.py

TIGER/Line county geometry ingestion.

**Functions:**
| Function | Purpose |
|----------|---------|
| `download_tiger_counties()` | Download and parse national county shapefiles |
| `ingest_tiger_counties()` | Full pipeline: download, normalize, save to GeoParquet |

**Output Schema:**
- `GEOID` - 5-digit county FIPS code
- `NAME` - County name
- `STATEFP` - State FIPS code
- `geometry` - Polygon/MultiPolygon in EPSG:4326

## nhgis/ingest.py

NHGIS tract shapefile ingestion via ipumspy.

**Environment Variable:**
- `IPUMS_API_KEY` - Required IPUMS API key

**Functions:**
| Function | Purpose |
|----------|---------|
| `ingest_nhgis_tracts()` | Full pipeline: submit extract, poll, download, normalize |
| `_create_extract()` | Build NHGIS extract definition for tract shapefiles |
| `_wait_for_extract()` | Poll API until extract completes |
| `_download_and_extract()` | Download and unzip completed extract |
| `_normalize_to_schema()` | Convert NHGIS schema to TIGER-compatible format |

**Supported Years:** 2010, 2020

**NHGIS Shapefile Names:**
- 2010: `us_tract_2010_tl2010`
- 2020: `us_tract_2020_tl2020`

**Output Schema:**
- `geo_vintage` - Census year as string
- `geoid` - 11-digit tract FIPS code
- `geometry` - Polygon/MultiPolygon in EPSG:4326
- `source` - Always `nhgis`
- `ingested_at` - UTC timestamp

## xwalks/tract.py

CoC-to-census-tract crosswalk builder.

**Functions:**
| Function | Purpose |
|----------|---------|
| `build_coc_tract_crosswalk()` | Compute area-weighted tract-to-CoC mappings |
| `add_population_weights()` | Add population-weighted shares to crosswalk |
| `validate_population_shares()` | Validate that pop_share sums to ~1 per CoC |
| `save_crosswalk()` | Save crosswalk to GeoParquet |

**Algorithm:**
1. Reproject both layers to ESRI:102003 (Albers Equal Area)
2. Compute geometric overlay (intersection)
3. Calculate `area_share = intersection_area / tract_area`
4. Filter to shares > 1e-9 (remove slivers)

## xwalks/county.py

CoC-to-county crosswalk builder.

**Functions:**
| Function | Purpose |
|----------|---------|
| `build_coc_county_crosswalk()` | Compute area-weighted county-to-CoC mappings |
| `save_county_crosswalk()` | Save crosswalk to GeoParquet |

## measures/acs.py

ACS 5-year estimate aggregation to CoC level.

**Constants:**
- `ACS_VARS` - Dictionary of ACS variable codes (population, income, rent, poverty)
- `ADULT_VARS` - Age-specific population variables for adults 18+

**Functions:**
| Function | Purpose |
|----------|---------|
| `fetch_acs_tract_data()` | Fetch ACS data for a single state via Census API |
| `fetch_all_states_tract_data()` | Fetch ACS data for all states (parallelized) |
| `aggregate_to_coc()` | Aggregate tract data to CoC using crosswalk weights |
| `build_coc_measures()` | Full pipeline: fetch, aggregate, save |

**Weighting Methods:**
- `area` - Weight by `area_share` (fraction of tract area in CoC)
- `population` - Weight by `pop_share` (population-proportional)

## measures/diagnostics.py

Attribution diagnostics and coverage reporting.

**Functions:**
| Function | Purpose |
|----------|---------|
| `compute_crosswalk_diagnostics()` | Per-CoC metrics: tract count, coverage ratio |
| `compute_measure_diagnostics()` | Compare area vs population weighted results |
| `summarize_diagnostics()` | Generate CLI-readable text summary |
| `identify_problem_cocs()` | Flag CoCs with low coverage or high concentration |

**Diagnostic Metrics:**
- `num_tracts` - Number of tracts intersecting CoC
- `max_tract_contribution` - Largest single-tract area share
- `coverage_ratio_area` - Sum of area_share (ideally ~1.0)
- `coverage_ratio_pop` - Sum of pop_share (when available)

## acs/ingest/tract_population.py

ACS tract-level population data ingestion from Census API.

**Functions:**
| Function | Purpose |
|----------|---------|
| `parse_acs_vintage()` | Parse ACS vintage string to API year |
| `normalize_geoid()` | Normalize tract GEOID to 11-character format |
| `fetch_state_tract_population()` | Fetch population for one state |
| `fetch_tract_population()` | Fetch population for all US states/territories |
| `ingest_tract_population()` | Full pipeline: fetch, cache, save with provenance |

**Output Schema:**
- `tract_geoid` - 11-digit Census tract GEOID
- `acs_vintage` - ACS 5-year vintage (e.g., "2019-2023")
- `tract_vintage` - Census tract vintage year
- `total_population` - Population count (B01003_001E)
- `moe_total_population` - Margin of error (B01003_001M)
- `data_source` - Always "acs_5yr"
- `source_ref` - API parameters used
- `ingested_at` - UTC timestamp

## acs/rollup.py

Tract-to-CoC population aggregation.

**Functions:**
| Function | Purpose |
|----------|---------|
| `rollup_tract_population()` | Aggregate tract population to CoC using crosswalk |
| `build_coc_population_rollup()` | Full pipeline: load, rollup, save |
| `get_tract_population_path()` | Get path to tract population parquet |
| `get_crosswalk_path()` | Get path to crosswalk parquet |
| `get_output_path()` | Generate rollup output path |

**Output Schema:**
- `coc_id` - CoC identifier
- `boundary_vintage`, `acs_vintage`, `tract_vintage` - Version identifiers
- `weighting_method` - "area" or "population_mass"
- `coc_population` - Aggregated population estimate
- `coverage_ratio` - Fraction of CoC area covered by tracts with data
- `max_tract_contribution` - Maximum single tract contribution
- `tract_count` - Number of contributing tracts

## acs/crosscheck.py

Population rollup validation against existing CoC measures.

**Classes:**
| Class | Purpose |
|-------|---------|
| `CrosscheckResult` | Validation results with error/warning counts and report DataFrame |

**Functions:**
| Function | Purpose |
|----------|---------|
| `crosscheck_population()` | Compare rollup vs measures, flag outliers |
| `run_crosscheck()` | Full pipeline: load files, validate, save report |
| `print_crosscheck_report()` | Format console output, return exit code |
| `get_rollup_path()` | Get rollup input path |
| `get_measures_path()` | Get measures input path |
| `get_crosscheck_output_path()` | Generate report output path |

**CrosscheckResult Fields:**
- `error_count` - Number of CoCs with errors
- `warning_count` - Number of CoCs with warnings
- `report_df` - DataFrame with per-CoC comparison
- `missing_in_rollup` - CoC IDs in measures but not rollup
- `missing_in_measures` - CoC IDs in rollup but not measures
- `summary` - Dict with min/median/max deltas
- `passed` - Property: True if error_count == 0

**Default Thresholds:**
- Error: `abs(pct_delta) > 0.05` or `coverage_ratio > 1.01`
- Warning: `abs(pct_delta) > 0.01` or `coverage_ratio < 0.95`

## provenance.py

Dataset provenance tracking via Parquet metadata.

**Classes:**
| Class | Purpose |
|-------|---------|
| `ProvenanceBlock` | Dataclass holding provenance fields with JSON serialization |

**Functions:**
| Function | Purpose |
|----------|---------|
| `write_parquet_with_provenance()` | Write DataFrame with embedded provenance metadata |
| `read_provenance()` | Extract provenance from Parquet file without loading data |
| `has_provenance()` | Check if a Parquet file contains provenance |

**ProvenanceBlock Fields:**
- `boundary_vintage` - CoC boundary version
- `tract_vintage` - Census tract version
- `acs_vintage` - ACS estimate end year
- `weighting` - Weighting method used
- `created_at` - ISO 8601 creation timestamp
- `coclab_version` - CoC Lab version
- `extra` - Extensible metadata dictionary

## rents/ingest.py

ZORI data download and normalization from Zillow Economic Research.

**Functions:**
| Function | Purpose |
|----------|---------|
| `download_zori()` | Download raw ZORI CSV from Zillow |
| `parse_zori_county()` | Parse county ZORI to long format |
| `parse_zori_zip()` | Parse ZIP ZORI to long format |
| `ingest_zori()` | Full pipeline: download, normalize, save with provenance |
| `get_output_path()` | Get canonical output path |

**Zillow Download URLs:**
- County: `https://files.zillowstatic.com/research/public_csvs/zori/County_zori_uc_sfrcondomfr_sm_month.csv`
- ZIP: `https://files.zillowstatic.com/research/public_csvs/zori/Zip_zori_uc_sfrcondomfr_sm_month.csv`

## rents/weights.py

County-level ACS weight computation for ZORI aggregation.

**Functions:**
| Function | Purpose |
|----------|---------|
| `parse_acs_vintage()` | Parse ACS vintage string to API year |
| `fetch_state_county_acs()` | Fetch county ACS data for one state |
| `fetch_county_acs_totals()` | Fetch county ACS data for all states |
| `build_county_weights()` | Build and cache county weights |
| `load_county_weights()` | Load cached county weights |
| `get_county_weights_path()` | Get canonical weights path |

**ACS Variables:**
| Method | Table | Variable | Description |
|--------|-------|----------|-------------|
| `renter_households` | B25003 | B25003_003E | Renter-occupied housing units |
| `housing_units` | B25001 | B25001_001E | Total housing units |
| `population` | B01003 | B01003_001E | Total population |

## rents/aggregate.py

ZORI aggregation from county to CoC geography.

**Functions:**
| Function | Purpose |
|----------|---------|
| `load_zori()` | Load normalized ZORI parquet |
| `load_crosswalk()` | Load county-CoC crosswalk |
| `load_weights()` | Load county weights |
| `compute_coc_county_weights()` | Compute combined area × demographic weights |
| `aggregate_monthly()` | Aggregate county ZORI to CoC per month |
| `collapse_to_yearly()` | Collapse monthly to yearly values |
| `aggregate_zori_to_coc()` | Full pipeline: load, aggregate, save |
| `get_coc_zori_path()` | Get canonical output path |
| `get_coc_zori_yearly_path()` | Get yearly output path |

**Yearly Collapse Methods:**
- `pit_january`: Use January value only
- `calendar_mean`: Average of 12 monthly values
- `calendar_median`: Median of 12 monthly values

## rents/diagnostics.py

ZORI coverage and quality diagnostics.

**Functions:**
| Function | Purpose |
|----------|---------|
| `compute_coc_diagnostics()` | Per-CoC metrics: coverage, dominance, month counts |
| `identify_problem_cocs()` | Flag CoCs with low coverage or high dominance |
| `generate_text_summary()` | CLI-readable text summary |
| `summarize_coc_zori()` | Combined summary and diagnostics |
| `run_zori_diagnostics()` | Full diagnostics pipeline |

**Diagnostic Metrics:**
- `months_total` - Number of months in date range
- `months_covered` - Months with ZORI data
- `coverage_ratio_mean` - Mean coverage across months
- `coverage_ratio_p10/p50/p90` - Coverage percentiles
- `max_geo_contribution_max` - Maximum single county contribution
- `flag_low_coverage` - True if coverage < threshold
- `flag_high_dominance` - True if max contribution > threshold

## pit/ingest/hud_exchange.py (Phase 3)

PIT data download from HUD Exchange.

**Functions:**
| Function | Purpose |
|----------|---------|
| `get_pit_source_url()` | Get download URL for a PIT year |
| `download_pit_data()` | Download PIT Excel file from HUD Exchange |
| `list_available_years()` | List years with known PIT data URLs |

**DownloadResult Fields:**
- `path` - Path to downloaded file
- `source_url` - Original download URL
- `downloaded_at` - Timestamp of download
- `file_size` - Size in bytes

## pit/ingest/parser.py (Phase 3)

PIT data parsing and canonicalization.

**Classes:**
| Class | Purpose |
|-------|---------|
| `PITParseResult` | Result container with DataFrame and parsing metadata |

**Functions:**
| Function | Purpose |
|----------|---------|
| `normalize_coc_id()` | Normalize CoC ID to ST-NNN format |
| `parse_pit_file()` | Parse CSV/Excel to canonical schema, returns `PITParseResult` |
| `write_pit_parquet()` | Write with embedded provenance |
| `get_canonical_output_path()` | Generate standard output path |

**PITParseResult Fields:**
- `df` - Parsed DataFrame in canonical schema
- `cross_state_mappings` - Dict of cross-state CoC ID mappings (e.g., `{"MO-604a": "MO-604"}`)
- `rows_read` - Total rows read from source file
- `rows_skipped` - Rows skipped due to invalid CoC IDs or missing data

**CoC ID Normalization:**
- Handles various formats: `CO-500`, `co-500`, `CO500`, `CO 500`
- Zero-pads short numbers: `CO-5` → `CO-005`
- Strips cross-state letter suffixes: `MO-604a` → `MO-604`
- Rejects strings longer than 7 characters (skips footnotes/non-CoC text)
- Validates US state/territory codes

**Cross-State CoC Handling:**

Some CoCs span multiple states. HUD identifies these with a letter suffix in PIT files (e.g., `MO-604a` for Kansas City metro spanning MO and KS). The parser:
1. Detects the letter suffix
2. Normalizes to the base CoC ID (`MO-604`)
3. Records the mapping in `PITParseResult.cross_state_mappings`
4. Logs an info message: `Mapping CoC ID 'MO-604a' -> 'MO-604'`
5. Embeds the mapping in Parquet provenance metadata

## pit/registry.py (Phase 3)

PIT year tracking and version management.

**Functions:**
| Function | Purpose |
|----------|---------|
| `register_pit_year()` | Register a PIT year in the registry |
| `list_pit_years()` | List all registered PIT years |
| `get_pit_path()` | Get path for a specific PIT year |
| `latest_pit_year()` | Get most recent registered year |

## pit/qa.py (Phase 3)

PIT data quality validation.

**Classes:**
| Class | Purpose |
|-------|---------|
| `Severity` | Enum: ERROR, WARNING |
| `QAIssue` | Individual validation issue |
| `QAReport` | Collection of issues with pass/fail status |

**Functions:**
| Function | Purpose |
|----------|---------|
| `check_duplicates()` | Find duplicate CoC IDs per year |
| `check_missing_cocs()` | Compare against boundary vintages |
| `check_invalid_counts()` | Detect negative/non-integer values |
| `check_yoy_changes()` | Flag large year-over-year changes |
| `validate_pit_data()` | Run all validation checks |

## panel/policies.py (Phase 3)

Panel assembly alignment policies.

**Classes:**
| Class | Purpose |
|-------|---------|
| `AlignmentPolicy` | Dataclass defining vintage alignment rules |

**Functions:**
| Function | Purpose |
|----------|---------|
| `default_boundary_vintage()` | PIT year Y → boundary vintage Y |
| `default_acs_vintage()` | PIT year Y → ACS vintage Y-1 |

**DEFAULT_POLICY:**
- `boundary_vintage_fn`: Same year as PIT
- `acs_vintage_fn`: PIT year minus 1 (ACS lag)
- `weighting_method`: `population`

## panel/assemble.py (Phase 3)

CoC × year panel construction.

**Functions:**
| Function | Purpose |
|----------|---------|
| `build_panel()` | Construct panel for year range |
| `save_panel()` | Write panel with embedded provenance |

**Panel Assembly Steps:**
1. Load PIT counts for each year
2. Apply alignment policy for boundary/ACS vintages
3. Join ACS measures using crosswalks
4. Detect boundary changes between years
5. Compute coverage ratios

## panel/diagnostics.py (Phase 3)

Panel quality and sensitivity analysis.

**Classes:**
| Class | Purpose |
|-------|---------|
| `DiagnosticsReport` | Container for all diagnostic results |

**Functions:**
| Function | Purpose |
|----------|---------|
| `coverage_summary()` | Coverage ratio stats by year |
| `boundary_change_summary()` | CoCs with boundary changes |
| `weighting_sensitivity()` | Compare area vs population weighting |
| `missingness_report()` | Missing data patterns per column |
| `generate_diagnostics_report()` | Run all diagnostics |

**DiagnosticsReport Methods:**
- `to_dict()` - JSON-compatible serialization
- `to_csv(output_dir)` - Export individual CSVs
- `summary()` - CLI-readable text output

---

**Previous:** [[12-Bundle-Layout]] | **Next:** [[14-Development]]
