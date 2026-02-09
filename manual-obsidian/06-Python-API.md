# Python API

## Quick Start

```python
from coclab.ingest import ingest_hud_exchange, ingest_hud_opendata
from coclab.registry import latest_vintage, list_boundaries
from coclab.viz import render_coc_map

# Ingest a vintage
output_path = ingest_hud_exchange("2025")

# Get the latest vintage
vintage = latest_vintage()

# List all vintages
for entry in list_boundaries():
    print(f"{entry.boundary_vintage}: {entry.feature_count} features")

# Render a map
map_path = render_coc_map("CO-500", vintage="2025")
print(f"Map saved to: {map_path}")
```

## API Reference

### Ingestion Functions

```python
# HUD Exchange GIS Tools (annual vintages)
from coclab.ingest import ingest_hud_exchange

path = ingest_hud_exchange(
    boundary_vintage: str,  # e.g., "2025"
    url: str | None = None,  # Custom download URL
    download_dir: Path | None = None  # Custom download directory
) -> Path  # Returns path to curated GeoParquet

# HUD Open Data ArcGIS (live snapshots)
from coclab.ingest import ingest_hud_opendata

path = ingest_hud_opendata(
    snapshot_tag: str = "latest"  # Snapshot identifier
) -> Path  # Returns path to curated GeoParquet
```

### Registry Functions

```python
from coclab.registry import (
    register_vintage,
    list_boundaries,
    latest_vintage,
    RegistryEntry
)

# Register a new vintage
register_vintage(
    vintage: str,
    path: Path,
    source: str,
    feature_count: int,
    hash_of_file: str | None = None,
    ingested_at: datetime | None = None
) -> None

# List all vintages (sorted by ingested_at descending)
entries: list[RegistryEntry] = list_boundaries()

# Get latest vintage string
vintage: str = latest_vintage(source: str | None = None)
```

### Visualization Functions

```python
from coclab.viz import render_coc_map

html_path = render_coc_map(
    coc_id: str,           # e.g., "CO-500"
    vintage: str | None = None,  # Uses latest if None
    out_html: Path | None = None  # Custom output path
) -> Path  # Returns path to generated HTML
```

### Geo Processing Functions

```python
from coclab.geo import normalize_boundaries, validate_boundaries
import geopandas as gpd

# Normalize a GeoDataFrame
gdf_normalized = normalize_boundaries(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame

# Validate boundaries (returns ValidationResult)
result = validate_boundaries(gdf: gpd.GeoDataFrame) -> ValidationResult
print(result.errors)    # List of error messages
print(result.warnings)  # List of warning messages
print(result.is_valid)  # True if no errors
```

### Census Geometry Functions

```python
from coclab.census.ingest import (
    download_tiger_tracts,
    download_tiger_counties,
    ingest_tiger_tracts,
    ingest_tiger_counties,
)

# Download and normalize tract geometries
tracts_gdf = download_tiger_tracts(year: int = 2023) -> gpd.GeoDataFrame

# Download, normalize, and save to parquet
output_path = ingest_tiger_tracts(year: int = 2023) -> Path

# Same for counties
counties_gdf = download_tiger_counties(year: int = 2023) -> gpd.GeoDataFrame
output_path = ingest_tiger_counties(year: int = 2023) -> Path
```

### Crosswalk Functions

```python
from coclab.xwalks import (
    build_coc_tract_crosswalk,
    build_coc_county_crosswalk,
    add_population_weights,
    validate_population_shares,
    save_crosswalk,
)

# Build area-weighted tract crosswalk
crosswalk = build_coc_tract_crosswalk(
    coc_gdf: gpd.GeoDataFrame,      # CoC boundaries with 'coc_id' column
    tract_gdf: gpd.GeoDataFrame,    # Tract geometries with 'GEOID' column
    boundary_vintage: str,           # e.g., "2025"
    tract_vintage: str,              # e.g., "2023"
) -> pd.DataFrame

# Add population weights to crosswalk
crosswalk_with_pop = add_population_weights(
    crosswalk: pd.DataFrame,
    population_data: pd.DataFrame,   # Must have 'GEOID' and 'total_population'
) -> pd.DataFrame

# Validate population shares sum to ~1 per CoC
validation = validate_population_shares(crosswalk: pd.DataFrame) -> pd.DataFrame

# Save crosswalk to parquet
output_path = save_crosswalk(
    crosswalk: pd.DataFrame,
    boundary_vintage: str,
    tract_vintage: str,
    output_dir: Path = Path("data/curated/xwalks"),
) -> Path
```

### ACS Measure Functions

```python
from coclab.measures import (
    fetch_acs_tract_data,
    fetch_all_states_tract_data,
    aggregate_to_coc,
    build_coc_measures,
)

# Fetch ACS data for a single state
tract_data = fetch_acs_tract_data(
    year: int,           # ACS 5-year estimate end year
    state_fips: str,     # e.g., "06" for California
) -> pd.DataFrame

# Fetch for all states
all_tract_data = fetch_all_states_tract_data(year: int) -> pd.DataFrame

# Aggregate tract data to CoC level
coc_measures = aggregate_to_coc(
    acs_data: pd.DataFrame,
    crosswalk: pd.DataFrame,
    weighting: Literal["area", "population"] = "area",
) -> pd.DataFrame

# Full pipeline: fetch, aggregate, save
coc_measures = build_coc_measures(
    boundary_vintage: str,
    acs_vintage: int,
    crosswalk_path: Path,
    weighting: Literal["area", "population"] = "area",
    output_dir: Path | None = None,
) -> pd.DataFrame
```

### Diagnostics Functions

```python
from coclab.measures import (
    compute_crosswalk_diagnostics,
    compute_measure_diagnostics,
    summarize_diagnostics,
    identify_problem_cocs,
)

# Compute per-CoC crosswalk quality metrics
diagnostics = compute_crosswalk_diagnostics(crosswalk: pd.DataFrame) -> pd.DataFrame
# Returns: coc_id, num_tracts, max_tract_contribution, coverage_ratio_area, coverage_ratio_pop

# Compare area vs population weighted measures
comparison = compute_measure_diagnostics(
    area_measures: pd.DataFrame,
    pop_measures: pd.DataFrame,
) -> pd.DataFrame

# Generate CLI-readable summary
summary_text = summarize_diagnostics(diagnostics: pd.DataFrame) -> str

# Flag CoCs with potential issues
problems = identify_problem_cocs(
    diagnostics: pd.DataFrame,
    coverage_threshold: float = 0.95,
    max_contribution_threshold: float = 0.8,
) -> pd.DataFrame
```

### ACS Population Ingest Functions

```python
from coclab.acs import (
    fetch_tract_population,
    ingest_tract_population,
)

# Fetch tract population from Census API (all states)
tract_df = fetch_tract_population(
    acs_vintage: str,     # e.g., "2019-2023" or "2023"
    tract_vintage: str,   # e.g., "2023"
) -> pd.DataFrame
# Returns: tract_geoid, acs_vintage, tract_vintage, total_population, moe_total_population, ...

# Full pipeline: fetch, cache, save to parquet with provenance
output_path = ingest_tract_population(
    acs_vintage: str,
    tract_vintage: str,
    force: bool = False,  # Re-fetch even if cached
) -> Path
# Output: data/curated/acs/acs_tracts__A{acs_end}xT{tracts}.parquet
```

### ACS Population Rollup Functions

```python
from coclab.acs import (
    rollup_tract_population,
    build_coc_population_rollup,
)

# Aggregate tract population to CoC using crosswalk
rollup_df = rollup_tract_population(
    tract_pop_df: pd.DataFrame,
    crosswalk_df: pd.DataFrame,
    weighting: str = "area",  # "area" or "population_mass"
) -> pd.DataFrame
# Returns: coc_id, weighting_method, coc_population, coverage_ratio, max_tract_contribution, tract_count

# Full pipeline: load inputs, rollup, save with provenance
output_path = build_coc_population_rollup(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str = "area",
    force: bool = False,
) -> Path
# Output: data/curated/acs/coc_population_rollup__{boundary}__{acs}__{tracts}__{weighting}.parquet
```

### ACS Population Cross-check Functions

```python
from coclab.acs import (
    CrosscheckResult,
    crosscheck_population,
    run_crosscheck,
    print_crosscheck_report,
)

# Compare rollup vs measures
result = crosscheck_population(
    rollup_df: pd.DataFrame,
    measures_df: pd.DataFrame,
    warn_pct: float = 0.01,
    error_pct: float = 0.05,
    min_coverage: float = 0.95,
) -> CrosscheckResult
# CrosscheckResult contains: error_count, warning_count, report_df, missing_in_rollup, missing_in_measures

# Full pipeline: load files, crosscheck, save report
result = run_crosscheck(
    boundary_vintage: str,
    acs_vintage: str,
    tract_vintage: str,
    weighting: str = "area",
    warn_pct: float = 0.01,
    error_pct: float = 0.05,
    min_coverage: float = 0.95,
    save_report: bool = True,
) -> CrosscheckResult

# Print formatted console report
exit_code = print_crosscheck_report(
    result: CrosscheckResult,
    top_n: int = 25,  # Number of worst deltas to show
) -> int  # 0 = passed, 2 = errors found
```

### ZORI Ingestion Functions

```python
from coclab.rents import (
    ingest_zori,
    build_county_weights,
    load_county_weights,
)

# Ingest county-level ZORI data from Zillow
output_path = ingest_zori(
    geography: Literal["county", "zip"] = "county",
    url: str | None = None,      # Override download URL
    force: bool = False,         # Re-download even if cached
    output_dir: Path | None = None,
    raw_dir: Path | None = None,
    start: date | str | None = None,  # Filter dates >= start
    end: date | str | None = None,    # Filter dates <= end
) -> Path
# Output: data/curated/zori/zori__{geography}.parquet

# Build county weights from ACS data
weights_df = build_county_weights(
    acs_vintage: str,     # e.g., "2019-2023"
    method: str,          # renter_households, housing_units, population
    force: bool = False,
    output_dir: Path | None = None,
) -> pd.DataFrame
# Output: data/curated/acs/county_weights__{acs}__{method}.parquet

# Load cached county weights
weights_df = load_county_weights(
    acs_vintage: str,
    method: str,
    base_dir: Path | None = None,
) -> pd.DataFrame
```

### ZORI Aggregation Functions

```python
from coclab.rents import (
    aggregate_zori_to_coc,
    aggregate_monthly,
    collapse_to_yearly,
    compute_coc_county_weights,
)

# Full aggregation pipeline: county ZORI → CoC ZORI
output_path = aggregate_zori_to_coc(
    boundary: str,                # CoC boundary vintage
    counties: str,                # County vintage
    acs_vintage: str,             # ACS vintage for weights
    weighting: str = "renter_households",
    geography: str = "county",
    zori_path: Path | None = None,
    xwalk_path: Path | None = None,
    output_dir: Path | None = None,
    to_yearly: bool = False,
    yearly_method: str = "pit_january",
    force: bool = False,
) -> Path
# Output: data/curated/zori/coc_zori__{geography}__b{boundary}__c{counties}__acs{acs}__w{weighting}.parquet

# Aggregate county ZORI to CoC for each month
coc_zori_df = aggregate_monthly(
    zori_df: pd.DataFrame,
    xwalk_df: pd.DataFrame,
    weights_df: pd.DataFrame,
    min_coverage: float = 0.0,  # Minimum coverage ratio threshold
) -> pd.DataFrame
# Returns: coc_id, date, zori_coc, coverage_ratio, max_geo_contribution, geo_count

# Collapse monthly to yearly
yearly_df = collapse_to_yearly(
    monthly_df: pd.DataFrame,
    method: str = "pit_january",  # pit_january, calendar_mean, calendar_median
) -> pd.DataFrame
# Returns: coc_id, year, zori_coc, coverage_ratio, method
```

### ZORI Diagnostics Functions

```python
from coclab.rents import (
    summarize_coc_zori,
    compute_coc_diagnostics,
    identify_problem_cocs,
    run_zori_diagnostics,
)

# Generate text summary and diagnostics DataFrame
summary_text, diagnostics_df = summarize_coc_zori(
    coc_zori_path_or_df: Path | pd.DataFrame,
    min_coverage: float = 0.90,
    dominance_threshold: float = 0.80,
) -> tuple[str, pd.DataFrame]

# Compute per-CoC diagnostic metrics
diagnostics_df = compute_coc_diagnostics(
    coc_zori_df: pd.DataFrame,
) -> pd.DataFrame
# Returns: coc_id, months_total, months_covered, coverage_ratio_mean/p10/p50/p90, ...

# Identify CoCs with potential issues
problems_df = identify_problem_cocs(
    diagnostics_df: pd.DataFrame,
    min_coverage: float = 0.90,
    dominance_threshold: float = 0.80,
) -> pd.DataFrame
# Returns CoCs with flag_low_coverage or flag_high_dominance = True
```

---

**Previous:** [[05-Recipe-Format]] | **Next:** [[07-Data-Model]]
