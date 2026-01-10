# CoC Boundary Infrastructure (Steps 1–2) — Implementation Plan (Parallelizable)

**Goal (v0):** Build a Python-based data + geospatial foundation that (1) ingests **up-to-date Continuum of Care (CoC) boundary geometries** with explicit versioning, (2) normalizes and validates those geometries, and (3) provides an immediate capability to **render any CoC boundary on an interactive map** (by `coc_id`, defaulting to the latest boundary vintage).

This plan is structured as parallel “work packages” that multiple AI agents can implement concurrently with clear interfaces (“contracts”) between them.

---

## 0) Repo conventions (shared baseline)

### Suggested repository layout
```
coclab/
  coclab/
    __init__.py
    config.py
    registry/
      schema.py
      registry.py
    ingest/
      hud_exchange_gis.py
      hud_opendata_arcgis.py
      __init__.py
    geo/
      normalize.py
      validate.py
      io.py
      __init__.py
    viz/
      map_folium.py
      map_plotly.py
      __init__.py
    cli/
      main.py
      __init__.py
  data/
    raw/
    curated/
  tests/
  pyproject.toml
  README.md
```

### Standards and tooling
- **Python:** 3.11+
- **Formatting/linting:** ruff + black
- **Testing:** pytest
- **Packaging/CLI:** typer
- **Geospatial:** geopandas + shapely + pyproj
- **Storage:** GeoParquet (pyarrow); optional DuckDB queries later

---

## 1) Data contracts (interfaces between work packages)

### 1.1 Canonical boundary schema (GeoDataFrame columns)
All ingesters must output a GeoDataFrame with these columns (and a valid `geometry`):
- `boundary_vintage` *(str)*: e.g., `2025`, `2024`, or `HUDOpenData_2025-08-19`
- `coc_id` *(str)*: CoC code like `CO-500`
- `coc_name` *(str)*: official name/label (best available)
- `state_abbrev` *(str)*: e.g., `CO`
- `source` *(str)*: `hud_exchange_gis_tools` | `hud_opendata_arcgis`
- `source_ref` *(str)*: URL or dataset identifier
- `ingested_at` *(datetime, UTC)*: timestamp of ingestion
- `geom_hash` *(str)*: stable hash of geometry WKB normalized (used to detect changes)
- `geometry` *(shapely geometry)*: polygon/multipolygon in EPSG:4326

### 1.2 Storage targets
- Curated boundary vintages written as:  
  `data/curated/coc_boundaries/coc_boundaries__{boundary_vintage}.parquet`
- A registry table (CSV/Parquet) listing available vintages and basic counts:  
  `data/curated/boundary_registry.parquet`

### 1.3 Minimal public API (module-level functions)
These functions define the integration points:

**Ingest**
- `ingest_hud_exchange(boundary_vintage: str) -> Path`  *(writes curated parquet; returns path)*
- `ingest_hud_opendata(snapshot_tag: str = "latest") -> Path`

**Geo/Normalization**
- `normalize_boundaries(gdf) -> gdf`
- `validate_boundaries(gdf) -> list[str]` *(returns warnings/errors)*

**Registry**
- `register_vintage(vintage: str, path: Path, meta: dict) -> None`
- `latest_vintage() -> str`

**Viz**
- `render_coc_map(coc_id: str, vintage: str | None = None, out_html: Path | None = None) -> Path`

**CLI**
- `coclab ingest-boundaries --source hud_exchange --vintage 2025`
- `coclab show --coc CO-500 [--vintage 2025]`

---

## 2) Work Packages (parallel implementation)

### WP-A: HUD Exchange CoC GIS Tools ingester
**Owner:** Agent A  
**Objective:** Download and parse year-specific CoC shapefiles, output canonical schema GeoParquet.

**Tasks**
1. Implement downloader:
   - Inputs: `boundary_vintage` (e.g., `2025`)
   - Downloads zipped shapefile (or equivalent) to `data/raw/hud_exchange/{vintage}/`
2. Read shapefile to GeoDataFrame (`geopandas.read_file`)
3. Map source-specific fields to canonical fields:
   - identify CoC code field (e.g., `CocID` / `COC_NUM` etc.)
4. Add metadata fields: `boundary_vintage`, `source`, `source_ref`, `ingested_at`
5. Call WP-C normalize + WP-D validate
6. Compute `geom_hash` (WP-C helper)
7. Write GeoParquet to curated location

**Deliverables**
- `coclab/ingest/hud_exchange_gis.py`
- Unit test for schema completeness on a small sample

**Notes**
- Keep logic resilient to field name drift across vintages.

---

### WP-B: HUD Open Data (ArcGIS Hub) CoC Grantee Areas ingester
**Owner:** Agent B  
**Objective:** Programmatic fetch from ArcGIS feature service / Hub dataset to provide a “current snapshot” vintage.

**Tasks**
1. Implement ArcGIS querying:
   - Determine feature service endpoint and query paging
   - Pull all features and attributes
2. Convert to GeoDataFrame; enforce CRS EPSG:4326
3. Map fields to canonical columns; set `boundary_vintage` as `HUDOpenData_{YYYY-MM-DD}` (date from metadata or ingestion date)
4. Call WP-C normalize + WP-D validate
5. Write curated GeoParquet

**Deliverables**
- `coclab/ingest/hud_opendata_arcgis.py`
- Test that fetch returns non-empty and expected columns

---

### WP-C: Geometry normalization utilities
**Owner:** Agent C  
**Objective:** Make geometries consistent, valid, and hashable.

**Tasks**
1. CRS normalization:
   - Convert all to EPSG:4326 (lat/lon)
2. Geometry validity fixes:
   - `shapely.make_valid` where available; fallback patterns
3. Geometry simplification policy (optional; default OFF for v0):
   - Provide helper to simplify for web display without changing stored canonical
4. Stable geometry hashing:
   - Normalize geometry (e.g., WKB with fixed precision) then hash (SHA-256)
5. Ensure type is Polygon/MultiPolygon; drop/flag others

**Deliverables**
- `coclab/geo/normalize.py`
- `coclab/geo/io.py` (helpers to read/write GeoParquet)
- Tests for hashing determinism and CRS conversion

---

### WP-D: Boundary QA/validation
**Owner:** Agent D  
**Objective:** Detect and report issues early.

**Tasks**
1. Validate required columns exist and types are sane
2. Validate uniqueness of `coc_id` within a `boundary_vintage`
3. Validate geometry is non-empty and valid (post-normalization)
4. Basic anomaly checks:
   - extremely small area polygons (optional)
   - invalid bbox ranges (lat > 90, lon > 180)
5. Produce a structured report object (list of warnings/errors)

**Deliverables**
- `coclab/geo/validate.py`
- `tests/test_validate.py`

---

### WP-E: Registry and version selection
**Owner:** Agent E  
**Objective:** Track what boundary vintages exist and select “latest” deterministically.

**Tasks**
1. Registry schema (Parquet table):
   - `boundary_vintage`, `source`, `ingested_at`, `path`, `feature_count`, `hash_of_file`
2. `register_vintage(...)`:
   - append/update registry with idempotency
3. `latest_vintage()`:
   - choose most recent `ingested_at` or highest year for `hud_exchange_gis_tools` by policy
4. Provide `list_vintages()` for CLI

**Deliverables**
- `coclab/registry/registry.py`
- `coclab/registry/schema.py`
- Tests for stable latest selection

---

### WP-F: Visualization (interactive boundary map)
**Owner:** Agent F  
**Objective:** Render a single CoC boundary as interactive HTML.

**Tasks**
1. Implement `render_coc_map(...)` with Folium (recommended for v0):
   - Load curated GeoParquet for the selected vintage (or `latest_vintage()`)
   - Filter by `coc_id` (case/whitespace robust)
   - Create folium map centered on polygon centroid
   - Add polygon with tooltip (coc_id, name, vintage, source)
   - Save to `data/curated/maps/{coc_id}__{vintage}.html`
2. Optional: add Plotly alternative for dashboard integration

**Deliverables**
- `coclab/viz/map_folium.py` (primary)
- `coclab/viz/map_plotly.py` (optional)
- Simple test: function produces an HTML file

---

### WP-G: CLI wiring (Typer)
**Owner:** Agent G  
**Objective:** Make the system usable without notebooks.

**Commands**
- `coclab ingest-boundaries --source hud_exchange --vintage 2025`
- `coclab ingest-boundaries --source hud_opendata --snapshot latest`
- `coclab list-vintages`
- `coclab show --coc CO-500 [--vintage 2025]`

**Deliverables**
- `coclab/cli/main.py`
- `pyproject.toml` entrypoint

---

### WP-H: Integration tests + smoke workflow
**Owner:** Agent H  
**Objective:** Provide a single “smoke run” that proves the pipeline works end-to-end.

**Tasks**
1. End-to-end script:
   - Ingest one vintage (or snapshot)
   - Register it
   - Render one known CoC map
2. Add CI-ready test that can run offline if raw files are cached (or use a small fixture)
3. Document quickstart in README

**Deliverables**
- `tests/test_smoke.py`
- `README.md` quickstart section

---

## 3) Sequencing and critical path

### Can be done in parallel immediately
- WP-A, WP-B, WP-C, WP-D, WP-F, WP-G can all start concurrently (contracts defined above).

### Critical integration points
- WP-A/WP-B depend on WP-C and WP-D function signatures.
- WP-F depends on WP-E (`latest_vintage`) OR can temporarily accept a passed vintage.
- WP-G depends on all WPs for wiring but can be stubbed early.

### Suggested minimal “first demo”
1. Complete WP-A + WP-C + WP-D + WP-F (hardcode vintage)  
2. Add WP-E registry and update WP-F to default to latest  
3. Wire WP-G CLI to run ingest + show

---

## 4) Acceptance criteria (v0)

1. **Ingest**: Running `coclab ingest-boundaries --source hud_exchange --vintage <YEAR>` creates:
   - `data/curated/coc_boundaries/coc_boundaries__<YEAR>.parquet`
   - updates `data/curated/boundary_registry.parquet`
2. **Map**: Running `coclab show --coc CO-500` produces an HTML file with the boundary polygon rendered.
3. **Versioning**: Multiple vintages can coexist; `latest_vintage()` selects a default consistently.
4. **Validation**: Invalid geometries are fixed or flagged; duplicates and missing IDs are flagged.

---

## 5) Stretch goals (still within Steps 1–2 scope)

- Add **area-weighted** CoC↔county and CoC↔tract intersections (crosswalks) as additional curated outputs:
  - `data/curated/coc_county_xwalk__{vintage}.parquet`
  - `data/curated/coc_tract_xwalk__{vintage}.parquet`
- Add a simple **change detector**:
  - Compare `geom_hash` across vintages for the same `coc_id` and report “boundary changed”
- Add lightweight **DuckDB** querying for fast filtering without loading full GeoParquet into memory.

---

## 6) Implementation checklist (quick)

- [ ] WP-C normalize + hash utilities
- [ ] WP-D validate utilities
- [ ] WP-A HUD Exchange ingester → GeoParquet
- [ ] WP-E registry + latest selection
- [ ] WP-F Folium map rendering
- [ ] WP-G CLI wiring
- [ ] WP-H smoke test + README

---

## 7) Recommended environment (requirements)

Minimal dependencies:
- geopandas
- shapely
- pyproj
- pyarrow
- pandas
- folium
- typer
- ruff
- pytest

Optional:
- duckdb
- plotly

---
