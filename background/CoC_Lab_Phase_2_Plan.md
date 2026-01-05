# CoC Lab — Phase 2 Implementation Plan  
## Geographic Attribution & Measurement Layer (Parallelizable)

**Status prerequisite:** Phase 1 (Boundary Infrastructure) complete  
**Audience:** Multiple AI agents working in parallel  
**Primary objective:** Convert authoritative CoC boundaries into **reproducible, population-aware CoC-level measures** using modern geographic methods.

---

## Phase 2 Goal (Authoritative Statement)

> Given a CoC boundary vintage and a census vintage, the system must be able to **reproducibly attribute people, housing, income, and poverty measures to CoCs**, with explicit diagnostics showing how geography and weighting affect results.

Phase 2 establishes **measurement correctness**. No modeling is included.

---

## Scope Boundaries (What Phase 2 Includes / Excludes)

### Included
- Census tract and county ingestion
- Area-weighted and population-weighted CoC crosswalks
- ACS-derived CoC-level measures
- Coverage and attribution diagnostics
- Full versioning and reproducibility

### Explicitly Excluded
- PIT or HIC ingestion
- Regression or Bayesian models
- Longitudinal panel construction
- Policy or causal inference

---

## Repository Additions (High-level)

```
coclab/
  census/
    ingest/
      tiger_tracts.py
      tiger_counties.py
    registry.py
  xwalks/
    tract.py
    county.py
  measures/
    acs.py
    diagnostics.py
  cli/
    build_xwalks.py
    build_measures.py
data/
  curated/
    census/
    xwalks/
    measures/
```

---

## Data Contracts (Non-negotiable Interfaces)

### Canonical Census Geometry Schema
All census geometries must include:

- `geo_vintage` (e.g., `2023`)
- `geoid` (tract or county GEOID)
- `geometry` (EPSG:4326)
- `source` = `tiger_line`
- `ingested_at`

Stored as GeoParquet:
```
data/curated/census/{type}__{geo_vintage}.parquet
```

---

### Canonical Crosswalk Schema

**CoC ↔ Tract Crosswalk**
```
coc_tract_xwalk__{boundary_vintage}__{tract_vintage}.parquet
```

Columns:
- `coc_id`
- `boundary_vintage`
- `tract_geoid`
- `tract_vintage`
- `area_share`
- `pop_share` (nullable in v1)
- `intersection_area`
- `tract_area`

**CoC ↔ County Crosswalk**
```
coc_county_xwalk__{boundary_vintage}.parquet
```

Columns:
- `coc_id`
- `boundary_vintage`
- `county_fips`
- `area_share`

---

### Canonical CoC Measures Schema

```
coc_measures__{boundary_vintage}__{acs_vintage}.parquet
```

Columns:
- `coc_id`
- `boundary_vintage`
- `acs_vintage`
- `weighting_method` (`area` | `population`)
- `total_population`
- `adult_population`
- `population_below_poverty`
- `median_household_income`
- `median_gross_rent`
- `coverage_ratio` (0–1)
- `source` = `acs_5yr`

---

## Work Packages (Parallel Implementation)

---

## WP-2A: Census Geometry Ingestion (Tracts & Counties)
**Owner:** Agent A  
**Purpose:** Establish authoritative base geographies.

### Tasks
1. Download TIGER/Line tract shapefiles for a specified vintage
2. Download TIGER/Line county shapefiles
3. Normalize CRS to EPSG:4326
4. Retain only required attributes (`GEOID`, geometry)
5. Write GeoParquet outputs
6. Register census geometry vintages

### Deliverables
- `coclab/census/ingest/tiger_tracts.py`
- `coclab/census/ingest/tiger_counties.py`
- Tests for correct GEOID handling

---

## WP-2B: CoC ↔ Tract Crosswalk (Area-weighted)
**Owner:** Agent B  
**Purpose:** Replace centroid heuristics with spatial attribution.

### Method
- Polygon intersection: CoC boundary × census tract
- Compute exact intersection areas

### Tasks
1. Load CoC boundary vintage
2. Load tract geometry vintage
3. Compute intersections
4. Calculate `area_share = intersect_area / tract_area`
5. Persist full crosswalk (no thresholding)

### Deliverables
- `coclab/xwalks/tract.py`
- `data/curated/xwalks/coc_tract_xwalk__*.parquet`
- Performance test on large states

---

## WP-2C: CoC ↔ County Crosswalk (Area-weighted)
**Owner:** Agent C  
**Purpose:** Support county-based datasets.

### Tasks
1. Load CoC boundaries
2. Load county geometries
3. Compute polygon intersections
4. Compute area shares
5. Persist crosswalk

### Deliverables
- `coclab/xwalks/county.py`
- County crosswalk parquet files

---

## WP-2D: Population-weighted Enhancements (ACS-aware)
**Owner:** Agent D  
**Purpose:** Convert geometry-based attribution into people-based attribution.

### Method
For each tract:
```
pop_share = area_share × tract_population / Σ(area_share × tract_population)
```

### Tasks
1. Ingest ACS total population (`B01003`)
2. Join ACS data to tract crosswalk
3. Compute `pop_share`
4. Preserve both area and population shares

### Deliverables
- Enhanced tract crosswalks with `pop_share`
- Validation: shares sum to ~1 per CoC

---

## WP-2E: ACS Measure Builder (v1)
**Owner:** Agent E  
**Purpose:** Produce first analytical CoC dataset.

### Initial ACS Variables
| Measure | ACS Table |
|-------|-----------|
| Total population | B01003 |
| Adult population | Derived (B01001) |
| Population below poverty | C17002 |
| Median household income | B19013 |
| Median gross rent | B25064 |

### Tasks
1. Fetch ACS 5-year estimates at tract level
2. Join with tract crosswalks
3. Aggregate to CoC level using:
   - area weighting
   - population weighting
4. Store results with metadata

### Deliverables
- `coclab/measures/acs.py`
- CoC measures parquet files

---

## WP-2F: Attribution Diagnostics & Coverage Reporting
**Owner:** Agent F  
**Purpose:** Make spatial assumptions explicit.

### Diagnostics per CoC
- Number of tracts intersected
- Max single-tract contribution
- Coverage ratio (population-weighted)
- Area vs population estimate delta

### Deliverables
- `coclab/measures/diagnostics.py`
- Diagnostics tables
- CLI-readable summaries

---

## WP-2G: CLI Integration
**Owner:** Agent G  
**Purpose:** Operationalize Phase 2.

### Commands
```bash
coclab build-xwalks --boundary 2025 --tracts 2023
coclab build-measures --boundary 2025 --acs 2019-2023
```

### Deliverables
- `cli/build_xwalks.py`
- `cli/build_measures.py`
- Help text and examples

---

## WP-2H: Integration Tests & Validation
**Owner:** Agent H  
**Purpose:** Ensure reproducibility and correctness.

### Tests
- Known CoC produces stable totals across runs
- Population shares sum to ~1
- Boundary changes produce explainable deltas

### Deliverables
- `tests/test_phase2_smoke.py`
- Documented assumptions in README

---

## Sequencing & Critical Path

### Fully Parallel
- WP-2A, 2B, 2C can start immediately
- WP-2E can stub against sample crosswalks

### Dependencies
- WP-2D depends on WP-2B
- WP-2F depends on WP-2D and WP-2E
- WP-2G depends on all

---

## Acceptance Criteria (Phase 2 Complete)

1. CoC-level ACS measures can be produced for any boundary vintage
2. Area-weighted and population-weighted results are both available
3. Diagnostics quantify geographic sensitivity
4. Results are reproducible given fixed vintages
5. Outputs are ready for Phase 3 modeling

---

## Phase 3 Preview (Context Only)

- PIT & HIC ingestion
- Panel construction (CoC × year)
- Replication and extension of Byrne/Glynn methodologies
- Nonlinear clustering and threshold detection

---

*End of Phase 2 Plan*
