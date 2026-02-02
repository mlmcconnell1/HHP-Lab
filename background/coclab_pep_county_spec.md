# Spec: PEP Annual County Population Estimates Ingestion for CoC Lab

**Goal:** Add `coclab` CLI subcommands to (1) ingest Census Bureau Population Estimates Program (PEP) county-level annual population estimates and (2) aggregate to CoC geography using existing crosswalks.

This spec covers **intercensal (2010-2020)** and **postcensal (2020+)** county population estimates from the Census Bureau.
Ingest should default to the best-available series (postcensal for now) and track hashes so future intercensal releases can be detected and compared against current postcensal values.

---

## 1. Scope and Success Criteria

### 1.1 In Scope
- Download and ingest PEP county-level annual population estimates (total population)
- Handle both **intercensal** (2010-2020) and **postcensal** (2020-latest) series
- Normalize into canonical internal schema with full provenance
- Aggregate county estimates to CoC geography using existing county crosswalks
- Produce curated Parquet outputs with diagnostics

### 1.2 Out of Scope (for this phase)
- Demographic breakdowns (age, sex, race, Hispanic origin)
- Components of change (births, deaths, migration)
- Subcounty (place/city) estimates
- State or national aggregates (beyond what CoC rollup produces)

### 1.3 Success Criteria
- `coclab ingest pep` produces normalized Parquet under `data/curated/pep/`
- `coclab build pep-coc` aggregates to CoC level with coverage diagnostics
- Coverage spans 2010 through latest available vintage year
- Provenance includes source URLs, file hashes, and attribution

---

## 2. Data Sources

### 2.1 Primary Sources

#### Intercensal Estimates (2010-2020)
- **URL:** https://www.census.gov/data/tables/time-series/demo/popest/intercensal-2010-2020-counties.html
- **Description:** Revised estimates that bridge 2010 and 2020 decennial censuses
- **Release:** November 2024 (totals); Fall 2025 (demographics)
- **Coverage:** Annual estimates for July 1, 2010 through July 1, 2020
- **Format:** CSV with county FIPS, state FIPS, names, and annual population columns
- **Note:** These are the **official** estimates for the 2010-2020 decade

#### Postcensal Estimates (2020-latest)
- **URL:** https://www.census.gov/data/tables/time-series/demo/popest/2020s-counties-total.html
- **Description:** Current estimates (will be revised when 2030 intercensals released)
- **Release:** Annual (typically March for previous July 1 estimates)
- **Coverage:** Annual estimates from April 1, 2020 through July 1 of latest vintage
- **Format:** CSV with county FIPS, state FIPS, names, and annual population columns
- **Vintage naming:** "Vintage 2024" = estimates released in 2025 covering through July 1, 2024

### 2.2 Reference Date Convention
PEP estimates are as of **July 1** of each year (except decennial census years which also have April 1 values). This differs from:
- PIT counts (late January)
- ACS 5-year estimates (rolling 5-year period)

### 2.3 Attribution / Licensing
Public domain (U.S. Government work). Include attribution:
```
Source: U.S. Census Bureau, Population Estimates Program (PEP)
```

---

## 3. Data Model

### 3.1 Normalized County PEP Schema
File: `data/curated/pep/pep_county_population__{series}.parquet`

Where `{series}` is:
- `intercensal_2010_2020` for the bridged decade
- `postcensal_2020s_v{vintage}` for current estimates (e.g., `postcensal_2020s_v2024`)

| Column | Type | Description |
|--------|------|-------------|
| `county_fips` | string(5) | 5-digit county FIPS (state + county) |
| `state_fips` | string(2) | 2-digit state FIPS |
| `county_name` | string | County name |
| `state_name` | string | State name or abbreviation |
| `year` | int | Estimate year (e.g., 2015) |
| `reference_date` | date | July 1 of year (or April 1 for census base) |
| `population` | int | Total population estimate |
| `estimate_type` | string | `intercensal` or `postcensal` |
| `vintage` | string | Release vintage (e.g., `2024` for Vintage 2024) |
| `data_source` | string | `census_pep` |
| `source_url` | string | Download URL |
| `raw_sha256` | string | SHA-256 of source file |
| `ingested_at` | datetime | UTC timestamp |

### 3.2 Combined/Merged County Series
File: `data/curated/pep/pep_county_population__combined__{start_year}_{end_year}.parquet`

For convenience, a merged file spanning both intercensal and postcensal:
- Uses intercensal for 2010-2019
- Uses postcensal for 2020+ (avoiding overlap at 2020)
- Clearly marks `estimate_type` for each row
- If intercensal is unavailable, merged output should fall back to postcensal only

### 3.3 CoC-Level Aggregated Schema
File: `data/curated/pep/coc_pep_population__b{boundary}__c{counties}__w{weighting}__{start_year}_{end_year}.parquet`

| Column | Type | Description |
|--------|------|-------------|
| `coc_id` | string | CoC identifier |
| `year` | int | Estimate year |
| `reference_date` | date | July 1 of year |
| `population` | float | Aggregated population |
| `estimate_type` | string | `intercensal`, `postcensal`, or `mixed` |
| `boundary_vintage` | string | CoC boundary vintage used |
| `county_vintage` | string | TIGER county vintage for crosswalk |
| `weighting_method` | string | `area_share` or `population_share` |
| `coverage_ratio` | float | Share of CoC weight covered by counties with data |
| `county_count` | int | Number of contributing counties |
| `provenance` | string (JSON) | Full provenance record |

---

## 4. CLI Commands

### 4.1 `coclab ingest pep`
**Purpose:** Download and normalize PEP county population estimates.

```bash
coclab ingest pep --series auto
coclab ingest pep --series intercensal-2010-2020
coclab ingest pep --series postcensal --vintage 2024
coclab ingest pep --series all --vintage 2024  # Both series
```

**Options:**
| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--series` | No | `auto` | `auto`, `intercensal-2010-2020`, `postcensal`, or `all` |
| `--vintage` | If postcensal | latest | Postcensal vintage year (e.g., 2024) |
| `--prefer-postcensal-2020` | No | False | For `--series all/auto`, use postcensal for 2020 |
| `--force` | No | False | Re-download even if cached |
| `--output-dir` | No | `data/curated/pep` | Output directory |
| `--raw-dir` | No | `data/raw/pep` | Raw file cache directory |

**Outputs:**
- Raw CSV: `data/raw/pep/pep_county__{series}__{download_date}.csv`
- Curated: `data/curated/pep/pep_county_population__{series}.parquet`

**Notes:**
- Ingest tracks SHA-256 hashes for each source file so future intercensal releases can be detected and compared against current postcensal values.

**Exit codes:**
- `0`: Success
- `2`: Validation/parse error
- `3`: Download error

### 4.2 `coclab build pep-coc`
**Purpose:** Aggregate county PEP estimates to CoC geography.

```bash
coclab build pep-coc --boundary 2025 --counties 2023 --weighting area_share
```

**Options:**
| Option | Required | Default | Description |
|--------|----------|---------|-------------|
| `--boundary`, `-b` | Yes | - | CoC boundary vintage |
| `--counties`, `-c` | Yes | - | TIGER county vintage for crosswalk |
| `--weighting`, `-w` | No | `area_share` | `area_share` or `population_share` |
| `--pep-path` | No | auto-detect | Explicit path to county PEP parquet |
| `--xwalk-path` | No | auto-detect | Explicit crosswalk path |
| `--start-year` | No | 2010 | First year to include |
| `--end-year` | No | latest | Last year to include |
| `--force` | No | False | Recompute even if exists |

**Outputs:**
- `data/curated/pep/coc_pep_population__b{boundary}__c{counties}__w{weighting}__{start}_{end}.parquet`

### 4.3 `coclab show pep` (optional)
**Purpose:** Display summary of available PEP data.

```bash
coclab show pep --detail
```

---

## 5. Aggregation Methodology

### 5.1 Weight Construction
Reuse existing CoC-county crosswalk weights:
- `area_share`: Proportion of county area within CoC
- `population_share`: If tract-level population weights available

### 5.2 Aggregation Formula
For each CoC `i` and year `t`:
```
pop_coc_it = Σ_j (w_ij × pop_county_jt)
```

Where:
- `j` indexes counties overlapping CoC `i`
- `w_ij` is the weight (area or population share)
- Counties missing population data are excluded, with coverage tracked

### 5.3 Coverage Tracking
- `coverage_ratio = Σ(weights of counties with data) / Σ(all weights)`
- Flag CoC-years with coverage < 0.95

### 5.4 Handling Series Overlap
The year 2020 appears in both intercensal and postcensal series:
- **Default behavior:** Use intercensal for 2020 (as it's the "official" bridged value) when available
- If intercensal is not available, fall back to postcensal for all years
- Optionally allow user to prefer postcensal for 2020 via flag

### 5.5 Geographic Vintage Alignment
- PEP uses geography as of the estimate year
- FIPS code changes (county splits, merges) are rare but possible
- Log warnings for any FIPS codes in PEP not found in crosswalk

---

## 6. Implementation Plan

### Agent A: CLI + Orchestration
**Deliverables:**
- Typer commands: `ingest pep`, `build pep-coc`
- CLI wiring in `coclab/cli/`
- Standard console output and exit codes

**Interfaces expected:**
- `coclab.pep.ingest.ingest_pep_county(...) -> Path`
- `coclab.pep.aggregate.aggregate_pep_to_coc(...) -> Path`

### Agent B: PEP Download + Normalization
**Deliverables:**
- `coclab/pep/ingest.py`
  - Download functions for intercensal and postcensal CSVs
  - Wide-to-long transformation (years as columns → rows)
  - FIPS normalization (5-char left-padded)
  - Parquet writer with provenance metadata
- `coclab/pep/sources.py`
  - URL patterns and vintage detection

**Data quirks to handle:**
- Header rows vary between series
- Some files have multiple header lines or footnotes
- FIPS codes may be numeric (need left-pad)
- State totals may be included (filter to county rows only)

### Agent C: CoC Aggregation Engine
**Deliverables:**
- `coclab/pep/aggregate.py`
  - Load county PEP data
  - Load CoC-county crosswalk
  - Compute weighted aggregates per year
  - Track coverage metrics
  - Emit parquet with provenance

### Agent D: Registry Integration
**Deliverables:**
- Register PEP as a data source in `source_registry.parquet`
- Add PEP outputs to any dataset catalogs

---

## 7. File/Directory Layout

```
coclab/
  pep/
    __init__.py
    ingest.py        # Download and normalize
    aggregate.py     # County → CoC aggregation
    sources.py       # URL patterns, vintage detection
    schema.py        # Pandera schemas (optional)
  cli/
    ingest/
      pep.py         # Typer command
    build/
      pep_coc.py     # Typer command
data/
  raw/pep/
  curated/pep/
```

---

## 8. Testing Strategy

### 8.1 Unit Tests
- FIPS normalization (leading zeros preserved)
- Wide-to-long transformation correctness
- Coverage ratio computation
- Weight normalization

### 8.2 Integration Tests
- `coclab ingest pep --series intercensal-2010-2020 --force` with fixture
- `coclab build pep-coc --boundary 2025 --counties 2023`
- Assert output files exist and have expected columns

### 8.3 Data Validation
- No null county_fips
- Population values non-negative
- Years within expected range (2010-2025 currently)
- All 50 states + DC + PR represented

---

## 9. Known Quirks and Edge Cases

### 9.1 Puerto Rico
- PR municipios are included in county files
- FIPS codes 72001-72153
- Handle consistently with other county data

### 9.2 County FIPS Changes
- Rare but possible (e.g., Bedford City, VA merged 2013)
- Log warning if PEP FIPS not in crosswalk

### 9.3 Population Universe
- PEP estimates **resident population** (excludes overseas military, etc.)
- Consistent with ACS universe

### 9.4 Suppression
- No suppression in total population counts (unlike demographic detail)

---

## 10. Future Extensions (v2+)

### 10.1 Demographic Detail
Add age/sex/race/Hispanic origin breakdowns when intercensal demographics released (Fall 2025).

### 10.2 Components of Change
Ingest births, deaths, domestic migration, international migration for analytical uses.

### 10.3 Historical Series
Extend to 2000-2010 intercensal if needed for longer time series.

### 10.4 Comparison Tool
`coclab compare pep-acs` to compare PEP estimates vs ACS 5-year estimates for validation.

---

## Appendix A: Source File Details

### A.1 Intercensal 2010-2020 County Totals
- **Direct download:** `https://www2.census.gov/programs-surveys/popest/datasets/2010-2020/counties/asrh/`
- **File pattern:** `co-est00int-tot.csv` or similar
- **Key columns:** STATE, COUNTY, STNAME, CTYNAME, POPESTIMATE2010...POPESTIMATE2020

### A.2 Postcensal 2020s County Totals
- **Direct download:** `https://www2.census.gov/programs-surveys/popest/datasets/2020-{vintage}/counties/totals/`
- **File pattern:** `co-est{vintage}-alldata.csv`
- **Key columns:** STATE, COUNTY, STNAME, CTYNAME, POPESTIMATE2020...POPESTIMATE{vintage}

---

## Appendix B: Provenance JSON Template

```json
{
  "dataset": "pep_county_population",
  "series": "intercensal_2010_2020",
  "vintage": null,
  "source": "U.S. Census Bureau, Population Estimates Program",
  "source_url": "<download_url>",
  "downloaded_at": "<iso8601_utc>",
  "raw_sha256": "<sha256>",
  "years_covered": [2010, 2011, ..., 2020],
  "reference_date_convention": "july_1",
  "population_universe": "resident_population"
}
```

---

## Appendix C: Example CLI Workflow

```bash
# 1) Ensure county geometries and crosswalks exist
coclab ingest-census --year 2023 --type counties
coclab build-xwalks --boundary 2025 --counties 2023

# 2) Ingest PEP county estimates
coclab ingest pep --series intercensal-2010-2020
coclab ingest pep --series postcensal --vintage 2024

# 3) Aggregate to CoC
coclab build pep-coc --boundary 2025 --counties 2023 --weighting area_share

# 4) Verify output
coclab show pep --detail
```

---

*End of specification.*
