# Spec: ZORI Ingestion and CoC-Level Aggregation for CoC Lab

**Goal:** Add `coclab` CLI subcommands to (1) ingest Zillow Observed Rent Index (ZORI) data and (2) aggregate ZORI to **Continuum of Care (CoC)** geography using existing CoC Lab crosswalk and diagnostics patterns.

This spec is designed to enable **parallel implementation by multiple agents** with clear interfaces and deliverables.

---

## 1. Scope and Success Criteria

### 1.1 In Scope
- Download and ingest **ZORI** time series from Zillow Economic Research (public datasets).
- Normalize ZORI into a canonical internal schema.
- Aggregate ZORI from a supported base geography to CoC geography (initially **county → CoC**, optionally **ZIP → CoC** if/when crosswalk is available).
- Produce curated Parquet outputs with embedded provenance and diagnostics.
- Add Typer CLI subcommands consistent with existing CoC Lab CLI style.

### 1.2 Out of Scope (for this phase)
- Implementing the GBC Bayesian model (EPA clustering, latent-rate model).
- Creating new CoC boundaries or changing existing boundary ingest logic.
- Producing publication-ready figures.

### 1.3 Success Criteria
- `coclab ingest-zori` produces a normalized Parquet file under `data/curated/zori/` with reproducible metadata.
- `coclab aggregate-zori` produces a CoC×month (and optionally CoC×year) Parquet file and prints coverage diagnostics.
- Aggregation artifacts include:
  - boundary vintage, crosswalk vintage, base geography vintage (if applicable)
  - weighting method used
  - coverage ratio and dominance metrics per CoC and overall

---

## 2. Data Sources and Licensing / Attribution

### 2.1 Zillow ZORI Source
Use Zillow’s public data download(s) for the **Zillow Observed Rent Index (ZORI)**. Do not scrape.

### 2.2 Required Attribution (embed as provenance)
Store the following attribution string in the output metadata and/or a `provenance` JSON column:

> The Zillow Economic Research team publishes a variety of real estate metrics including median home values and rents, inventory, sale prices and volumes, negative equity, home value forecasts and many more. Most datasets are available at the neighborhood, ZIP code, city, county, metro, state and national levels, and many include data as far back as the late 1990s. All data accessed and downloaded from this page is free for public use by consumers, media, analysts, academics and policymakers, consistent with our published Terms of Use. Proper and clear attribution of all data to Zillow is required.

Also include:
- `source = "Zillow Economic Research"`
- `metric = "ZORI"`
- `download_url` (if configured or discovered)
- `downloaded_at` (UTC timestamp)
- `file_hash` (sha256 of raw download)

---

## 3. CLI Commands

### 3.1 `coclab ingest-zori`
**Purpose:** Download and normalize ZORI data into a canonical internal format.

**Command:**
```bash
coclab ingest-zori --geography county --force
```

**Options:**
- `--geography` (required): one of `county`, `zip` (zip is optional feature; implement county first)
- `--url` (optional): override download URL
- `--force` (flag): re-download and reprocess even if cached
- `--output-dir` (default: `data/curated/zori`)
- `--raw-dir` (default: `data/raw/zori`)
- `--start` / `--end` (optional): filter date range after ingest (do not truncate raw archive)

**Outputs:**
- Raw archive/csv saved to: `data/raw/zori/zori__{geography}__{download_date}.(csv|zip)`
- Curated normalized parquet:
  - `data/curated/zori/zori__{geography}.parquet` (latest)
  - optionally versioned by hash:
    - `data/curated/zori/zori__{geography}__{sha256[:10]}.parquet`

**Exit codes:**
- `0` success
- `2` validation/parse error
- `3` download error

### 3.2 `coclab aggregate-zori`
**Purpose:** Aggregate base geography ZORI to CoC geography using CoC boundaries/crosswalks.

**Command (county-based v1):**
```bash
coclab aggregate-zori --boundary 2025 --counties 2023 --acs 2019-2023 --weighting renter_households
```

**Options:**
- `--boundary`, `-b` (required): CoC boundary vintage (as used in crosswalks)
- `--counties`, `-c` (required for county mode): TIGER county vintage year used by the CoC↔county crosswalk
- `--geography` (default: `county`): aggregation base geography (`county` v1, `zip` later)
- `--zori-path` (optional): explicit path to curated `zori__county.parquet`
- `--xwalk-path` (optional): explicit crosswalk path; if omitted, infer from `boundary` and `counties`
- `--weighting`, `-w`: `renter_households` (preferred), `housing_units`, `population`, `equal`
- `--acs` (required when weighting depends on ACS): ACS 5-year vintage used to compute weights (e.g., `2019-2023`)
- `--output-dir` (default: `data/curated/zori`)
- `--to-yearly` (flag): also emit a yearly collapsed file (see §5.4)
- `--yearly-method` (default: `pit_january`): `pit_january`, `calendar_mean`, `calendar_median`
- `--force` (flag): recompute outputs even if present

**Outputs:**
- Monthly CoC-level ZORI:
  - `data/curated/zori/coc_zori__{geography}__b{boundary}__c{counties}__acs{acs}__w{weighting}.parquet`
- Optional yearly output:
  - `data/curated/zori/coc_zori_yearly__{geography}__b{boundary}__c{counties}__acs{acs}__w{weighting}__m{yearly_method}.parquet`
- Diagnostics summary printed to console and optionally saved:
  - `data/curated/zori/diagnostics__coc_zori__{...}.parquet`

**Exit codes:**
- `0` success
- `2` missing required inputs / mismatched vintages
- `3` failure to compute weights (ACS missing)

### 3.3 `coclab zori-diagnostics` (optional, but recommended)
**Purpose:** Summarize missingness, coverage, dominance, and outlier detection.

```bash
coclab zori-diagnostics --coc-zori <path> --output diagnostics.csv
```

---

## 4. Canonical Data Model

### 4.1 Normalized ZORI Schema (base geography)
File: `zori__{geography}.parquet`

Columns (minimum):
- `geo_type` (literal): `county` or `zip`
- `geo_id` (string): county FIPS (5 chars) or ZIP code (5 chars)
- `date` (date): month start (e.g., 2024-01-01)
- `zori` (float): ZORI value (level)
- `series_id` (string, optional): if Zillow provides a series key
- `region_name` (string, optional)
- `state` (string, optional)
- `data_source` = `"Zillow Economic Research"`
- `metric` = `"ZORI"`
- `ingested_at` (timestamp UTC)
- `source_ref` (string): URL or dataset identifier
- `raw_sha256` (string)

Validation:
- `geo_id` not null
- `date` monthly, monotonically increasing per `geo_id`
- `zori` positive when present

### 4.2 CoC-Level Monthly ZORI Schema
File: `coc_zori__...parquet`

Columns:
- `coc_id`
- `date` (month start)
- `zori_coc` (float)
- `base_geo_type` (`county`)
- `boundary_vintage`
- `base_geo_vintage` (e.g., counties year, if applicable)
- `acs_vintage` (if weights depend on ACS)
- `weighting_method` (`renter_households`, etc.)
- `coverage_ratio` (0..1): share of the CoC weight mass represented by base geos with non-null zori that month
- `max_geo_contribution` (0..1): dominance of the largest contributor that month
- `geo_count` (int): count of base geos contributing (non-null zori)
- `provenance` (json string): includes attribution text, urls, hashes, timestamps

### 4.3 Diagnostics Output Schema
Per-CoC summary across a time window:
- `coc_id`
- `months_total`
- `months_covered`
- `coverage_ratio_mean`
- `coverage_ratio_p10`
- `coverage_ratio_p50`
- `coverage_ratio_p90`
- `max_geo_contribution_p90`
- `flag_low_coverage` (bool)
- `flag_high_dominance` (bool)

---

## 5. Aggregation Methodology

### 5.1 Base Geography Strategy (v1)
Implement **county → CoC** first because:
- CoC Lab already supports CoC↔county crosswalk building (`coclab build-xwalks --counties …`).
- County identifiers are stable (5-digit FIPS).

ZIP-based aggregation can be a v2 feature (see §9).

### 5.2 Weight Construction
The aggregation requires weights `w(coc, geo)` that sum to 1 per CoC.

Preferred weight: **renter households** (ACS). Alternatives: housing units, total population, equal weight.

Implementation approach:
1. Build CoC↔county crosswalk (existing artifact).
2. Build county-level weights from ACS tract data aggregated to county, then intersect with CoC→county shares.
   - If tract-level ACS is already available in CoC Lab measures, reuse it; otherwise add a small helper to compute county totals for the selected ACS table(s).
3. For each CoC and month:
   - Filter to counties with non-null ZORI.
   - Compute coverage ratio = sum(weights of included counties)
   - Normalize weights within covered subset for computing `zori_coc` (while keeping coverage as separate diagnostic).

**Formula:**
- Given counties `j` in CoC `i` with weights `w_ij` (sum to 1),
- Let `A_it` be counties with ZORI available at month `t`.
- `coverage_ratio_it = sum_{j in A_it} w_ij`
- `zori_coc_it = sum_{j in A_it} (w_ij / coverage_ratio_it) * zori_jt`

If `coverage_ratio_it < min_threshold` (default 0.90), set `zori_coc_it = null` and flag.

### 5.3 Handling CoC Boundary Changes
Use the selected `boundary_vintage` explicitly and treat results as valid only for that vintage. This matches CoC Lab’s reproducibility stance.

### 5.4 Yearly Collapse (optional output)
If `--to-yearly`:
- `pit_january`: select the ZORI value for **January** of each year (aligns to PIT timing)
- `calendar_mean`: mean across all months in year
- `calendar_median`: median across all months in year

Yearly schema:
- `coc_id`, `year`, `zori_coc`, `coverage_ratio`, `method`, plus provenance fields

---

## 6. Implementation Plan (Parallel Agents)

### Agent A: CLI + Orchestration
**Deliverables:**
- Typer commands:
  - `ingest-zori`
  - `aggregate-zori`
  - (optional) `zori-diagnostics`
- Wiring into existing `coclab/cli/` patterns
- Standard console reporting and exit codes

**Interfaces expected from other agents:**
- `coclab.rents.ingest.ingest_zori(geography, url, ...) -> Path`
- `coclab.rents.aggregate.aggregate_zori_to_coc(...) -> Path`
- `coclab.rents.diagnostics.summarize_coc_zori(...) -> (text, df)`

### Agent B: ZORI Ingestion + Normalization
**Deliverables:**
- `coclab/rents/ingest.py`
  - downloader (requests/httpx), caching, sha256 hashing
  - parser for Zillow format (wide → long)
  - normalized parquet writer
- Validation checks for:
  - monthly continuity
  - geo_id formatting (county FIPS/ZIP)

**Notes:**
- Zillow datasets are often in “wide” format with dates as columns; normalize to long format.

### Agent C: Aggregation Engine (county→CoC v1)
**Deliverables:**
- `coclab/rents/aggregate.py`
  - load normalized ZORI
  - load CoC↔county crosswalk
  - load/compute county weights for selected ACS vintage
  - compute monthly CoC ZORI with coverage/dominance metrics
  - emit parquet + provenance

**Key functions:**
- `build_county_weights(acs_vintage, method) -> DataFrame[county_fips, weight_component]`
- `aggregate_monthly(zori_df, xwalk_df, weights_df) -> coc_zori_df`

### Agent D: Weighting + County ACS Helpers
**Deliverables:**
- Minimal reusable functions to compute county totals for:
  - renter households (ACS table B25003)
  - housing units (ACS table B25001)
  - total population (B01003)
- Ensure consistent vintage handling with existing CoC Lab ACS ingest conventions.

**Optional:** caching county aggregates under `data/curated/acs/`.

### Agent E: Diagnostics + Reporting
**Deliverables:**
- `coclab/rents/diagnostics.py`
  - coverage summaries
  - missingness heatmaps (text summary only, v1)
  - identify problematic CoCs based on thresholds
- Integrate with CLI output style (top N worst coverage, etc.)

---

## 7. File/Directory Layout

Add:
```
coclab/
  rents/
    __init__.py
    ingest.py
    aggregate.py
    weights.py
    diagnostics.py
    schema.py
  cli/
    zori.py            # new Typer group
data/
  raw/zori/
  curated/zori/
```

---

## 8. Testing Strategy

### 8.1 Unit Tests
- Parse test: sample Zillow CSV → normalized long format
- Geo id validation: county FIPS 5 chars; ZIP 5 chars
- Aggregation math: deterministic toy example verifying coverage and normalized weighted mean

### 8.2 Integration / Smoke Tests
- `coclab ingest-zori --geography county --force` (with a small fixture or mocked download)
- `coclab aggregate-zori --boundary 2025 --counties 2023 --acs 2019-2023 --weighting renter_households`
- Assert output file existence and non-empty

### 8.3 Regression Tests
- Verify that a stable run (same inputs) yields identical parquet hash except for timestamps (consider storing timestamps only in metadata, not in-row, or allow for hash differences due to `ingested_at`).

---

## 9. Future Extensions (v2+)

### 9.1 ZIP → CoC Aggregation
Requires one of:
- ZIP ↔ tract crosswalk (e.g., HUD/USPS or Census ZCTA approximations)
- Zillow ZORI ZCTA series (if available in a consistent manner)
- Then reuse existing tract-based CoC crosswalk to map ZIP/ZCTA → CoC.

### 9.2 Rent as Share of Income
Add a derived measure output:
- `zori_to_income = zori_coc / (median_income_coc / 12)` or similar
- Be explicit about monthly/annual alignment choices.

### 9.3 Vintage-Aware Comparisons
Add `compare-zori` akin to `compare-vintages`, to compare CoC ZORI under different boundary vintages or weighting methods.

---

## 10. Open Decisions (default choices; implement now unless blockers)

- Default base geography: `county`
- Default weighting: `renter_households`
- Default yearly method: `pit_january`
- Default min coverage threshold for “usable”: `0.90`
- Store attribution string in `provenance` JSON and include `data_source="Zillow Economic Research"`

---

## Appendix A: Example CLI Workflows

### A.1 County-based pipeline (recommended v1)
```bash
# 1) Ensure boundaries + counties + crosswalk exist
coclab ingest-boundaries --source hud_exchange --vintage 2025
coclab ingest-census --year 2023 --type counties
coclab build-xwalks --boundary 2025 --tracts 2023 --counties 2023

# 2) Ingest ZORI (county)
coclab ingest-zori --geography county

# 3) Aggregate to CoC
coclab aggregate-zori --boundary 2025 --counties 2023 --acs 2019-2023 --weighting renter_households --to-yearly

# 4) Optional diagnostics
coclab zori-diagnostics --coc-zori data/curated/zori/coc_zori__county__b2025__c2023__acs2019-2023__wrenter_households.parquet
```

---

## Appendix B: Provenance JSON Template

```json
{
  "metric": "ZORI",
  "source": "Zillow Economic Research",
  "attribution": "<full required attribution string>",
  "download_url": "<url>",
  "downloaded_at": "<iso8601 utc>",
  "raw_sha256": "<sha256>",
  "boundary_vintage": "2025",
  "base_geo_type": "county",
  "base_geo_vintage": "2023",
  "acs_vintage": "2019-2023",
  "weighting_method": "renter_households"
}
```
