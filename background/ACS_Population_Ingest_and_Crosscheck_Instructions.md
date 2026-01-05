# ACS Direct Ingestion + Population Cross-Check — Agent Instructions

## Objective

Ingest **ACS 5-year tract-level population data** directly (as a first-class curated dataset), then run a **cross-check** that verifies:

1. Aggregating tract population to CoCs via your existing crosswalks reproduces the `total_population` already stored in `coc_measures__*`.
2. Coverage and weighting behavior is sane (no double counting, no silent tract drops).
3. Boundary vintage sensitivity can be quantified (optional but recommended).

This work is intentionally scoped so it **does not change** existing Phase 2/3 outputs unless a bug is found. It only adds:
- a canonical tract-population artifact, and
- a validation pipeline + CLI commands.

---

## Pre-reqs (assumed to exist from Phases 1–3)

- Curated census tract geometries:
  - `data/curated/census/tracts__{tract_vintage}.parquet`
- Curated CoC ↔ tract crosswalks:
  - `data/curated/xwalks/coc_tract_xwalk__{boundary_vintage}__{tract_vintage}.parquet`
  - with `area_share` and (if available) `pop_share`
- Curated CoC ACS measures (already built):
  - `data/curated/measures/coc_measures__{boundary_vintage}__{acs_vintage}.parquet`
  - with `total_population`, `weighting_method`, and provenance metadata

---

## Canonical Output (new)

### A) Tract population dataset (new curated artifact)

Write:
```
data/curated/acs/tract_population__{acs_vintage}__{tract_vintage}.parquet
```

Schema (minimum):
- `tract_geoid` (str) — Census tract GEOID (11 chars typical)
- `acs_vintage` (str) — e.g., `2019-2023` or `2022` (whatever convention you use)
- `tract_vintage` (str) — e.g., `2023` (to make geography explicit)
- `total_population` (int)
- `data_source` (str) = `acs_5yr`
- `source_ref` (str) — dataset identifier / retrieval parameters
- `ingested_at` (datetime UTC)
- (optional) `moe_total_population` (float/int) — margin of error if you fetch it

**Provenance**: embed a JSON provenance block in Parquet metadata, consistent with your existing approach:
```json
{
  "dataset": "tract_population",
  "acs_vintage": "2019-2023",
  "tract_vintage": "2023",
  "table": "B01003",
  "variables": ["B01003_001E"],
  "retrieved_at": "UTC timestamp"
}
```

### B) CoC population rollups (optional but recommended)

Write:
```
data/curated/acs/coc_population_rollup__{boundary_vintage}__{acs_vintage}__{tract_vintage}__{weighting}.parquet
```

Schema:
- `coc_id`
- `boundary_vintage`
- `acs_vintage`
- `tract_vintage`
- `weighting_method` (`area` | `population_mass`)
- `coc_population` (float)
- `coverage_ratio` (float)
- `max_tract_contribution` (float)
- `tract_count` (int)

---

## Work Packages (parallel agent implementation)

### WP-A: ACS data fetcher (tract-level B01003)
**Owner:** Agent A  
**Goal:** Implement a robust fetch-and-cache pipeline for ACS tract-level population.

#### Requirements
- Inputs:
  - `acs_vintage` (e.g., `2019-2023`)
  - `tract_vintage` (e.g., `2023`)
- Output: `tract_population__...parquet` (schema above)

#### Implementation notes
- Prefer an approach that does not depend on brittle HTML scraping.
- Support caching: do not re-download if the curated Parquet exists unless `--force`.
- Enforce GEOID normalization: left-pad and preserve leading zeros.

#### Deliverables
- `coclab/acs/ingest/tract_population.py`
- `coclab/acs/registry.py` (optional, if you mirror registries)
- Unit tests:
  - GEOID formatting (leading zeros)
  - non-negative population
  - non-empty dataset

---

### WP-B: Population rollup engine (tract → CoC)
**Owner:** Agent B  
**Goal:** Aggregate tract population to CoC using existing crosswalks.

#### Requirements
- Inputs:
  - `tract_population__...parquet`
  - `coc_tract_xwalk__boundary__tract.parquet`
- Parameter:
  - `--weighting area|population_mass`
- Output:
  - `coc_population_rollup__...parquet` (optional artifact above)

#### Computation
- Join:
  - `xwalk.tract_geoid == tract_population.tract_geoid`
- Weight:
  - `area`: `coc_pop = sum(area_share * tract_pop)`
  - `population_mass`: **same as above**, but records that this is a population-mass attribution assumption rather than a geometric average.

> Note: Avoid “weighted median” style semantics here. Population aggregation is additive. The correct aggregation under partial-tract overlap assumption is:
> `coc_pop = Σ(area_share_i × tract_pop_i)`.

**Deliverables**
- `coclab/acs/rollup.py`
- Tests:
  - sums for a sample CoC are stable
  - `coverage_ratio` computation

---

### WP-C: Cross-check validator (compare rollups vs existing coc_measures)
**Owner:** Agent C  
**Goal:** Compare newly aggregated CoC population vs `coc_measures.total_population`.

#### Requirements
- Inputs:
  - `coc_population_rollup__...parquet` (or computed in-memory)
  - `coc_measures__...parquet`
- Output:
  - human-readable report (console)
  - machine-readable report:
    ```
    data/curated/acs/acs_population_crosscheck__{boundary_vintage}__{acs_vintage}__{tract_vintage}__{weighting}.parquet
    ```

#### Checks (must implement)
1. **Key matching**
   - CoCs present in measures but missing in rollup (and vice versa)
2. **Absolute and percent deltas**
   - `delta = coc_pop_rollup - total_population_measures`
   - `pct_delta = delta / total_population_measures`
3. **Outlier flags**
   - `abs(pct_delta) > warn_pct` warn; `> error_pct` error
4. **Coverage sanity**
   - `coverage_ratio > 1.01` error
   - `coverage_ratio < min_coverage` warn
5. **Rank sanity (optional)**
   - compare top-20 CoCs by population between rollup and measures

**Deliverables**
- `coclab/acs/crosscheck.py`
- Tests for delta computations and thresholding

---

### WP-D: CLI integration
**Owner:** Agent D  
**Goal:** Provide straightforward CLI commands.

#### Commands to implement

1) Ingest tract population
```bash
coclab ingest-acs-population --acs 2019-2023 --tracts 2023
```

2) Build a CoC rollup from tract population
```bash
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
```

3) Cross-check rollup vs existing CoC measures
```bash
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
```

4) One-shot “do all three”
```bash
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
```

#### CLI behavior requirements
- All commands should print:
  - input file paths resolved
  - row counts
  - summary metrics (min/median/max pct_delta; #warnings; #errors)
- Exit codes:
  - 0: no errors
  - 2: errors found (threshold exceeded)
- Support `--force` to re-ingest or recompute artifacts.
- Support thresholds:
  - `--warn-pct 0.01`
  - `--error-pct 0.05`
  - `--min-coverage 0.95`

**Deliverables**
- `coclab/cli/ingest_acs_population.py`
- `coclab/cli/rollup_acs_population.py`
- `coclab/cli/crosscheck_acs_population.py`
- `coclab/cli/verify_acs_population.py`

---

## Recommended Cross-Check Thresholds (initial defaults)

- **Error** if:
  - `abs(pct_delta) > 0.05` (5%)
  - `coverage_ratio > 1.01`
- **Warning** if:
  - `abs(pct_delta) > 0.01` (1%)
  - `coverage_ratio < 0.95`

Always print the top 25 worst deltas (by abs pct).

---

## Notes: “Population vs CoC Area” diagnostics (non-blocking)

Population is not a function of area alone, so “population vs area” is a **diagnostic**, not a correctness assertion.

Recommended outputs:
- `coc_area_km2` (from CoC geometry; equal-area projection)
- `population_density = coc_population / coc_area_km2`
- list top/bottom 20 densities
- soft-flag densities outside plausible bounds (geometry errors often show up here)

---

## Example Workflows

### A) Validate one boundary + ACS vintage end-to-end
```bash
coclab ingest-acs-population --acs 2019-2023 --tracts 2023
coclab rollup-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
coclab crosscheck-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
```

### B) Sensitivity check across weighting modes
```bash
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting area
coclab verify-acs-population --boundary 2025 --acs 2019-2023 --tracts 2023 --weighting population_mass
```

---

## Acceptance Criteria (complete)

1. `ingest-acs-population` produces a tract-level population parquet with correct GEOIDs.
2. Rollups produce `coc_population` and `coverage_ratio` for each CoC.
3. Cross-check produces:
   - missing CoCs
   - worst deltas
   - pass/fail based on thresholds
4. The default run for a known-good configuration exits with code 0 and only a small number of warnings (ideally none).

---

*End of instructions.*
